from __future__ import annotations

from collections.abc import Collection, Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Any

import pyarrow as pa

__all__ = [
    "ChangeBatch",
    "ChangeRecord",
    "EventKind",
    "SchemaDiff",
    "TableBatch",
    "TableSchema",
]

DDL_MARKER_FIELDS: frozenset[str] = frozenset({"ddl", "databaseName", "tableChanges"})
HEARTBEAT_SCHEMA_SUFFIX: str = "io.debezium.connector.common.Heartbeat"
SCHEMA_CHANGE_SUFFIXES: tuple[str, str] = (
    ".SchemaChangeValue",
    ".SchemaChangeKey",
)


def _has_lossy_promotion(old_t: pa.DataType, new_t: pa.DataType) -> bool:
    if (
        pa.types.is_int64(old_t)
        or pa.types.is_uint64(old_t)
        or pa.types.is_decimal(old_t)
    ) and pa.types.is_floating(new_t):
        return True
    if pa.types.is_struct(old_t) and pa.types.is_struct(new_t):
        new_fields = {f.name: f.type for f in new_t}
        return any(
            f.name in new_fields and _has_lossy_promotion(f.type, new_fields[f.name])
            for f in old_t
        )
    if pa.types.is_list(old_t) and pa.types.is_list(new_t):
        return _has_lossy_promotion(old_t.value_type, new_t.value_type)
    if pa.types.is_map(old_t) and pa.types.is_map(new_t):
        return _has_lossy_promotion(
            old_t.key_type, new_t.key_type
        ) or _has_lossy_promotion(old_t.item_type, new_t.item_type)
    return False


def _has_non_nullable_addition(old_t: pa.DataType, new_t: pa.DataType) -> bool:
    if pa.types.is_struct(old_t) and pa.types.is_struct(new_t):
        old_map = {f.name: f for f in old_t}
        for new_f in new_t:
            old_f = old_map.get(new_f.name)
            if old_f is None:
                if not new_f.nullable:
                    return True
            elif _has_non_nullable_addition(old_f.type, new_f.type):
                return True
        return False
    if pa.types.is_list(old_t) and pa.types.is_list(new_t):
        return _has_non_nullable_addition(old_t.value_type, new_t.value_type)
    if pa.types.is_map(old_t) and pa.types.is_map(new_t):
        return _has_non_nullable_addition(
            old_t.key_type, new_t.key_type
        ) or _has_non_nullable_addition(old_t.item_type, new_t.item_type)
    return False


def _is_additive_struct_change(old_f: pa.Field, new_f: pa.Field) -> bool:
    if old_f.nullable != new_f.nullable or not (
        pa.types.is_struct(old_f.type) and pa.types.is_struct(new_f.type)
    ):
        return False
    old_fields = list(old_f.type)
    new_fields = list(new_f.type)
    old_map = {f.name: f for f in old_fields}
    new_map = {f.name: f for f in new_fields}
    if any(name not in new_map for name in old_map):
        return False
    if [f.name for f in old_fields] != [
        f.name for f in new_fields if f.name in old_map
    ]:
        return False
    found_addition = False
    for new_sub in new_fields:
        old_sub = old_map.get(new_sub.name)
        if old_sub is None:
            if not new_sub.nullable:
                return False
            found_addition = True
        elif old_sub.type != new_sub.type or old_sub.nullable != new_sub.nullable:
            if not _is_additive_struct_change(old_sub, new_sub):
                return False
            found_addition = True
    return found_addition


def _validate_arrow_nullability(
    col: pa.Array | pa.ChunkedArray,
    field: pa.Field,
    path: str,
) -> None:
    if not field.nullable and col.null_count > 0:
        raise ValueError(
            f"Non-nullable target field '{path}' contains {col.null_count} null value(s)."
        )
    dtype = field.type
    if not pa.types.is_nested(dtype):
        return
    chunks: list[Any] = col.chunks if isinstance(col, pa.ChunkedArray) else [col]
    for chunk in chunks:
        if len(chunk) == 0:
            continue
        if pa.types.is_struct(dtype):
            active = chunk.filter(chunk.is_valid()) if chunk.null_count > 0 else chunk
            if len(active) == 0:
                continue
            for idx, sub_field in enumerate(dtype):
                _validate_arrow_nullability(
                    active.field(idx), sub_field, f"{path}.{sub_field.name}"
                )
        elif pa.types.is_map(dtype):
            active = chunk.filter(chunk.is_valid()) if chunk.null_count > 0 else chunk
            if len(active) == 0:
                continue
            start = active.offsets[0].as_py()
            end = active.offsets[-1].as_py()
            if start < end:
                _validate_arrow_nullability(
                    active.keys[start:end], dtype.key_field, f"{path}.key"
                )
                _validate_arrow_nullability(
                    active.items[start:end], dtype.item_field, f"{path}.value"
                )
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            flattened = chunk.flatten()
            if len(flattened) > 0:
                _validate_arrow_nullability(
                    flattened, dtype.value_field, f"{path}.{dtype.value_field.name}"
                )


class EventKind(str, Enum):
    """Categorization of a change event based on payload presence, destination, and schema markers."""

    DATA = "data"
    HEARTBEAT = "heartbeat"
    SCHEMA_CHANGE = "schema_change"
    TOMBSTONE = "tombstone"

    @classmethod
    def classify(
        cls,
        destination: str,
        field_names: Collection[str] | None,
        heartbeat_prefix: str,
        *,
        schema_name: str | None = None,
    ) -> EventKind:
        if field_names is None:
            return cls.TOMBSTONE
        if destination.startswith(heartbeat_prefix) or (
            schema_name is not None and schema_name.endswith(HEARTBEAT_SCHEMA_SUFFIX)
        ):
            return cls.HEARTBEAT
        if (
            schema_name is not None and schema_name.endswith(SCHEMA_CHANGE_SUFFIXES)
        ) or DDL_MARKER_FIELDS.issubset(field_names):
            return cls.SCHEMA_CHANGE
        return cls.DATA


@dataclass(frozen=True, slots=True)
class SchemaDiff:
    """Describes structural differences between a previous schema and a new schema."""

    added_fields: tuple[pa.Field, ...] = ()
    removed_fields: tuple[pa.Field, ...] = ()
    changed_fields: tuple[tuple[pa.Field, pa.Field], ...] = ()
    reordered: bool = False
    key_fields_changed: bool = False
    key_schema_changed: bool = False

    @property
    def has_changes(self) -> bool:
        return bool(
            self.added_fields
            or self.removed_fields
            or self.changed_fields
            or self.reordered
            or self.key_fields_changed
            or self.key_schema_changed
        )

    @property
    def is_additive(self) -> bool:
        if (
            self.removed_fields
            or self.reordered
            or self.key_fields_changed
            or self.key_schema_changed
            or (not self.added_fields and not self.changed_fields)
            or any(not f.nullable for f in self.added_fields)
        ):
            return False
        return all(
            _is_additive_struct_change(old_f, new_f)
            for old_f, new_f in self.changed_fields
        )

    @property
    def is_compatible(self) -> bool:
        if (
            self.removed_fields
            or self.key_fields_changed
            or self.key_schema_changed
            or any(not f.nullable for f in self.added_fields)
        ):
            return False
        if not self.changed_fields:
            return True
        if any(
            _has_lossy_promotion(old_f.type, new_f.type)
            or _has_non_nullable_addition(old_f.type, new_f.type)
            for old_f, new_f in self.changed_fields
        ):
            return False
        old_s = pa.schema([old_f for old_f, _ in self.changed_fields])
        new_s = pa.schema([new_f for _, new_f in self.changed_fields])
        try:
            unified = pa.unify_schemas([new_s, old_s], promote_options="permissive")
        except pa.ArrowException:
            return False
        return bool(unified.equals(new_s))


@dataclass(frozen=True, slots=True)
class TableSchema:
    """Compiled analytical and operational schema descriptor for a CDC destination."""

    destination: str
    arrow_schema: pa.Schema
    key_fields: tuple[str, ...] = ()
    key_schema: pa.Schema | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.destination, str) or not self.destination.strip():
            raise ValueError("TableSchema destination cannot be empty.")
        stripped = self.destination.strip()
        if stripped != self.destination:
            object.__setattr__(self, "destination", stripped)

    @property
    def namespace(self) -> tuple[str, ...]:
        return tuple(self.destination.split(".")[:-1])

    @property
    def table_name(self) -> str:
        return self.destination.rsplit(".", 1)[-1]

    def diff(self, previous: TableSchema | pa.Schema) -> SchemaDiff:
        """Computes the structural difference from `previous` schema to `self`."""
        old_pa = (
            previous.arrow_schema if isinstance(previous, TableSchema) else previous
        )
        new_pa = self.arrow_schema
        old_map = {f.name: f for f in old_pa}
        new_map = {f.name: f for f in new_pa}

        added = tuple(f for name, f in new_map.items() if name not in old_map)
        removed = tuple(f for name, f in old_map.items() if name not in new_map)
        changed = tuple(
            (old_map[name], new_f)
            for name, new_f in new_map.items()
            if name in old_map
            and (
                old_map[name].type != new_f.type
                or old_map[name].nullable != new_f.nullable
            )
        )
        reordered = [name for name in old_pa.names if name in new_map] != [
            name for name in new_pa.names if name in old_map
        ]
        is_table_schema = isinstance(previous, TableSchema)
        return SchemaDiff(
            added_fields=added,
            removed_fields=removed,
            changed_fields=changed,
            reordered=reordered,
            key_fields_changed=(
                self.key_fields != previous.key_fields if is_table_schema else False
            ),
            key_schema_changed=(
                self.key_schema != previous.key_schema if is_table_schema else False
            ),
        )


@dataclass(frozen=True, slots=True)
class ChangeRecord:
    """Unified domain model representing a single converted CDC change event."""

    destination: str
    value: dict[str, Any] | None = None
    key: dict[str, Any] | None = None
    schema: TableSchema | None = None
    partition: int | None = None
    kind: EventKind = EventKind.DATA

    def __post_init__(self) -> None:
        if not isinstance(self.destination, str) or not self.destination.strip():
            raise ValueError("ChangeRecord destination cannot be empty.")
        stripped = self.destination.strip()
        if stripped != self.destination:
            object.__setattr__(self, "destination", stripped)
        if self.kind is EventKind.TOMBSTONE:
            if self.value is not None:
                raise ValueError("Tombstone ChangeRecord cannot have a non-null value.")
            if not self.key:
                raise ValueError("Tombstone ChangeRecord requires a non-empty key.")
        elif self.value is None:
            raise ValueError(
                f"ChangeRecord with kind '{self.kind.value}' cannot have a null value."
            )


@dataclass(frozen=True, slots=True)
class TableBatch:
    """Homogeneous batch of consecutive DATA ChangeRecords sharing a single TableSchema."""

    schema: TableSchema
    records: tuple[ChangeRecord, ...] = ()

    def __post_init__(self) -> None:
        dest = self.schema.destination
        for record in self.records:
            if record.kind is not EventKind.DATA or record.value is None:
                raise ValueError(
                    f"TableBatch accepts only DATA ChangeRecords with non-null values, got kind '{record.kind.value}'."
                )
            if record.destination != dest:
                raise ValueError(
                    f"TableBatch record destination '{record.destination}' does not match schema destination '{dest}'."
                )

    @property
    def destination(self) -> str:
        return self.schema.destination

    @property
    def rows(self) -> list[dict[str, Any]]:
        return [r.value for r in self.records if r.value is not None]

    def to_arrow(self, target_schema: pa.Schema | None = None) -> pa.Table:
        table = pa.Table.from_pylist(self.rows, schema=self.schema.arrow_schema)
        if target_schema is None or table.schema.equals(
            target_schema, check_metadata=True
        ):
            return table
        arrays: list[pa.Array | pa.ChunkedArray] = []
        for f in target_schema:
            idx = table.schema.get_field_index(f.name)
            if idx != -1:
                col = table.column(idx).cast(f.type)
                _validate_arrow_nullability(col, f, f.name)
                arrays.append(col)
            else:
                if not f.nullable and table.num_rows > 0:
                    raise ValueError(
                        f"Cannot populate missing non-nullable target field '{f.name}' with nulls."
                    )
                arrays.append(pa.nulls(table.num_rows, type=f.type))
        return pa.Table.from_arrays(arrays, schema=target_schema)

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        """Arrow PyCapsule Interface for zero-glue interop with DuckDB, Polars, PyIceberg, and DataFusion."""
        return self.to_arrow().__arrow_c_stream__(requested_schema)

    def __len__(self) -> int:
        return len(self.records)

    def __iter__(self) -> Iterator[ChangeRecord]:
        return iter(self.records)


@dataclass(frozen=True, slots=True)
class ChangeBatch:
    """Ordered collection of converted ChangeRecord items produced from an engine batch."""

    records: tuple[ChangeRecord, ...] = ()

    @property
    def data_records(self) -> tuple[ChangeRecord, ...]:
        return self.of_kind(EventKind.DATA)

    def of_kind(self, kind: EventKind) -> tuple[ChangeRecord, ...]:
        return tuple(r for r in self.records if r.kind is kind)

    def destination_events(self) -> tuple[TableBatch | ChangeRecord, ...]:
        """
        Segments the batch into causal per-destination TableBatch runs and
        lifecycle ChangeRecords (TOMBSTONE, SCHEMA_CHANGE, HEARTBEAT).
        """
        output: list[TableBatch | ChangeRecord] = []
        streams: dict[str, list[TableBatch | ChangeRecord]] = {}
        open_runs: dict[str, tuple[TableSchema, list[ChangeRecord]]] = {}
        heartbeats: list[ChangeRecord] = []

        def flush_destination(dest: str) -> None:
            run = open_runs.pop(dest, None)
            if run is not None:
                streams.setdefault(dest, []).append(
                    TableBatch(schema=run[0], records=tuple(run[1]))
                )

        def flush_all() -> None:
            for dest in list(open_runs):
                flush_destination(dest)
            for items in streams.values():
                output.extend(items)
            streams.clear()

        for record in self.records:
            if record.kind is EventKind.DATA:
                if record.schema is None:
                    raise ValueError(
                        f"Data ChangeRecord for destination '{record.destination}' is missing a TableSchema."
                    )
                dest = record.destination
                streams.setdefault(dest, [])
                current = open_runs.get(dest)
                if current is not None and current[0] == record.schema:
                    current[1].append(record)
                else:
                    flush_destination(dest)
                    open_runs[dest] = (record.schema, [record])
            elif record.kind is EventKind.TOMBSTONE:
                dest = record.destination
                flush_destination(dest)
                streams.setdefault(dest, []).append(record)
            elif record.kind is EventKind.SCHEMA_CHANGE:
                flush_all()
                output.append(record)
            elif record.kind is EventKind.HEARTBEAT:
                heartbeats.append(record)

        flush_all()
        output.extend(heartbeats)
        return tuple(output)

    def table_batches(self) -> tuple[TableBatch, ...]:
        return tuple(
            item for item in self.destination_events() if isinstance(item, TableBatch)
        )

    def __iter__(self) -> Iterator[ChangeRecord]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)
