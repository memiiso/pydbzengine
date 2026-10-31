from __future__ import annotations

from collections.abc import Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import Any

import pyarrow as pa

from pydbzengine.schema.types import (
    DataTypeHandler,
    FieldSpec,
    MapTypeHandler,
    SchemaField,
    StructTypeHandler,
    TypeRegistry,
)

__all__ = [
    "DDL_MARKER_FIELDS",
    "INFERRED_NULL_META_KEY",
    "ChangeBatch",
    "ChangeOp",
    "ChangeRecord",
    "EventKind",
    "SchemaDiff",
    "TableBatch",
    "TableSchema",
]

DDL_MARKER_FIELDS: frozenset[str] = frozenset({"ddl", "databaseName", "tableChanges"})
INFERRED_NULL_META_KEY = b"debezium.inferred_null"


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
        field_names: Collection[str],
        heartbeat_prefix: str,
    ) -> EventKind:
        if destination.startswith(heartbeat_prefix):
            return cls.HEARTBEAT
        if DDL_MARKER_FIELDS.issubset(field_names):
            return cls.SCHEMA_CHANGE
        return cls.DATA


class ChangeOp(str, Enum):
    """Debezium CDC operation type."""

    INSERT = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"
    TRUNCATE = "t"
    MESSAGE = "m"


@dataclass(frozen=True, slots=True)
class SchemaDiff:
    """Describes structural differences between a previous schema and a new schema."""

    added_fields: tuple[pa.Field, ...] = ()
    removed_fields: tuple[pa.Field, ...] = ()
    changed_fields: tuple[tuple[pa.Field, pa.Field], ...] = ()

    @property
    def has_changes(self) -> bool:
        return bool(self.added_fields or self.removed_fields or self.changed_fields)

    @property
    def is_additive(self) -> bool:
        return (
            bool(self.added_fields)
            and not self.removed_fields
            and not self.changed_fields
        )

    @property
    def is_compatible(self) -> bool:
        return not self.removed_fields and all(
            TableSchema.is_inferred_null_field(old_f)
            for old_f, _ in self.changed_fields
        )


@dataclass(frozen=True, eq=False)
class TableSchema:
    """
    Compiled analytical and operational schema descriptor for a CDC destination.
    Combines PyArrow value/key/row schemas with primary keys, event kind, and
    bound DataTypeHandler trees for single-pass JSON and Connect conversion.
    """

    destination: str
    kind: EventKind
    arrow_schema: pa.Schema
    primary_keys: tuple[str, ...] = ()
    key_schema: pa.Schema | None = None
    value_handler: StructTypeHandler | None = None
    key_handler: DataTypeHandler | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.destination, str) or not self.destination.strip():
            raise ValueError("TableSchema destination cannot be empty.")
        if not isinstance(self.kind, EventKind):
            raise TypeError(
                f"Expected EventKind for TableSchema.kind, got {type(self.kind).__name__}"
            )
        if not isinstance(self.arrow_schema, pa.Schema):
            raise TypeError(
                f"Expected pa.Schema for TableSchema.arrow_schema, got {type(self.arrow_schema).__name__}"
            )
        if len(self.arrow_schema) == 0:
            raise ValueError(
                f"TableSchema for destination '{self.destination}' must contain at least one field."
            )
        if self.key_schema is not None:
            if not isinstance(self.key_schema, pa.Schema):
                raise TypeError(
                    f"Expected pa.Schema for TableSchema.key_schema, got {type(self.key_schema).__name__}"
                )
            if len(self.key_schema) == 0:
                raise ValueError(
                    f"TableSchema key_schema for destination '{self.destination}' cannot be empty."
                )

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, TableSchema):
            return NotImplemented
        key_eq = (self.key_schema is None and other.key_schema is None) or (
            self.key_schema is not None
            and other.key_schema is not None
            and self.key_schema.equals(other.key_schema, check_metadata=True)
        )
        return (
            self.destination == other.destination
            and self.kind == other.kind
            and self.primary_keys == other.primary_keys
            and self.arrow_schema.equals(other.arrow_schema, check_metadata=True)
            and key_eq
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.destination,
                self.kind,
                self.arrow_schema,
                self.primary_keys,
                self.key_schema,
            )
        )

    @property
    def value_schema(self) -> pa.Schema:
        return self.arrow_schema

    @property
    def names(self) -> list[str]:
        return self.arrow_schema.names

    @property
    def table_name(self) -> str:
        return self.destination.rsplit(".", 1)[-1]

    @cached_property
    def fields(self) -> tuple[SchemaField, ...]:
        if self.value_handler is None:
            return ()
        return self.value_handler.fields

    @cached_property
    def row_schema(self) -> pa.Schema:
        """
        Returns the unpacked table row schema when the event uses a Debezium
        envelope (before/after/op), or `arrow_schema` for flattened events.
        """
        return self.extract_row_schema(self.arrow_schema)

    @cached_property
    def is_envelope(self) -> bool:
        return self.row_schema is not self.arrow_schema

    @classmethod
    def is_inferred_null_field(cls, field_obj: pa.Field) -> bool:
        if (
            field_obj.metadata
            and field_obj.metadata.get(INFERRED_NULL_META_KEY) == b"true"
        ):
            return True
        if pa.types.is_struct(field_obj.type):
            return any(cls.is_inferred_null_field(sf) for sf in field_obj.type)
        if pa.types.is_list(field_obj.type):
            return cls.is_inferred_null_field(field_obj.type.value_field)
        if pa.types.is_map(field_obj.type):
            return cls.is_inferred_null_field(field_obj.type.item_field)
        return False

    @staticmethod
    def extract_row_schema(schema: pa.Schema) -> pa.Schema:
        names = set(schema.names)
        if "op" in names:
            for candidate in ("after", "before"):
                if candidate in names:
                    f_type = schema.field(candidate).type
                    if pa.types.is_struct(f_type):
                        return pa.schema(list(f_type))
        return schema

    @staticmethod
    def select_primary_keys(
        primary_keys: tuple[str, ...],
        fields: Sequence[pa.Field],
    ) -> tuple[str, ...]:
        candidate_fields: dict[str, pa.DataType] = {}
        is_env = any(f.name == "op" for f in fields) and any(
            f.name in ("after", "before") and pa.types.is_struct(f.type) for f in fields
        )
        if is_env:
            for f in fields:
                if f.name in ("after", "before") and pa.types.is_struct(f.type):
                    for sub_f in f.type:
                        candidate_fields.setdefault(sub_f.name, sub_f.type)
        else:
            for f in fields:
                candidate_fields[f.name] = f.type

        valid_pks = tuple(pk for pk in primary_keys if pk in candidate_fields)
        for pk in valid_pks:
            pk_type = candidate_fields[pk]
            if pa.types.is_nested(pk_type):
                raise ValueError(
                    f"Complex field '{pk}' of type {pk_type} cannot be part of the primary key."
                )
        return valid_pks

    def diff(
        self,
        previous: TableSchema | pa.Schema,
        *,
        use_row_schema: bool = True,
    ) -> SchemaDiff:
        """Computes the structural difference from `previous` schema to `self`."""
        old_pa = (
            (previous.row_schema if use_row_schema else previous.arrow_schema)
            if isinstance(previous, TableSchema)
            else (self.extract_row_schema(previous) if use_row_schema else previous)
        )
        new_pa = self.row_schema if use_row_schema else self.arrow_schema

        old_map: dict[str, pa.Field] = {f.name: f for f in old_pa}
        new_map: dict[str, pa.Field] = {f.name: f for f in new_pa}

        added = tuple(f for name, f in new_map.items() if name not in old_map)
        removed = tuple(f for name, f in old_map.items() if name not in new_map)
        changed = tuple(
            (old_map[name], new_f)
            for name, new_f in new_map.items()
            if name in old_map and old_map[name].type != new_f.type
        )
        return SchemaDiff(
            added_fields=added,
            removed_fields=removed,
            changed_fields=changed,
        )

    def convert_json_value(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if self.value_handler is None:
            raise ValueError(
                f"TableSchema for destination '{self.destination}' has no compiled value_handler."
            )
        return self.value_handler.convert_json(payload)

    def convert_connect_value(self, struct_val: Any) -> dict[str, Any]:
        if self.value_handler is None:
            raise ValueError(
                f"TableSchema for destination '{self.destination}' has no compiled value_handler."
            )
        return self.value_handler.convert_connect(struct_val)

    @classmethod
    def convert_json_key_with_handler(
        cls,
        key_payload: Mapping[str, Any],
        *,
        key_handler: DataTypeHandler | None,
        key_schema: pa.Schema | None = None,
    ) -> dict[str, Any]:
        if not isinstance(key_payload, Mapping):
            raise TypeError(
                f"Expected Mapping for JSON key payload, got {type(key_payload).__name__}: {key_payload!r}"
            )
        if isinstance(key_handler, StructTypeHandler):
            return key_handler.convert_json(key_payload)
        if isinstance(key_handler, MapTypeHandler):
            converted = key_handler.convert_json(key_payload)
            return {str(k): v for k, v in converted.items()}
        if key_handler is not None:
            key_name = (
                key_schema[0].name
                if key_schema is not None and len(key_schema) > 0
                else "key"
            )
            if key_name in key_payload:
                return {key_name: key_handler.convert_json(key_payload[key_name])}
            if len(key_payload) == 1:
                return {
                    str(k): key_handler.convert_json(v) for k, v in key_payload.items()
                }
            raise ValueError(
                f"Scalar key handler expected field '{key_name}', got {list(key_payload.keys())}"
            )
        return dict(key_payload)

    def convert_json_key(
        self, key_payload: Mapping[str, Any] | None
    ) -> dict[str, Any] | None:
        if key_payload is None:
            return None
        return self.convert_json_key_with_handler(
            key_payload,
            key_handler=self.key_handler,
            key_schema=self.key_schema,
        )

    @classmethod
    def convert_connect_key_with_handler(
        cls,
        key_val: Any,
        *,
        key_handler: DataTypeHandler | None,
        key_schema: pa.Schema | None,
        destination: str,
    ) -> dict[str, Any]:
        if isinstance(key_handler, StructTypeHandler):
            return key_handler.convert_connect(key_val)
        if isinstance(key_handler, MapTypeHandler):
            converted = key_handler.convert_connect(key_val)
            return {str(k): v for k, v in converted.items()}
        if key_handler is not None:
            key_name = (
                key_schema[0].name
                if key_schema is not None and len(key_schema) > 0
                else "key"
            )
            return {key_name: key_handler.convert_connect(key_val)}
        if hasattr(key_val, "schema"):
            ks = key_val.schema()
            if ks is None:
                raise ValueError(
                    f"Connect key Struct has None schema() for destination '{destination}'"
                )
            return (
                TypeRegistry()
                .compile_struct(FieldSpec.from_connect(ks, field_name="key"))
                .convert_connect(key_val)
            )
        raise TypeError(
            f"Unsupported Connect key type {type(key_val).__name__} without key schema "
            f"for destination '{destination}'"
        )

    def convert_connect_key(self, key_val: Any) -> dict[str, Any] | None:
        if key_val is None:
            return None
        return self.convert_connect_key_with_handler(
            key_val,
            key_handler=self.key_handler,
            key_schema=self.key_schema,
            destination=self.destination,
        )


@dataclass(frozen=True, slots=True)
class ChangeRecord:
    """
    Unified domain model representing a single converted CDC change event
    (data records, heartbeats, DDL schema changes, and delete tombstones).
    """

    schema: TableSchema | None = None
    value: dict[str, Any] | None = None
    key: dict[str, Any] | None = None
    partition: int | None = None
    destination: str = ""

    def __post_init__(self) -> None:
        if self.schema is not None and not isinstance(self.schema, TableSchema):
            raise TypeError(
                f"Expected TableSchema or None for ChangeRecord.schema, got {type(self.schema).__name__}"
            )
        if self.value is not None and not isinstance(self.value, dict):
            raise TypeError(
                f"Expected dict or None for ChangeRecord.value, got {type(self.value).__name__}"
            )
        if self.value is not None and self.schema is None:
            raise ValueError(
                "Non-tombstone ChangeRecord (value is not None) requires a TableSchema."
            )
        if self.key is not None and not isinstance(self.key, dict):
            raise TypeError(
                f"Expected dict or None for ChangeRecord.key, got {type(self.key).__name__}"
            )
        if self.partition is not None and (
            isinstance(self.partition, bool) or not isinstance(self.partition, int)
        ):
            raise TypeError(
                f"Expected int or None for ChangeRecord.partition, got {type(self.partition).__name__}"
            )
        if not isinstance(self.destination, str):
            raise TypeError(
                f"Expected str for ChangeRecord.destination, got {type(self.destination).__name__}"
            )
        resolved_dest = self.destination.strip()
        if self.schema is not None:
            if resolved_dest and resolved_dest != self.schema.destination:
                raise ValueError(
                    f"ChangeRecord destination '{resolved_dest}' does not match "
                    f"TableSchema destination '{self.schema.destination}'."
                )
            object.__setattr__(self, "destination", self.schema.destination)
        else:
            if not resolved_dest:
                raise ValueError(
                    "Tombstone ChangeRecord without TableSchema requires a non-empty destination."
                )
            object.__setattr__(self, "destination", resolved_dest)

    @property
    def row(self) -> dict[str, Any] | None:
        return self.value

    @property
    def kind(self) -> EventKind:
        if self.value is None:
            return EventKind.TOMBSTONE
        assert self.schema is not None
        return self.schema.kind

    @property
    def op(self) -> ChangeOp | None:
        """Debezium operation enum (INSERT, UPDATE, DELETE, READ, TRUNCATE, MESSAGE) if present."""
        if self.value is None:
            return None
        val = self.value.get("op")
        if val is None:
            val = self.value.get("__op")
        if val is None:
            return None
        if isinstance(val, ChangeOp):
            return val
        if not isinstance(val, str):
            raise TypeError(
                f"Expected str for operation code 'op', got {type(val).__name__}: {val!r}"
            )
        try:
            return ChangeOp(val)
        except ValueError as err:
            raise ValueError(f"Unsupported Debezium operation code: {val!r}") from err

    @property
    def before(self) -> dict[str, Any] | None:
        """Before-state payload dictionary if present in envelope value."""
        if self.value is None:
            return None
        val = self.value.get("before")
        if val is None:
            return None
        if not isinstance(val, dict):
            raise TypeError(
                f"Expected dict for 'before' state, got {type(val).__name__}: {val!r}"
            )
        return val

    @property
    def after(self) -> dict[str, Any] | None:
        """After-state payload dictionary if present in envelope value."""
        if self.value is None:
            return None
        val = self.value.get("after")
        if val is None:
            return None
        if not isinstance(val, dict):
            raise TypeError(
                f"Expected dict for 'after' state, got {type(val).__name__}: {val!r}"
            )
        return val

    @property
    def source(self) -> dict[str, Any] | None:
        """Debezium source metadata dictionary if present in envelope value."""
        if self.value is None:
            return None
        val = self.value.get("source")
        if val is None:
            return None
        if not isinstance(val, dict):
            raise TypeError(
                f"Expected dict for 'source' metadata, got {type(val).__name__}: {val!r}"
            )
        return val

    @property
    def data(self) -> dict[str, Any] | None:
        """
        Returns the active table row dictionary: `after` (or `before` on delete)
        for envelope events, or `value` directly for flattened events.
        """
        if self.value is None:
            return None
        assert self.schema is not None
        if self.schema.is_envelope:
            return self.after if self.after is not None else self.before
        return self.value


@dataclass(frozen=True, slots=True)
class TableBatch:
    """A homogeneous batch of consecutive ChangeRecords sharing a single TableSchema."""

    schema: TableSchema
    records: tuple[ChangeRecord, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.schema, TableSchema):
            raise TypeError(
                f"Expected TableSchema for TableBatch.schema, got {type(self.schema).__name__}"
            )
        for r in self.records:
            if r.value is None:
                raise ValueError(
                    f"TableBatch for destination '{self.destination}' cannot contain tombstone records."
                )
            if r.destination != self.schema.destination:
                raise ValueError(
                    f"TableBatch destination mismatch: expected '{self.schema.destination}', got '{r.destination}'."
                )
            if r.schema != self.schema:
                raise ValueError(
                    f"TableBatch schema mismatch for destination '{self.destination}'."
                )

    @property
    def destination(self) -> str:
        return self.schema.destination

    @property
    def rows(self) -> list[dict[str, Any]]:
        return [r.value for r in self.records]  # type: ignore[misc]

    @property
    def data_rows(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for r in self.records:
            row_data = r.data
            if row_data is None:
                raise ValueError(
                    f"Cannot extract row data from record with neither 'after' nor 'before' state "
                    f"for destination '{self.destination}'."
                )
            result.append(row_data)
        return result

    def to_arrow(
        self,
        target_schema: pa.Schema | None = None,
        *,
        extract_rows: bool = False,
    ) -> pa.Table:
        if extract_rows:
            resolved = (
                self.schema.row_schema if target_schema is None else target_schema
            )
            return pa.Table.from_pylist(self.data_rows, schema=resolved)
        resolved = self.schema.arrow_schema if target_schema is None else target_schema
        return pa.Table.from_pylist(self.rows, schema=resolved)

    def __len__(self) -> int:
        return len(self.records)

    def __iter__(self) -> Iterator[ChangeRecord]:
        return iter(self.records)


@dataclass(frozen=True, slots=True)
class ChangeBatch:
    """
    Ordered collection of converted ChangeRecord items produced from an engine batch.
    Preserves arrival order across all event kinds (data, heartbeat, schema_change, tombstone).
    """

    records: tuple[ChangeRecord, ...] = ()

    def __post_init__(self) -> None:
        for r in self.records:
            if not isinstance(r, ChangeRecord):
                raise TypeError(
                    f"Expected ChangeRecord in ChangeBatch.records, got {type(r).__name__}"
                )

    @property
    def data_records(self) -> tuple[ChangeRecord, ...]:
        return self.of_kind(EventKind.DATA)

    def of_kind(self, kind: EventKind) -> tuple[ChangeRecord, ...]:
        """Returns all records matching the requested EventKind in arrival order."""
        return tuple(r for r in self.records if r.kind is kind)

    def table_batches(
        self,
        kind: EventKind = EventKind.DATA,
    ) -> tuple[TableBatch, ...]:
        """
        Groups records of `kind` by destination in arrival order and slices them
        into homogeneous TableBatch runs whenever the TableSchema changes.
        """
        if kind is EventKind.TOMBSTONE:
            raise ValueError(
                "Cannot build TableBatch runs for EventKind.TOMBSTONE; use of_kind(EventKind.TOMBSTONE)."
            )
        runs_by_destination: dict[
            str, list[tuple[TableSchema, list[ChangeRecord]]]
        ] = {}
        for record in self.of_kind(kind):
            assert record.schema is not None
            runs = runs_by_destination.setdefault(record.destination, [])
            if runs and runs[-1][0] == record.schema:
                runs[-1][1].append(record)
            else:
                runs.append((record.schema, [record]))
        return tuple(
            TableBatch(schema=schema, records=tuple(rec_list))
            for runs in runs_by_destination.values()
            for schema, rec_list in runs
        )

    def __iter__(self) -> Iterator[ChangeRecord]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)
