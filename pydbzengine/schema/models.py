from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc

if TYPE_CHECKING:
    from pydbzengine.schema.converter import SchemaConverter

__all__ = [
    "DebeziumConfig",
    "DeleteHandlingMode",
    "EventFormat",
    "EventStructure",
    "SchemaDiff",
    "TableSchema",
]


@dataclass(frozen=True)
class SchemaDiff:
    """
    Structured schema comparison result between an existing target schema
    and an incoming CDC schema.
    """

    added_fields: tuple[pa.Field, ...] = ()
    removed_fields: tuple[pa.Field, ...] = ()
    type_changes: tuple[tuple[pa.Field, pa.Field], ...] = ()  # (old_field, new_field)
    nullability_changes: tuple[tuple[pa.Field, pa.Field], ...] = ()

    @property
    def has_changes(self) -> bool:
        """True if any structural, type, or nullability difference exists."""
        return bool(
            self.added_fields
            or self.removed_fields
            or self.type_changes
            or self.nullability_changes
        )

    @property
    def has_expansion(self) -> bool:
        """
        True if the target table schema needs to be expanded:
        - New columns were added in the incoming schema.
        - Existing column types changed/widened.
        - A previously non-nullable target column became nullable in the incoming schema.
        """
        has_relaxed_nullability = any(
            not old_f.nullable and new_f.nullable
            for old_f, new_f in self.nullability_changes
        )
        return bool(self.added_fields or self.type_changes or has_relaxed_nullability)


@dataclass(frozen=True, eq=False)
class TableSchema:
    """
    Compiled, schema-aware domain descriptor for a CDC destination table.
    Combines a standard PyArrow Schema with primary keys, sparse value coercers,
    and downstream schema evolution and table alignment operations.

    Hashable and comparable by `fingerprint` so instances can be used as dictionary
    keys when grouping records by schema version.
    """

    destination: str
    arrow_schema: pa.Schema
    primary_keys: tuple[str, ...] = ()
    row_coercers: tuple[tuple[str, Callable[[Any], Any]], ...] = ()

    @cached_property
    def fingerprint(self) -> str:
        """Deterministic SHA-256 fingerprint of destination, primary keys, and Arrow fields."""
        fields_payload = []
        for f in self.arrow_schema:
            f_dict: dict[str, Any] = {
                "name": f.name,
                "type": str(f.type),
                "nullable": f.nullable,
            }
            if f.metadata:
                f_dict["metadata"] = {
                    k.decode("utf-8"): v.decode("utf-8")
                    for k, v in sorted(f.metadata.items())
                }
            fields_payload.append(f_dict)
        payload = {
            "destination": self.destination,
            "fields": fields_payload,
            "primary_keys": sorted(self.primary_keys),
        }
        canonical_json = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, TableSchema):
            return NotImplemented
        return self.fingerprint == other.fingerprint

    def __hash__(self) -> int:
        return hash(self.fingerprint)

    @property
    def column_names(self) -> list[str]:
        return self.arrow_schema.names

    @property
    def table_name(self) -> str:
        return self.destination.rsplit(".", 1)[-1]

    @property
    def schema_name(self) -> str:
        parts = self.destination.split(".")
        return parts[-2] if len(parts) >= 2 else ""

    @property
    def namespace(self) -> str:
        s = self.schema_name
        t = self.table_name
        return f"{s}.{t}" if s else t

    def field(self, name: str) -> pa.Field | None:
        idx = self.arrow_schema.get_field_index(name)
        return self.arrow_schema.field(idx) if idx != -1 else None

    def same_schema(self, other: TableSchema | pa.Schema) -> bool:
        if isinstance(other, TableSchema):
            return self.fingerprint == other.fingerprint
        return self.arrow_schema.equals(other)

    def coerce_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Mutates only the fields in `row` that require wire-format decoding and returns `row`."""
        for col_name, coerce_fn in self.row_coercers:
            val = row.get(col_name)
            if val is not None:
                row[col_name] = coerce_fn(val)
        return row

    def to_arrow(
        self,
        rows: Sequence[dict[str, Any]],
        *,
        coerce: bool = True,
        target_schema: pa.Schema | TableSchema | None = None,
    ) -> pa.Table:
        """
        Converts a sequence of row dicts into a `pa.Table` typed with `self.arrow_schema`.
        When `coerce=True`, applies sparse wire coercion in-place before building the Arrow table.
        When `target_schema` is provided, aligns the resulting table to `target_schema`.
        """
        if coerce and self.row_coercers:
            for row in rows:
                self.coerce_row(row)
        table = pa.Table.from_pylist(list(rows), schema=self.arrow_schema)
        if target_schema is not None:
            return self.align_table(table, target_schema)
        return table

    def diff_from(self, target_schema: pa.Schema | TableSchema) -> SchemaDiff:
        """
        Computes the structural difference from `target_schema` (old/target)
        to `self.arrow_schema` (new/incoming).
        """
        old_pa = (
            target_schema.arrow_schema
            if isinstance(target_schema, TableSchema)
            else target_schema
        )
        new_pa = self.arrow_schema

        old_map = {f.name: f for f in old_pa}
        new_map = {f.name: f for f in new_pa}

        added = tuple(f for name, f in new_map.items() if name not in old_map)
        removed = tuple(f for name, f in old_map.items() if name not in new_map)
        type_changes: list[tuple[pa.Field, pa.Field]] = []
        null_changes: list[tuple[pa.Field, pa.Field]] = []

        for name, new_f in new_map.items():
            if name in old_map:
                old_f = old_map[name]
                if not self._normalize_type(old_f.type).equals(
                    self._normalize_type(new_f.type)
                ):
                    type_changes.append((old_f, new_f))
                if old_f.nullable != new_f.nullable:
                    null_changes.append((old_f, new_f))

        return SchemaDiff(
            added_fields=added,
            removed_fields=removed,
            type_changes=tuple(type_changes),
            nullability_changes=tuple(null_changes),
        )

    def expand_target_schema(self, target_schema: pa.Schema | TableSchema) -> pa.Schema:
        """
        Non-destructively expands `target_schema` with new fields and widened types
        from `self.arrow_schema` (union-by-name semantics for Lakehouse / Parquet / Iceberg):
        - Existing fields in `target_schema` keep their position.
        - Fields removed in `self` become nullable in the expanded target schema.
        - Widened types (e.g. int32 -> int64, float32 -> float64, null -> T) are promoted.
        - New fields in `self` are appended as nullable.
        """
        old_pa = (
            target_schema.arrow_schema
            if isinstance(target_schema, TableSchema)
            else target_schema
        )
        new_pa = self.arrow_schema

        new_map = {f.name: f for f in new_pa}
        expanded_fields: list[pa.Field] = []

        for old_f in old_pa:
            if old_f.name not in new_map:
                expanded_fields.append(old_f.with_nullable(True))
            else:
                new_f = new_map[old_f.name]
                promoted_type = self._promote_type(old_f.type, new_f.type)
                expanded_fields.append(
                    old_f.with_type(promoted_type).with_nullable(
                        old_f.nullable or new_f.nullable
                    )
                )

        existing_names = {f.name for f in old_pa}
        for new_f in new_pa:
            if new_f.name not in existing_names:
                expanded_fields.append(new_f.with_nullable(True))

        return pa.schema(expanded_fields, metadata=new_pa.metadata or old_pa.metadata)

    def evolve(self, incoming: TableSchema) -> TableSchema:
        """
        Returns a new `TableSchema` that non-destructively expands `self` with
        new fields, widened types, and row coercers from `incoming`.
        """
        if self == incoming:
            return self
        expanded_pa = incoming.expand_target_schema(self.arrow_schema)
        merged_coercers = dict(self.row_coercers)
        merged_coercers.update(incoming.row_coercers)
        return TableSchema(
            destination=incoming.destination,
            arrow_schema=expanded_pa,
            primary_keys=incoming.primary_keys or self.primary_keys,
            row_coercers=tuple(merged_coercers.items()),
        )

    def align_table(
        self,
        table: pa.Table,
        target_schema: pa.Schema | TableSchema | None = None,
    ) -> pa.Table:
        """
        Aligns an Arrow `pa.Table` to match `target_schema` (or `self.arrow_schema` if omitted) in C++:
        - Adds missing target columns as typed NULL arrays (e.g., dropped source columns).
        - Casts columns (including nested structs and lists) whose types were widened in `target_schema`.
        - Reorders columns to match `target_schema`.
        """
        resolved_target = target_schema if target_schema is not None else self
        target_pa = (
            resolved_target.arrow_schema
            if isinstance(resolved_target, TableSchema)
            else resolved_target
        )
        if table.schema.equals(target_pa):
            return table

        num_rows = table.num_rows
        columns: list[pa.Array | pa.ChunkedArray] = []
        src_names = set(table.schema.names)

        for target_field in target_pa:
            if target_field.name in src_names:
                col = table.column(target_field.name)
                columns.append(self._align_column(col, target_field.type, num_rows))
            else:
                columns.append(pa.nulls(num_rows, type=target_field.type))

        return pa.Table.from_arrays(columns, schema=target_pa)

    @staticmethod
    def _align_column(
        col: pa.Array | pa.ChunkedArray,
        target_type: pa.DataType,
        num_rows: int,
    ) -> pa.Array | pa.ChunkedArray:
        if col.type.equals(target_type):
            return col
        if pa.types.is_null(col.type):
            return pa.nulls(num_rows, type=target_type)
        if pa.types.is_struct(col.type) and pa.types.is_struct(target_type):
            arr = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
            if len(arr) == 0:
                return pa.nulls(0, type=target_type)
            src_sub_names = {f.name for f in arr.type}
            sub_arrays: list[pa.Array] = []
            for sub_f in target_type:
                if sub_f.name in src_sub_names:
                    sub_col = arr.field(sub_f.name)
                    aligned_sub = TableSchema._align_column(
                        sub_col, sub_f.type, len(arr)
                    )
                    sub_arrays.append(
                        aligned_sub.combine_chunks()
                        if isinstance(aligned_sub, pa.ChunkedArray)
                        else aligned_sub
                    )
                else:
                    sub_arrays.append(pa.nulls(len(arr), type=sub_f.type))
            return pa.StructArray.from_arrays(
                sub_arrays,
                fields=list(target_type),
                mask=arr.is_null(),
            )
        if (pa.types.is_list(col.type) and pa.types.is_list(target_type)) or (
            pa.types.is_large_list(col.type) and pa.types.is_large_list(target_type)
        ):
            arr = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
            if len(arr) == 0:
                return pa.nulls(0, type=target_type)
            aligned_vals = TableSchema._align_column(
                arr.values, target_type.value_type, len(arr.values)
            )
            vals_arr = (
                aligned_vals.combine_chunks()
                if isinstance(aligned_vals, pa.ChunkedArray)
                else aligned_vals
            )
            list_cls = (
                pa.LargeListArray
                if pa.types.is_large_list(target_type)
                else pa.ListArray
            )
            return list_cls.from_arrays(
                arr.offsets,
                vals_arr,
                type=target_type,
                mask=arr.is_null(),
            )
        return pc.cast(col, target_type)

    @staticmethod
    def _normalize_type(t: pa.DataType) -> pa.DataType:
        """
        Normalizes Arrow large_string/large_binary/large_list variants (used by PyIceberg)
        to standard Arrow types so schema comparisons only report genuine type changes.
        """
        if pa.types.is_large_string(t):
            return pa.string()
        if pa.types.is_large_binary(t):
            return pa.binary()
        if pa.types.is_large_list(t) or pa.types.is_list(t):
            return pa.list_(TableSchema._normalize_type(t.value_type))
        if pa.types.is_struct(t):
            return pa.struct(
                [
                    pa.field(
                        f.name, TableSchema._normalize_type(f.type), nullable=f.nullable
                    )
                    for f in t
                ]
            )
        if pa.types.is_map(t):
            return pa.map_(
                TableSchema._normalize_type(t.key_type),
                TableSchema._normalize_type(t.item_type),
            )
        return t

    @staticmethod
    def _promote_type(old_t: pa.DataType, new_t: pa.DataType) -> pa.DataType:
        """Losslessly widens `old_t` and `new_t` or raises TypeError if incompatible."""
        if old_t.equals(new_t) or TableSchema._normalize_type(old_t).equals(
            TableSchema._normalize_type(new_t)
        ):
            return old_t
        if pa.types.is_null(old_t):
            return new_t
        if pa.types.is_null(new_t):
            return old_t
        if pa.types.is_signed_integer(old_t) and pa.types.is_signed_integer(new_t):
            return new_t if new_t.bit_width > old_t.bit_width else old_t
        if pa.types.is_unsigned_integer(old_t) and pa.types.is_unsigned_integer(new_t):
            return new_t if new_t.bit_width > old_t.bit_width else old_t
        if pa.types.is_floating(old_t) and pa.types.is_floating(new_t):
            return new_t if new_t.bit_width > old_t.bit_width else old_t
        if pa.types.is_date(old_t) and pa.types.is_date(new_t):
            return new_t if new_t.bit_width > old_t.bit_width else old_t
        if (pa.types.is_time32(old_t) or pa.types.is_time64(old_t)) and (
            pa.types.is_time32(new_t) or pa.types.is_time64(new_t)
        ):
            unit_order = {"s": 1, "ms": 2, "us": 3, "ns": 4}
            return new_t if unit_order[new_t.unit] > unit_order[old_t.unit] else old_t
        if pa.types.is_timestamp(old_t) and pa.types.is_timestamp(new_t):
            if old_t.tz == new_t.tz:
                unit_order = {"s": 1, "ms": 2, "us": 3, "ns": 4}
                return (
                    new_t if unit_order[new_t.unit] > unit_order[old_t.unit] else old_t
                )
        if pa.types.is_decimal(old_t) and pa.types.is_decimal(new_t):
            scale = max(old_t.scale, new_t.scale)
            int_digits = max(
                old_t.precision - old_t.scale, new_t.precision - new_t.scale
            )
            precision = int_digits + scale
            if precision > 38:
                raise ValueError(
                    f"Cannot promote decimal types {old_t} and {new_t}: "
                    f"combined precision {precision} exceeds maximum 38"
                )
            return pa.decimal128(precision, scale)
        if pa.types.is_struct(old_t) and pa.types.is_struct(new_t):
            new_sub_map = {f.name: f for f in new_t}
            merged_sub: list[pa.Field] = []
            for old_sub in old_t:
                if old_sub.name not in new_sub_map:
                    merged_sub.append(old_sub.with_nullable(True))
                else:
                    new_sub = new_sub_map[old_sub.name]
                    merged_sub.append(
                        old_sub.with_type(
                            TableSchema._promote_type(old_sub.type, new_sub.type)
                        ).with_nullable(old_sub.nullable or new_sub.nullable)
                    )
            old_sub_names = {f.name for f in old_t}
            for new_sub in new_t:
                if new_sub.name not in old_sub_names:
                    merged_sub.append(new_sub.with_nullable(True))
            return pa.struct(merged_sub)
        if (pa.types.is_list(old_t) or pa.types.is_large_list(old_t)) and (
            pa.types.is_list(new_t) or pa.types.is_large_list(new_t)
        ):
            promoted_elem = TableSchema._promote_type(
                old_t.value_type, new_t.value_type
            )
            return (
                pa.large_list(promoted_elem)
                if pa.types.is_large_list(old_t)
                else pa.list_(promoted_elem)
            )
        if pa.types.is_map(old_t) and pa.types.is_map(new_t):
            return pa.map_(
                TableSchema._promote_type(old_t.key_type, new_t.key_type),
                TableSchema._promote_type(old_t.item_type, new_t.item_type),
            )
        raise TypeError(f"Cannot promote incompatible Arrow types: {old_t} -> {new_t}")


class EventFormat(str, Enum):
    """Wire serialization format produced by the Debezium engine."""

    JSON = "json"
    CONNECT = "connect"


class EventStructure(str, Enum):
    """Structural envelope layout of change data capture records."""

    ENVELOPE = "envelope"
    FLATTENED = "flattened"


class DeleteHandlingMode(str, Enum):
    """Delete handling strategy when flattening CDC envelope events."""

    DROP = "drop"
    REWRITE = "rewrite"
    NONE = "none"


@dataclass(frozen=True)
class DebeziumConfig:
    """
    Immutable configuration capturing Debezium engine, converter, and transform specifications.
    """

    format: EventFormat = EventFormat.JSON
    structure: EventStructure = EventStructure.ENVELOPE
    schemas_enabled: bool = True
    delete_handling_mode: DeleteHandlingMode = DeleteHandlingMode.DROP
    topic_prefix: str = ""
    connector_type: str = ""
    default_variable_scale: int = 10
    connector_config: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def is_flattened(self) -> bool:
        """Returns True if Debezium SMT unwrap flattening is enabled."""
        return self.structure == EventStructure.FLATTENED

    @property
    def is_envelope(self) -> bool:
        """Returns True if records retain standard Debezium envelope structure."""
        return self.structure == EventStructure.ENVELOPE

    @property
    def is_json(self) -> bool:
        """Returns True if engine emits JSON records."""
        return self.format == EventFormat.JSON

    @property
    def is_connect(self) -> bool:
        """Returns True if engine emits Java Kafka Connect records."""
        return self.format == EventFormat.CONNECT

    @classmethod
    def from_dict(
        cls,
        config: Mapping[str, Any],
        format: str | EventFormat = EventFormat.JSON,
        *,
        structure: EventStructure | str | None = None,
        schemas_enabled: bool | None = None,
        default_variable_scale: int = 10,
    ) -> DebeziumConfig:
        """
        Builds a DebeziumConfig from a connector configuration dictionary, automatically
        detecting SMT unwrap transforms, converter schemas, topic prefix, and connector dialect.
        """
        raw = dict(config)

        fmt = (
            format
            if isinstance(format, EventFormat)
            else EventFormat(format.strip().lower())
        )

        delete_mode = DeleteHandlingMode.DROP
        if structure is not None:
            struct = (
                structure
                if isinstance(structure, EventStructure)
                else EventStructure(structure.strip().lower())
            )
        else:
            struct = EventStructure.ENVELOPE
            transforms = str(raw.get("transforms", "")).split(",")
            for t in transforms:
                t_name = t.strip()
                if not t_name:
                    continue
                t_type = str(raw.get(f"transforms.{t_name}.type", "")).strip()
                if t_type in (
                    "io.debezium.transforms.ExtractNewRecordState",
                    "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
                ):
                    struct = EventStructure.FLATTENED
                    raw_del_mode = raw.get(
                        f"transforms.{t_name}.delete.tombstone.handling.mode",
                        raw.get(f"transforms.{t_name}.delete.handling.mode", "drop"),
                    )
                    mode_str = str(raw_del_mode).strip().lower()
                    if mode_str == "rewrite-with-tombstone":
                        mode_str = "rewrite"
                    elif mode_str == "tombstone":
                        mode_str = "drop"
                    try:
                        delete_mode = DeleteHandlingMode(mode_str)
                    except ValueError as err:
                        raise ValueError(
                            f"Invalid delete.handling.mode: {mode_str!r}. "
                            "Expected 'drop', 'rewrite', or 'none'."
                        ) from err
                    break

        if schemas_enabled is not None:
            s_enabled = schemas_enabled
        else:
            raw_schemas = None
            for key in (
                "value.converter.schemas.enable",
                "converter.schemas.enable",
                "schemas.enable",
            ):
                if key in raw:
                    raw_schemas = raw[key]
                    break
            if raw_schemas is None:
                s_enabled = True
            elif isinstance(raw_schemas, bool):
                s_enabled = raw_schemas
            elif isinstance(raw_schemas, str) and raw_schemas.strip().lower() in (
                "true",
                "false",
            ):
                s_enabled = raw_schemas.strip().lower() == "true"
            else:
                raise ValueError(
                    f"Invalid boolean value for schemas.enable: {raw_schemas!r}"
                )

        raw_prefix = (
            raw["topic.prefix"]
            if "topic.prefix" in raw
            else raw.get("database.server.name", "")
        )
        topic_prefix = str(raw_prefix).strip() if raw_prefix is not None else ""

        connector_class = str(raw.get("connector.class", "")).lower()
        connector_type = ""
        for dialect in (
            "postgresql",
            "mysql",
            "oracle",
            "sqlserver",
            "mongodb",
            "db2",
        ):
            if dialect in connector_class:
                connector_type = dialect
                break

        return cls(
            format=fmt,
            structure=struct,
            schemas_enabled=s_enabled,
            delete_handling_mode=delete_mode,
            topic_prefix=topic_prefix,
            connector_type=connector_type,
            default_variable_scale=default_variable_scale,
            connector_config=raw,
        )

    def create_converter(
        self,
        flatten: bool = True,
        *,
        strict_schemas: bool = False,
    ) -> SchemaConverter:
        """Instantiates a SchemaConverter configured for this DebeziumConfig."""
        from pydbzengine.schema.converter import SchemaConverter

        return SchemaConverter(
            config=self,
            flatten=flatten,
            strict_schemas=strict_schemas,
        )
