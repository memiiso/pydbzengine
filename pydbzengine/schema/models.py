from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from pydbzengine.engine.base import ChangeEvent
    from pydbzengine.schema.reader import SchemaBatchReader

__all__ = [
    "DebeziumConfig",
    "EventFormat",
    "EventStructure",
    "SchemaDiff",
    "TableBatch",
    "TableSchema",
]


@dataclass(frozen=True)
class SchemaDiff:
    """
    Structured schema comparison result between an existing target schema
    and an incoming CDC batch schema.
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
        - New columns were added in the incoming batch.
        - Existing column types changed/widened.
        - A previously non-nullable target column became nullable in the incoming batch.
        """
        has_relaxed_nullability = any(
            not old_f.nullable and new_f.nullable
            for old_f, new_f in self.nullability_changes
        )
        return bool(self.added_fields or self.type_changes or has_relaxed_nullability)


@dataclass(frozen=True)
class TableSchema:
    """
    Compiled, schema-aware descriptor for a CDC destination table.
    Combines a standard PyArrow Schema with primary keys, sparse value coercers,
    and downstream schema evolution helpers.
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

    @property
    def column_names(self) -> list[str]:
        return self.arrow_schema.names

    def field(self, name: str) -> pa.Field | None:
        idx = self.arrow_schema.get_field_index(name)
        return self.arrow_schema.field(idx) if idx != -1 else None

    def same_schema(self, other: TableSchema | pa.Schema) -> bool:
        if isinstance(other, TableSchema):
            return self.fingerprint == other.fingerprint
        return self.arrow_schema.equals(other)

    def coerce_row_in_place(self, row: dict[str, Any]) -> dict[str, Any]:
        """Mutates only the fields in `row` that require wire-format decoding."""
        for col_name, coerce_fn in self.row_coercers:
            val = row.get(col_name)
            if val is not None:
                row[col_name] = coerce_fn(val)
        return row

    def diff_from(self, target_schema: pa.Schema | TableSchema) -> SchemaDiff:
        """
        Computes the exact structural difference from `target_schema` (old/target)
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
        - Existing fields in target_schema keep their position.
        - Fields removed in source become nullable in the expanded target schema.
        - Widened types (e.g. int32 -> int64, float32 -> float64) are promoted.
        - New fields in self.arrow_schema are appended as nullable.
        """
        old_pa = (
            target_schema.arrow_schema
            if isinstance(target_schema, TableSchema)
            else target_schema
        )
        new_map = {f.name: f for f in self.arrow_schema}
        expanded_fields: list[pa.Field] = []

        for old_f in old_pa:
            if old_f.name not in new_map:
                # Dropped in source: retain in target schema as nullable
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
        for new_f in self.arrow_schema:
            if new_f.name not in existing_names:
                expanded_fields.append(new_f.with_nullable(True))

        return pa.schema(expanded_fields, metadata=self.arrow_schema.metadata)

    @classmethod
    def _normalize_type(cls, t: pa.DataType) -> pa.DataType:
        """
        Normalizes Arrow large_string/large_binary/large_list variants (used by PyIceberg)
        to standard Arrow types so schema comparisons only report genuine type changes.
        """
        if pa.types.is_large_string(t):
            return pa.string()
        if pa.types.is_large_binary(t):
            return pa.binary()
        if pa.types.is_large_list(t) or pa.types.is_list(t):
            return pa.list_(cls._normalize_type(t.value_type))
        if pa.types.is_struct(t):
            return pa.struct(
                [
                    pa.field(f.name, cls._normalize_type(f.type), nullable=f.nullable)
                    for f in t
                ]
            )
        if pa.types.is_map(t):
            return pa.map_(
                cls._normalize_type(t.key_type),
                cls._normalize_type(t.item_type),
            )
        return t

    @classmethod
    def _promote_type(cls, old_t: pa.DataType, new_t: pa.DataType) -> pa.DataType:
        """Losslessly widens `old_t` and `new_t` or raises TypeError if incompatible."""
        if old_t.equals(new_t) or cls._normalize_type(old_t).equals(
            cls._normalize_type(new_t)
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
                            cls._promote_type(old_sub.type, new_sub.type)
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
            promoted_elem = cls._promote_type(old_t.value_type, new_t.value_type)
            return (
                pa.large_list(promoted_elem)
                if pa.types.is_large_list(old_t)
                else pa.list_(promoted_elem)
            )
        if pa.types.is_map(old_t) and pa.types.is_map(new_t):
            return pa.map_(
                cls._promote_type(old_t.key_type, new_t.key_type),
                cls._promote_type(old_t.item_type, new_t.item_type),
            )
        raise TypeError(f"Cannot promote incompatible Arrow types: {old_t} -> {new_t}")


@dataclass(frozen=True)
class TableBatch:
    """
    Strongly-typed batch of CDC events for a single destination and schema version.
    Carries both the converted Arrow Table and full schema management context.
    """

    destination: str
    table_schema: TableSchema
    arrow_table: pa.Table
    schema_changed: bool = False
    schema_diff: SchemaDiff | None = None
    records: tuple[ChangeEvent, ...] = ()

    @property
    def schema(self) -> pa.Schema:
        """The PyArrow Schema with exact column types, nullability, and field docs."""
        return self.table_schema.arrow_schema

    @property
    def primary_keys(self) -> tuple[str, ...]:
        return self.table_schema.primary_keys

    @property
    def num_rows(self) -> int:
        return self.arrow_table.num_rows

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

    def diff_from(self, target_schema: pa.Schema | TableSchema) -> SchemaDiff:
        """Compares this batch's schema against a downstream target table's schema."""
        return self.table_schema.diff_from(target_schema)

    def expand_target_schema(self, target_schema: pa.Schema | TableSchema) -> pa.Schema:
        """Returns an expanded target `pa.Schema` incorporating any new/widened columns from this batch."""
        return self.table_schema.expand_target_schema(target_schema)

    def align_to(self, target_schema: pa.Schema | TableSchema) -> pa.Table:
        """
        Aligns this batch's Arrow Table to match `target_schema` in C++:
        - Adds missing target columns as typed NULL arrays (e.g., dropped source columns).
        - Casts columns (including nested structs and lists) whose types were widened in `target_schema`.
        - Reorders columns to match `target_schema`.
        """
        target_pa = (
            target_schema.arrow_schema
            if isinstance(target_schema, TableSchema)
            else target_schema
        )
        if self.arrow_table.schema.equals(target_pa):
            return self.arrow_table

        columns: list[pa.Array | pa.ChunkedArray] = []
        src_names = set(self.arrow_table.schema.names)

        for target_field in target_pa:
            if target_field.name in src_names:
                col = self.arrow_table.column(target_field.name)
                columns.append(
                    self._align_column(col, target_field.type, self.num_rows)
                )
            else:
                columns.append(pa.nulls(self.num_rows, type=target_field.type))

        return pa.Table.from_arrays(columns, schema=target_pa)

    @classmethod
    def _align_column(
        cls,
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
                    aligned_sub = cls._align_column(sub_col, sub_f.type, len(arr))
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
            aligned_vals = cls._align_column(
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

    def to_arrow(self) -> pa.Table:
        return self.arrow_table

    def to_parquet(
        self,
        where: Any,
        target_schema: pa.Schema | TableSchema | None = None,
        **kwargs: Any,
    ) -> None:
        """Writes this batch (optionally aligned to `target_schema`) directly to Parquet."""
        tbl = (
            self.align_to(target_schema)
            if target_schema is not None
            else self.arrow_table
        )
        pq.write_table(tbl, where, **kwargs)

    def to_pylist(self) -> list[dict[str, Any]]:
        """Returns rows as Python dicts with native Python types (datetime, date, Decimal, bytes)."""
        return self.arrow_table.to_pylist()


class EventFormat(str, Enum):
    """Wire serialization format produced by the Debezium engine."""

    JSON = "json"
    CONNECT = "connect"


class EventStructure(str, Enum):
    """Structural envelope layout of change data capture records."""

    ENVELOPE = "envelope"
    FLATTENED = "flattened"


@dataclass(frozen=True)
class DebeziumConfig:
    """
    Immutable configuration capturing Debezium engine, converter, and transform specifications.
    """

    format: EventFormat = EventFormat.JSON
    structure: EventStructure = EventStructure.ENVELOPE
    schemas_enabled: bool = True
    delete_handling_mode: str = "drop"
    topic_prefix: str = ""
    connector_type: str = ""
    default_variable_scale: int = 10
    tz_aware: bool = True
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
        tz_aware: bool = True,
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

        delete_mode = "drop"
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
                    delete_mode = str(raw_del_mode).strip().lower()
                    if delete_mode == "rewrite-with-tombstone":
                        delete_mode = "rewrite"
                    elif delete_mode == "tombstone":
                        delete_mode = "drop"
                    if delete_mode not in ("drop", "rewrite", "none"):
                        raise ValueError(
                            f"Invalid delete.handling.mode: {delete_mode!r}. "
                            "Expected 'drop', 'rewrite', or 'none'."
                        )
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
            tz_aware=tz_aware,
            connector_config=raw,
        )

    def create_batch_reader(self, flatten: bool = True) -> SchemaBatchReader:
        """Instantiates a SchemaBatchReader configured for this DebeziumConfig."""
        from pydbzengine.schema.reader import SchemaBatchReader

        return SchemaBatchReader(
            config=self,
            flatten=flatten,
            delete_handling_mode=self.delete_handling_mode,
            default_variable_scale=self.default_variable_scale,
        )
