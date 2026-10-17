from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

import pyarrow as pa


class EventFormat(str, Enum):
    """Wire serialization format produced by the Debezium engine."""

    JSON = "json"
    CONNECT = "connect"


__all__ = [
    "ConverterConfig",
    "DebeziumConfig",
    "EventFormat",
    "TableSchema",
    "unify_arrow_schemas",
]


def unify_arrow_schemas(schemas: Sequence[pa.Schema]) -> pa.Schema:
    """
    Unifies a sequence of Arrow schemas in C++ with permissive type promotion,
    marking columns absent from any schema epoch as nullable so unified tables
    containing null-padded rows pass Parquet/Iceberg schema validation.
    """
    if not schemas:
        return pa.schema([])
    if len(schemas) == 1:
        return schemas[0]
    common_names = set.intersection(*(set(s.names) for s in schemas))
    unified = pa.unify_schemas(list(schemas), promote_options="permissive")
    return pa.schema(
        [f if f.name in common_names else f.with_nullable(True) for f in unified],
        metadata=unified.metadata,
    )


@dataclass(frozen=True, eq=False)
class TableSchema:
    """
    Compiled analytical schema descriptor for a CDC destination.
    Combines an Apache Arrow `pa.Schema` with primary key column names and
    sparse wire-format row coercers.
    """

    destination: str
    arrow_schema: pa.Schema
    primary_keys: tuple[str, ...] = ()
    row_coercers: tuple[tuple[str, Callable[[Any], Any]], ...] = ()

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, TableSchema):
            return NotImplemented
        return (
            self.destination == other.destination
            and self.primary_keys == other.primary_keys
            and self.arrow_schema.equals(other.arrow_schema, check_metadata=True)
        )

    def __hash__(self) -> int:
        return hash((self.destination, self.arrow_schema, self.primary_keys))

    def __contains__(self, name: str) -> bool:
        return self.arrow_schema.get_field_index(name) != -1

    @property
    def names(self) -> list[str]:
        return self.arrow_schema.names

    @property
    def field_names(self) -> list[str]:
        return self.arrow_schema.names

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
            return self == other
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
        Materializes a sequence of row dicts into a `pa.Table` in C++.
        Applies sparse wire coercion when `coerce=True` and aligns directly to
        `target_schema` when provided.
        """
        if coerce and self.row_coercers:
            for row in rows:
                self.coerce_row(row)
        resolved = (
            target_schema.arrow_schema
            if isinstance(target_schema, TableSchema)
            else (target_schema if target_schema is not None else self.arrow_schema)
        )
        return pa.Table.from_pylist(list(rows), schema=resolved)

    def evolve(self, incoming: TableSchema) -> TableSchema:
        """
        Returns a new `TableSchema` unifying `self` and `incoming` via PyArrow C++
        schema unification and merging sparse row coercers.
        """
        if self == incoming:
            return self
        unified_pa = unify_arrow_schemas([self.arrow_schema, incoming.arrow_schema])
        merged_coercers = dict(self.row_coercers)
        merged_coercers.update(incoming.row_coercers)
        return TableSchema(
            destination=incoming.destination,
            arrow_schema=unified_pa,
            primary_keys=incoming.primary_keys or self.primary_keys,
            row_coercers=tuple(merged_coercers.items()),
        )


def _parse_schemas_enabled(props: Mapping[str, Any]) -> bool:
    for key in (
        "value.converter.schemas.enable",
        "converter.schemas.enable",
        "schemas.enable",
    ):
        if key in props:
            val = props[key]
            if isinstance(val, bool):
                return val
            if isinstance(val, str) and val.strip().lower() in ("true", "false"):
                return val.strip().lower() == "true"
            raise ValueError(f"Invalid boolean value for schemas.enable: {val!r}")
    return True


@dataclass(frozen=True)
class ConverterConfig:
    """
    Deterministic init-time configuration for the schema event converter.
    """

    format: EventFormat = EventFormat.JSON
    schemas_enabled: bool = True
    default_variable_scale: int = 10

    @property
    def is_json(self) -> bool:
        return self.format == EventFormat.JSON

    @property
    def is_connect(self) -> bool:
        return self.format == EventFormat.CONNECT

    @classmethod
    def from_properties(
        cls,
        config: ConverterConfig | Mapping[str, Any] | None = None,
        *,
        format: EventFormat | str | None = None,
        schemas_enabled: bool | None = None,
        default_variable_scale: int | None = None,
    ) -> ConverterConfig:
        """Centralized factory resolving converter config from properties or overrides."""
        fmt = (
            format
            if isinstance(format, EventFormat)
            else (EventFormat(format.strip().lower()) if format is not None else None)
        )
        scale = default_variable_scale if default_variable_scale is not None else 10

        if isinstance(config, ConverterConfig):
            return cls(
                format=fmt if fmt is not None else config.format,
                schemas_enabled=(
                    schemas_enabled
                    if schemas_enabled is not None
                    else config.schemas_enabled
                ),
                default_variable_scale=(
                    default_variable_scale
                    if default_variable_scale is not None
                    else config.default_variable_scale
                ),
            )

        props = config or {}
        resolved_fmt = fmt if fmt is not None else EventFormat.JSON
        if schemas_enabled is not None:
            s_enabled = schemas_enabled
        elif resolved_fmt == EventFormat.CONNECT:
            s_enabled = True
        else:
            s_enabled = _parse_schemas_enabled(props)

        return cls(
            format=resolved_fmt,
            schemas_enabled=s_enabled,
            default_variable_scale=scale,
        )

    from_dict = from_properties
    resolve = from_properties


DebeziumConfig = ConverterConfig
