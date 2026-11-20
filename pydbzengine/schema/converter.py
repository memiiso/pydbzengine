from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar

import pyarrow as pa

from pydbzengine.engine.base import ChangeEvent, EventFormat
from pydbzengine.engine.config import DebeziumConfig
from pydbzengine.schema.models import (
    ChangeBatch,
    ChangeRecord,
    EventKind,
    TableSchema,
)
from pydbzengine.schema.types import (
    build_arrow_field,
    build_map_arrow_type,
    resolve_scalar_arrow_type,
    validate_variable_scale,
)

__all__ = [
    "EventConverter",
    "RowDecoder",
    "ValueDecoder",
]

ValueDecoder = Callable[[Any], Any]
RowDecoder = Callable[[Any], dict[str, Any]]
CompiledField = tuple[str, bool, ValueDecoder]


@dataclass(frozen=True, slots=True)
class _SchemaNode:
    """Single-level metadata view of a wire schema node during one-pass compilation."""

    name: str
    type_name: str
    nullable: bool
    logical_name: str | None
    doc: str | None
    parameters: Mapping[str, str]
    raw_fields: tuple[tuple[str, Any], ...] = ()
    raw_items: Any = None
    raw_keys: Any = None
    raw_values: Any = None


@dataclass(frozen=True, slots=True)
class _CompiledKey:
    raw_schema: Any
    decoder: RowDecoder
    arrow_schema: pa.Schema | None
    key_fields: tuple[str, ...]

    def matches(self, raw_schema: Any) -> bool:
        return bool(raw_schema is self.raw_schema or raw_schema == self.raw_schema)


@dataclass(frozen=True, slots=True)
class _CachedSchema:
    value_schema: Any
    key_schema: Any
    table_schema: TableSchema
    value_decoder: RowDecoder
    key_decoder: RowDecoder | None
    logical_name: str | None = None

    def matches(self, value_schema: Any, key_schema: Any) -> bool:
        return bool(
            (value_schema is self.value_schema or value_schema == self.value_schema)
            and (key_schema is self.key_schema or key_schema == self.key_schema)
        )


def _decode_partition(val: Any) -> int | None:
    if val is None:
        return None
    if type(val) is int:
        return val
    if (
        isinstance(val, (bool, float, Decimal))
        or hasattr(val, "booleanValue")
        or hasattr(val, "toPlainString")
        or hasattr(val, "isNaN")
    ):
        raise TypeError(f"Cannot convert {type(val).__name__} {val!r} to int")
    if isinstance(val, int):
        return int(val)
    if hasattr(val, "longValue"):
        return int(val.longValue())
    if hasattr(val, "intValue"):
        return int(val.intValue())
    raise TypeError(f"Cannot convert partition {type(val).__name__} to int: {val!r}")


def decode_struct_fields(
    reader: Callable[[str], Any],
    compiled_fields: tuple[CompiledField, ...],
) -> dict[str, Any]:
    """Shared single-try struct field loop preserving nested Field '<name>' diagnostics."""
    result: dict[str, Any] = {}
    name = ""
    try:
        for name, nullable, dec in compiled_fields:
            raw = reader(name)
            if raw is None:
                if not nullable:
                    raise ValueError(
                        f"Non-nullable field '{name}' received null value."
                    )
                result[name] = None
            else:
                val = dec(raw)
                if val is None and not nullable:
                    raise ValueError(
                        f"Non-nullable field '{name}' received null value."
                    )
                result[name] = val
    except TypeError as err:
        raise TypeError(f"Field '{name}': {err}") from err
    except ValueError as err:
        if str(err).startswith(f"Non-nullable field '{name}'"):
            raise
        raise ValueError(f"Field '{name}': {err}") from err
    return result


def decode_sequence_items(
    items: Iterable[Any],
    item_name: str,
    item_nullable: bool,
    item_dec: ValueDecoder,
) -> list[Any]:
    """Shared array element loop enforcing nullability and field-path diagnostics."""
    out: list[Any] = []
    try:
        for raw in items:
            if raw is None:
                if not item_nullable:
                    raise ValueError(
                        f"Non-nullable field '{item_name}' received null value."
                    )
                out.append(None)
            else:
                val = item_dec(raw)
                if val is None and not item_nullable:
                    raise ValueError(
                        f"Non-nullable field '{item_name}' received null value."
                    )
                out.append(val)
    except TypeError as err:
        raise TypeError(f"Field '{item_name}': {err}") from err
    except ValueError as err:
        if str(err).startswith(f"Non-nullable field '{item_name}'"):
            raise
        raise ValueError(f"Field '{item_name}': {err}") from err
    return out


def decode_map_value(
    raw_val: Any,
    val_name: str,
    val_nullable: bool,
    val_dec: ValueDecoder,
) -> Any:
    if raw_val is None:
        if not val_nullable:
            raise ValueError(f"Non-nullable field '{val_name}' received null value.")
        return None
    try:
        val = val_dec(raw_val)
    except TypeError as err:
        raise TypeError(f"Field '{val_name}': {err}") from err
    except ValueError as err:
        raise ValueError(f"Field '{val_name}': {err}") from err
    if val is None and not val_nullable:
        raise ValueError(f"Non-nullable field '{val_name}' received null value.")
    return val


class EventConverter(ABC):
    """
    Abstract base pipeline shared by schema-enabled CDC converters
    (ConnectEventConverter and JsonEventConverter).

    Owns one-pass schema compilation, per-destination schema caching, key
    resolution, tombstone handling, and ChangeBatch assembly while delegating
    format-specific wire parsing and value decoding to concrete subclasses.
    """

    DEFAULT_HEARTBEAT_PREFIX: ClassVar[str] = DebeziumConfig.DEFAULT_HEARTBEAT_PREFIX
    format: ClassVar[EventFormat]
    schemas_enabled: ClassVar[bool] = True

    def __init__(
        self,
        *,
        heartbeat_prefix: str = DEFAULT_HEARTBEAT_PREFIX,
        default_variable_scale: int = 10,
    ) -> None:
        if not isinstance(heartbeat_prefix, str) or not heartbeat_prefix.strip():
            raise ValueError(
                f"heartbeat_prefix must be a non-empty string, got {heartbeat_prefix!r}"
            )
        self.heartbeat_prefix: str = heartbeat_prefix.strip()
        self.default_variable_scale: int = validate_variable_scale(
            default_variable_scale
        )
        self._cache: dict[str, _CachedSchema] = {}
        self._key_cache: dict[str, _CompiledKey] = {}

    @classmethod
    def from_config(
        cls,
        config: DebeziumConfig,
        *,
        default_variable_scale: int = 10,
    ) -> EventConverter:
        """Instantiates the concrete EventConverter matching the DebeziumConfig."""
        if not isinstance(config, DebeziumConfig):
            raise TypeError(
                f"Expected DebeziumConfig for config, got {type(config).__name__}"
            )
        if config.format is EventFormat.CONNECT:
            from pydbzengine.schema.connect import ConnectEventConverter

            return ConnectEventConverter(
                heartbeat_prefix=config.heartbeat_prefix,
                default_variable_scale=default_variable_scale,
            )
        if not config.schemas_enabled:
            raise ValueError(
                "pydbzengine.schema requires schema-enabled events "
                "(EventFormat.CONNECT or EventFormat.JSON with schemas_enabled=True). "
                "For schemaless JSON payloads, subclass BasePythonChangeHandler directly."
            )
        from pydbzengine.schema.json import JsonEventConverter

        return JsonEventConverter(
            heartbeat_prefix=config.heartbeat_prefix,
            default_variable_scale=default_variable_scale,
        )

    @abstractmethod
    def _inspect_schema(
        self, raw_schema: Any, field_name: str | None = None
    ) -> _SchemaNode:
        """Extracts single-level metadata from a format-specific wire schema."""

    @abstractmethod
    def _scalar_decoder(
        self, arrow_type: pa.DataType, logical_name: str | None
    ) -> ValueDecoder:
        """Returns a format-specific decoder for a compiled scalar/logical Arrow type."""

    @abstractmethod
    def _struct_decoder(self, fields: tuple[CompiledField, ...]) -> RowDecoder:
        """Returns a format-specific decoder for a struct with compiled child fields."""

    @abstractmethod
    def _array_decoder(self, item_field: CompiledField) -> ValueDecoder:
        """Returns a format-specific decoder for an array with compiled item field."""

    @abstractmethod
    def _map_decoder(
        self,
        key_type: pa.DataType,
        key_dec: ValueDecoder,
        val_field: CompiledField,
    ) -> ValueDecoder:
        """Returns a format-specific decoder for a map with compiled key and value decoders."""

    @abstractmethod
    def _unpack_event(
        self,
        event: ChangeEvent,
        destination: str,
    ) -> tuple[Any, Any, Any, Any]:
        """Unpacks (raw_val_schema, raw_val, raw_key_schema, raw_key) from a ChangeEvent."""

    def _compile_field(
        self, raw_schema: Any, field_name: str | None = None
    ) -> tuple[pa.Field, ValueDecoder, str | None]:
        node = self._inspect_schema(raw_schema, field_name)
        if (
            node.type_name == "struct"
            and node.logical_name != "io.debezium.data.VariableScaleDecimal"
        ):
            if not node.raw_fields:
                raise ValueError(f"Struct field '{node.name}' has no sub-fields")
            pa_fields: list[pa.Field] = []
            children: list[CompiledField] = []
            for sub_name, sub_raw in node.raw_fields:
                sub_field, sub_dec, _ = self._compile_field(sub_raw, sub_name or None)
                pa_fields.append(sub_field)
                children.append((sub_field.name, sub_field.nullable, sub_dec))
            arrow_field = build_arrow_field(
                node.name,
                pa.struct(pa_fields),
                nullable=node.nullable,
                doc=node.doc,
                logical_name=node.logical_name,
            )
            return (
                arrow_field,
                self._struct_decoder(tuple(children)),
                node.logical_name,
            )

        if node.type_name == "array":
            if node.raw_items is None:
                raise ValueError(f"Array field '{node.name}' missing 'items' schema")
            item_field, item_dec, _ = self._compile_field(node.raw_items, "item")
            arrow_field = build_arrow_field(
                node.name,
                pa.list_(item_field),
                nullable=node.nullable,
                doc=node.doc,
                logical_name=node.logical_name,
            )
            return (
                arrow_field,
                self._array_decoder((item_field.name, item_field.nullable, item_dec)),
                node.logical_name,
            )

        if node.type_name == "map":
            if node.raw_keys is None or node.raw_values is None:
                raise ValueError(
                    f"Map field '{node.name}' missing 'keys' or 'values' schema"
                )
            key_field, key_dec, _ = self._compile_field(node.raw_keys, "key")
            val_field, val_dec, _ = self._compile_field(node.raw_values, "value")
            arrow_field = build_arrow_field(
                node.name,
                build_map_arrow_type(key_field, val_field),
                nullable=node.nullable,
                doc=node.doc,
                logical_name=node.logical_name,
            )
            return (
                arrow_field,
                self._map_decoder(
                    key_field.type,
                    key_dec,
                    (val_field.name, val_field.nullable, val_dec),
                ),
                node.logical_name,
            )

        arrow_type = resolve_scalar_arrow_type(
            node.name,
            node.type_name,
            node.logical_name,
            node.parameters,
            default_variable_scale=self.default_variable_scale,
        )
        arrow_field = build_arrow_field(
            node.name,
            arrow_type,
            nullable=node.nullable,
            doc=node.doc,
            logical_name=node.logical_name,
        )
        return (
            arrow_field,
            self._scalar_decoder(arrow_type, node.logical_name),
            node.logical_name,
        )

    def _compile_struct(
        self, raw_schema: Any, field_name: str
    ) -> tuple[pa.Schema, RowDecoder, str | None]:
        arrow_field, decoder, logical_name = self._compile_field(raw_schema, field_name)
        if not pa.types.is_struct(arrow_field.type):
            raise ValueError(
                f"Expected struct schema for '{field_name}', got '{arrow_field.type}'"
            )
        return pa.schema(list(arrow_field.type)), decoder, logical_name

    def _compile_key(
        self, raw_schema: Any
    ) -> tuple[pa.Schema | None, tuple[str, ...], RowDecoder]:
        arrow_field, decoder, _ = self._compile_field(raw_schema, "key")
        if pa.types.is_struct(arrow_field.type):
            fields = list(arrow_field.type)
            return pa.schema(fields), tuple(f.name for f in fields), decoder
        if pa.types.is_map(arrow_field.type):
            return (
                None,
                (),
                lambda raw: {str(k): v for k, v in decoder(raw).items()},
            )
        return (
            pa.schema([arrow_field]),
            (arrow_field.name,),
            lambda raw: {"key": decoder(raw)},
        )

    @staticmethod
    def _destination(event: ChangeEvent) -> str:
        raw_dest = event.destination()
        if raw_dest is None:
            raise ValueError("Record contains an empty or missing destination.")
        if not (
            isinstance(raw_dest, str) or type(raw_dest).__name__ == "java.lang.String"
        ):
            raise TypeError(
                f"Expected str for record destination, got {type(raw_dest).__name__}: {raw_dest!r}"
            )
        dest = str(raw_dest).strip()
        if not dest:
            raise ValueError("Record contains an empty or missing destination.")
        return dest

    def _resolve_key(
        self, destination: str, raw_key_schema: Any
    ) -> _CompiledKey | None:
        if raw_key_schema is None:
            return None
        cached = self._key_cache.get(destination)
        if cached is not None and cached.matches(raw_key_schema):
            return cached
        arrow_schema, key_fields, decoder = self._compile_key(raw_key_schema)
        compiled = _CompiledKey(
            raw_schema=raw_key_schema,
            decoder=decoder,
            arrow_schema=arrow_schema,
            key_fields=key_fields,
        )
        self._key_cache[destination] = compiled
        return compiled

    def _compile_entry(
        self,
        destination: str,
        value_schema: Any,
        key_schema: Any = None,
        *,
        cache: bool = True,
    ) -> _CachedSchema:
        cached = self._cache.get(destination)
        if cached is not None and cached.matches(value_schema, key_schema):
            return cached

        arrow_schema, value_decoder, logical_name = self._compile_struct(
            value_schema, "value"
        )
        compiled_key = self._resolve_key(destination, key_schema)
        table_schema = TableSchema(
            destination=destination,
            arrow_schema=arrow_schema,
            key_fields=compiled_key.key_fields if compiled_key is not None else (),
            key_schema=compiled_key.arrow_schema if compiled_key is not None else None,
        )
        entry = _CachedSchema(
            value_schema=value_schema,
            key_schema=key_schema,
            table_schema=table_schema,
            value_decoder=value_decoder,
            key_decoder=compiled_key.decoder if compiled_key is not None else None,
            logical_name=logical_name,
        )
        if cache:
            self._cache[destination] = entry
        return entry

    def compile_schema(
        self,
        destination: str,
        value_schema: Any,
        key_schema: Any = None,
    ) -> TableSchema:
        """Compiles raw value and key schemas into a cached TableSchema for destination."""
        return self._compile_entry(destination, value_schema, key_schema).table_schema

    def _resolve_tombstone_schema(
        self,
        destination: str,
        raw_val_schema: Any,
        raw_key_schema: Any,
    ) -> TableSchema | None:
        if raw_val_schema is None:
            return None
        cached = self._cache.get(destination)
        if cached is not None and cached.matches(raw_val_schema, raw_key_schema):
            return cached.table_schema
        node = self._inspect_schema(raw_val_schema, "value")
        if node.type_name != "struct" or not node.raw_fields:
            return None
        return self._compile_entry(
            destination, raw_val_schema, raw_key_schema, cache=False
        ).table_schema

    def _convert_key(
        self,
        destination: str,
        raw_key: Any,
        raw_key_schema: Any,
        *,
        key_decoder: RowDecoder | None = None,
    ) -> dict[str, Any] | None:
        if raw_key is None:
            return None
        if key_decoder is None:
            compiled = self._resolve_key(destination, raw_key_schema)
            if compiled is None:
                raise ValueError(
                    f"Event with non-null key is missing key schema for destination '{destination}'"
                )
            key_decoder = compiled.decoder
        return key_decoder(raw_key)

    def convert(self, events: Iterable[ChangeEvent]) -> ChangeBatch:
        """Converts an iterable of raw ChangeEvents into an ordered ChangeBatch."""
        return ChangeBatch(records=tuple(self.convert_event(event) for event in events))

    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        """Converts a single raw ChangeEvent into a ChangeRecord."""
        destination = self._destination(event)
        partition = _decode_partition(event.partition())
        raw_val_schema, raw_val, raw_key_schema, raw_key = self._unpack_event(
            event, destination
        )
        if raw_val is None and raw_key is None:
            raise ValueError(f"Tombstone for destination '{destination}' has no key.")
        if raw_val is not None and raw_val_schema is None:
            raise ValueError(
                f"Event with non-null value is missing value schema for destination '{destination}'"
            )
        if raw_key is not None and raw_key_schema is None:
            raise ValueError(
                f"Event with non-null key is missing key schema for destination '{destination}'"
            )

        if raw_val is not None:
            entry = self._compile_entry(destination, raw_val_schema, raw_key_schema)
            converted_value = entry.value_decoder(raw_val)
            converted_key = self._convert_key(
                destination, raw_key, raw_key_schema, key_decoder=entry.key_decoder
            )
            return ChangeRecord(
                destination=destination,
                value=converted_value,
                key=converted_key,
                schema=entry.table_schema,
                partition=partition,
                kind=EventKind.classify(
                    destination,
                    converted_value,
                    self.heartbeat_prefix,
                    schema_name=entry.logical_name,
                ),
            )

        converted_key = self._convert_key(destination, raw_key, raw_key_schema)
        return ChangeRecord(
            destination=destination,
            value=None,
            key=converted_key,
            schema=self._resolve_tombstone_schema(
                destination, raw_val_schema, raw_key_schema
            ),
            partition=partition,
            kind=EventKind.TOMBSTONE,
        )
