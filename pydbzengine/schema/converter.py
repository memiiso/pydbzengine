from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar

import pyarrow as pa

from pydbzengine.engine.base import (
    ChangeEvent,
    EventFormat,
)
from pydbzengine.engine.config import DebeziumConfig
from pydbzengine.schema.handlers import (
    DataTypeHandler,
    IntTypeHandler,
    MapTypeHandler,
    StructTypeHandler,
)
from pydbzengine.schema.models import (
    ChangeBatch,
    ChangeRecord,
    EventKind,
    TableSchema,
)
from pydbzengine.schema.registry import TypeRegistry
from pydbzengine.schema.spec import FieldSpec

__all__ = [
    "ConnectEventConverter",
    "EventConverter",
    "JsonEventConverter",
]


def _decode_json_object(raw: Any, context: str) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray, memoryview)):
        payload_str = bytes(raw).decode("utf-8").strip()
    elif isinstance(raw, str) or type(raw).__name__ == "java.lang.String":
        payload_str = str(raw).strip()
    else:
        raise TypeError(
            f"Unsupported payload type {type(raw).__name__} for {context}: {raw!r}"
        )

    try:
        parsed = json.loads(payload_str, parse_float=Decimal)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {context}: {e}") from e

    if not isinstance(parsed, dict):
        raise ValueError(
            f"Expected JSON object in {context}, got {type(parsed).__name__}"
        )
    return parsed


@dataclass(frozen=True, slots=True)
class _CompiledKey:
    raw_schema: Any
    handler: DataTypeHandler
    arrow_schema: pa.Schema | None
    key_fields: tuple[str, ...]

    def matches(self, raw_schema: Any) -> bool:
        return bool(raw_schema is self.raw_schema or raw_schema == self.raw_schema)


@dataclass(frozen=True, slots=True)
class _CachedSchema:
    value_schema: Any
    key_schema: Any
    table_schema: TableSchema
    value_handler: StructTypeHandler
    key_handler: DataTypeHandler | None
    logical_name: str | None = None

    def matches(self, value_schema: Any, key_schema: Any) -> bool:
        val_eq = value_schema is self.value_schema or value_schema == self.value_schema
        return val_eq and self.matches_key(key_schema)

    def matches_key(self, key_schema: Any) -> bool:
        return bool(key_schema is self.key_schema or key_schema == self.key_schema)


class EventConverter(ABC):
    """Base pipeline for schema-enabled CDC event converters (Connect and JSON)."""

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
        self.default_variable_scale: int = default_variable_scale
        self.registry: TypeRegistry = TypeRegistry(
            default_variable_scale=default_variable_scale
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
        return JsonEventConverter(
            heartbeat_prefix=config.heartbeat_prefix,
            default_variable_scale=default_variable_scale,
        )

    def convert(self, events: Iterable[ChangeEvent]) -> ChangeBatch:
        """Converts an iterable of raw ChangeEvents into an ordered ChangeBatch."""
        return ChangeBatch(records=tuple(self.convert_event(event) for event in events))

    @abstractmethod
    def _parse_field_spec(self, raw_schema: Any, field_name: str) -> FieldSpec:
        """Parses a format-specific wire schema into a normalized FieldSpec."""

    @abstractmethod
    def _convert_with_handler(self, handler: DataTypeHandler, raw_val: Any) -> Any:
        """Converts a format-specific wire value using the compiled DataTypeHandler."""

    @abstractmethod
    def _unpack_event(
        self,
        event: ChangeEvent,
        destination: str,
    ) -> tuple[Any, Any, Any, Any]:
        """Unpacks (raw_val_schema, raw_val, raw_key_schema, raw_key) from a ChangeEvent."""

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

    @staticmethod
    def _partition(event: ChangeEvent) -> int | None:
        val = event.partition()
        return IntTypeHandler.decode_connect_int(val) if val is not None else None

    def _resolve_key(
        self,
        destination: str,
        raw_key_schema: Any,
    ) -> _CompiledKey | None:
        if raw_key_schema is None:
            return None
        cached_key = self._key_cache.get(destination)
        if cached_key is not None and cached_key.matches(raw_key_schema):
            return cached_key

        key_spec = self._parse_field_spec(raw_key_schema, "key")
        key_field = self.registry.compile_field(key_spec)
        handler = key_field.handler
        if isinstance(handler, StructTypeHandler):
            arrow_schema: pa.Schema | None = handler.arrow_schema
            key_fields: tuple[str, ...] = tuple(f.name for f in handler.fields)
        elif isinstance(handler, MapTypeHandler):
            arrow_schema = None
            key_fields = ()
        else:
            arrow_schema = pa.schema([key_field.arrow_field])
            key_fields = (key_field.name,)

        compiled = _CompiledKey(
            raw_schema=raw_key_schema,
            handler=handler,
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

        val_spec = self._parse_field_spec(value_schema, "value")
        value_handler = self.registry.compile_struct(val_spec)
        compiled_key = self._resolve_key(destination, key_schema)
        table_schema = TableSchema(
            destination=destination,
            arrow_schema=value_handler.arrow_schema,
            key_fields=(compiled_key.key_fields if compiled_key is not None else ()),
            key_schema=(
                compiled_key.arrow_schema if compiled_key is not None else None
            ),
        )
        entry = _CachedSchema(
            value_schema=value_schema,
            key_schema=key_schema,
            table_schema=table_schema,
            value_handler=value_handler,
            key_handler=compiled_key.handler if compiled_key is not None else None,
            logical_name=val_spec.logical_name,
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
        val_spec = self._parse_field_spec(raw_val_schema, "value")
        if val_spec.type_name != "struct" or not val_spec.fields:
            return None
        return self._compile_entry(
            destination,
            raw_val_schema,
            raw_key_schema,
            cache=False,
        ).table_schema

    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        """Converts a single raw ChangeEvent into a ChangeRecord."""
        destination = self._destination(event)
        partition = self._partition(event)
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
            converted_value: dict[str, Any] = self._convert_with_handler(
                entry.value_handler, raw_val
            )
            converted_key = self._convert_key(
                destination, raw_key, raw_key_schema, key_handler=entry.key_handler
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

    def _convert_key(
        self,
        destination: str,
        raw_key: Any,
        raw_key_schema: Any,
        *,
        key_handler: DataTypeHandler | None = None,
    ) -> dict[str, Any] | None:
        if raw_key is None:
            return None
        if key_handler is None:
            compiled_key = self._resolve_key(destination, raw_key_schema)
            if compiled_key is None:
                raise ValueError(
                    f"Event with non-null key is missing key schema for destination '{destination}'"
                )
            key_handler = compiled_key.handler
        if isinstance(key_handler, StructTypeHandler):
            struct_key: dict[str, Any] = self._convert_with_handler(
                key_handler, raw_key
            )
            return struct_key
        converted = self._convert_with_handler(key_handler, raw_key)
        if isinstance(key_handler, MapTypeHandler):
            return {str(k): v for k, v in converted.items()}
        return {"key": converted}


class ConnectEventConverter(EventConverter):
    """
    Dedicated converter for in-memory Java Connect SourceRecords (`EventFormat.CONNECT`).
    Schemas are always enabled and cached per destination with strong Java references.
    """

    format: ClassVar[EventFormat] = EventFormat.CONNECT

    def _parse_field_spec(self, raw_schema: Any, field_name: str) -> FieldSpec:
        return FieldSpec.from_connect(raw_schema, field_name=field_name)

    def _convert_with_handler(self, handler: DataTypeHandler, raw_val: Any) -> Any:
        return handler.convert_connect(raw_val)

    @staticmethod
    def _is_empty_connect_struct(raw_key_schema: Any, raw_key: Any) -> bool:
        if raw_key_schema is None:
            return False
        j_type = raw_key_schema.type()
        if j_type is None or str(j_type.name()).strip().lower() != "struct":
            return False
        if raw_key_schema.fields():
            return False
        if raw_key is None:
            return True
        if hasattr(raw_key, "schema"):
            struct_schema = raw_key.schema()
            if struct_schema is None:
                return False
            st_type = struct_schema.type()
            return (
                st_type is not None
                and str(st_type.name()).strip().lower() == "struct"
                and not struct_schema.fields()
            )
        return False

    def _unpack_event(
        self,
        event: ChangeEvent,
        destination: str,
    ) -> tuple[Any, Any, Any, Any]:
        source_rec = event.value()
        if source_rec is None or not hasattr(source_rec, "valueSchema"):
            raise TypeError(
                f"Expected Java Connect SourceRecord with valueSchema() for destination '{destination}', "
                f"got {type(source_rec).__name__}"
            )
        raw_val_schema = source_rec.valueSchema()
        raw_val = source_rec.value()
        raw_key_schema = source_rec.keySchema()
        raw_key = source_rec.key()
        if self._is_empty_connect_struct(raw_key_schema, raw_key):
            raw_key_schema, raw_key = None, None
        return raw_val_schema, raw_val, raw_key_schema, raw_key


class JsonEventConverter(EventConverter):
    """
    Dedicated converter for schema-enabled JSON records (`EventFormat.JSON`, `schemas_enabled=True`).
    Strictly requires `{"schema": ..., "payload": ...}` envelopes on records.
    """

    format: ClassVar[EventFormat] = EventFormat.JSON

    def _parse_field_spec(self, raw_schema: Any, field_name: str) -> FieldSpec:
        return FieldSpec.from_json(raw_schema, field_name=field_name)

    def _convert_with_handler(self, handler: DataTypeHandler, raw_val: Any) -> Any:
        return handler.convert_json(raw_val)

    def _unpack_event(
        self,
        event: ChangeEvent,
        destination: str,
    ) -> tuple[Any, Any, Any, Any]:
        val_schema, val_payload = self._unpack_envelope(
            event.value(), f"event for destination '{destination}'"
        )
        key_schema, key_payload = self._unpack_envelope(
            event.key(), f"CDC record key for destination '{destination}'"
        )
        if (
            isinstance(key_schema, Mapping)
            and isinstance(key_schema.get("type"), str)
            and key_schema["type"].strip().lower() == "struct"
            and (key_schema.get("fields") == [] or key_schema.get("fields") is None)
            and (
                key_payload is None
                or (isinstance(key_payload, Mapping) and len(key_payload) == 0)
            )
        ):
            key_schema, key_payload = None, None
        return val_schema, val_payload, key_schema, key_payload

    @staticmethod
    def _unpack_envelope(raw: Any, context: str) -> tuple[Any, Any]:
        if raw is None:
            return None, None
        envelope = _decode_json_object(raw, context)
        if "schema" not in envelope or "payload" not in envelope:
            raise ValueError(
                f"Missing required 'schema' and 'payload' blocks in {context}. "
                "Enable schemas ('converter.schemas.enable=true') for pydbzengine.schema, "
                "or subclass BasePythonChangeHandler for schemaless JSON payloads."
            )
        return envelope["schema"], envelope["payload"]
