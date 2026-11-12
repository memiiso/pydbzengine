from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa

from pydbzengine.engine.base import (
    ChangeEvent,
    EventFormat,
)
from pydbzengine.engine.config import DebeziumConfig
from pydbzengine.schema.models import (
    ChangeBatch,
    ChangeRecord,
    EventKind,
    TableSchema,
)
from pydbzengine.schema.types import (
    DataTypeHandler,
    FieldSpec,
    IntTypeHandler,
    MapTypeHandler,
    StructTypeHandler,
    TypeRegistry,
)

__all__ = [
    "ConnectEventConverter",
    "EventConverter",
    "JsonEventConverter",
    "SchemalessJsonEventConverter",
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
        parsed = json.loads(payload_str)
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
    primary_keys: tuple[str, ...]

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


def _merge_inferred_type(prev_t: pa.DataType, curr_t: pa.DataType) -> pa.DataType:
    if prev_t == curr_t or pa.types.is_null(curr_t):
        return prev_t
    if pa.types.is_null(prev_t):
        return curr_t
    if pa.types.is_floating(prev_t) and pa.types.is_integer(curr_t):
        return prev_t
    if pa.types.is_struct(prev_t) and pa.types.is_struct(curr_t):
        return pa.struct(_merge_inferred_fields(list(prev_t), list(curr_t)))
    if pa.types.is_list(prev_t) and pa.types.is_list(curr_t):
        return pa.list_(_merge_inferred_field(prev_t.value_field, curr_t.value_field))
    if pa.types.is_map(prev_t) and pa.types.is_map(curr_t):
        return pa.map_(
            _merge_inferred_field(prev_t.key_field, curr_t.key_field).with_nullable(
                False
            ),
            _merge_inferred_field(prev_t.item_field, curr_t.item_field),
        )
    return curr_t


def _merge_inferred_field(prev_f: pa.Field, curr_f: pa.Field) -> pa.Field:
    return pa.field(
        curr_f.name,
        _merge_inferred_type(prev_f.type, curr_f.type),
        nullable=prev_f.nullable or curr_f.nullable,
    )


def _merge_inferred_fields(
    prev_fields: list[pa.Field],
    curr_fields: list[pa.Field],
) -> list[pa.Field]:
    curr_map = {f.name: f for f in curr_fields}
    prev_names = {f.name for f in prev_fields}
    merged: list[pa.Field] = []
    for pf in prev_fields:
        cf = curr_map.get(pf.name)
        if cf is None:
            merged.append(pf.with_nullable(True))
        else:
            merged.append(_merge_inferred_field(pf, cf))
    for cf in curr_fields:
        if cf.name not in prev_names:
            merged.append(cf.with_nullable(True))
    return merged


def _merge_inferred_schema(prev: pa.Schema, current: pa.Schema) -> pa.Schema:
    return pa.schema(_merge_inferred_fields(list(prev), list(current)))


class EventConverter(ABC):
    """Base contract for CDC event converters."""

    DEFAULT_HEARTBEAT_PREFIX: ClassVar[str] = DebeziumConfig.DEFAULT_HEARTBEAT_PREFIX
    format: ClassVar[EventFormat]
    schemas_enabled: ClassVar[bool]

    def __init__(
        self,
        *,
        heartbeat_prefix: str = DEFAULT_HEARTBEAT_PREFIX,
    ) -> None:
        if not isinstance(heartbeat_prefix, str) or not heartbeat_prefix.strip():
            raise ValueError(
                f"heartbeat_prefix must be a non-empty string, got {heartbeat_prefix!r}"
            )
        self.heartbeat_prefix: str = heartbeat_prefix.strip()

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
        if config.schemas_enabled:
            return JsonEventConverter(
                heartbeat_prefix=config.heartbeat_prefix,
                default_variable_scale=default_variable_scale,
            )
        return SchemalessJsonEventConverter(
            heartbeat_prefix=config.heartbeat_prefix,
        )

    def convert(self, events: Iterable[ChangeEvent]) -> ChangeBatch:
        """Converts an iterable of raw ChangeEvents into an ordered ChangeBatch."""
        return ChangeBatch(records=tuple(self.convert_event(event) for event in events))

    @abstractmethod
    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        """Converts a single raw ChangeEvent into a ChangeRecord."""

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
        destination = str(raw_dest).strip()
        if not destination:
            raise ValueError("Record contains an empty or missing destination.")
        return destination

    @staticmethod
    def _partition(event: ChangeEvent) -> int | None:
        val = event.partition()
        return IntTypeHandler.decode_connect_int(val) if val is not None else None


class _SchemaEventConverter(EventConverter):
    """Shared pipeline for schema-enabled converters (Connect and JSON)."""

    def __init__(
        self,
        *,
        heartbeat_prefix: str = EventConverter.DEFAULT_HEARTBEAT_PREFIX,
        default_variable_scale: int = 10,
    ) -> None:
        super().__init__(heartbeat_prefix=heartbeat_prefix)
        self.default_variable_scale: int = default_variable_scale
        self.registry: TypeRegistry = TypeRegistry(
            default_variable_scale=default_variable_scale
        )
        self._cache: dict[str, _CachedSchema] = {}
        self._key_cache: dict[str, _CompiledKey] = {}

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
            primary_keys: tuple[str, ...] = tuple(f.name for f in handler.fields)
        elif isinstance(handler, MapTypeHandler):
            arrow_schema = None
            primary_keys = ()
        else:
            arrow_schema = pa.schema([key_field.arrow_field])
            primary_keys = (key_field.name,)

        compiled = _CompiledKey(
            raw_schema=raw_key_schema,
            handler=handler,
            arrow_schema=arrow_schema,
            primary_keys=primary_keys,
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
            primary_keys=(
                compiled_key.primary_keys if compiled_key is not None else ()
            ),
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


# ============================================================================
# Scenario 1: Connect Event Converter (format=CONNECT, schemas_enabled=True)
# ============================================================================


class ConnectEventConverter(_SchemaEventConverter):
    """
    Dedicated converter for in-memory Java Connect SourceRecords (`EventFormat.CONNECT`).
    Schemas are always enabled and cached per destination with strong Java references.
    """

    format: ClassVar[EventFormat] = EventFormat.CONNECT
    schemas_enabled: ClassVar[bool] = True

    def _parse_field_spec(self, raw_schema: Any, field_name: str) -> FieldSpec:
        return FieldSpec.from_connect(raw_schema, field_name=field_name)

    def _convert_with_handler(self, handler: DataTypeHandler, raw_val: Any) -> Any:
        return handler.convert_connect(raw_val)

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
        return (
            source_rec.valueSchema(),
            source_rec.value(),
            source_rec.keySchema(),
            source_rec.key(),
        )


# ============================================================================
# Scenario 2: Schema-Enabled JSON Event Converter (format=JSON, schemas_enabled=True)
# ============================================================================


class JsonEventConverter(_SchemaEventConverter):
    """
    Dedicated converter for schema-enabled JSON records (`EventFormat.JSON`, `schemas_enabled=True`).
    Strictly requires `{"schema": ..., "payload": ...}` envelopes on records.
    """

    format: ClassVar[EventFormat] = EventFormat.JSON
    schemas_enabled: ClassVar[bool] = True

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
            and key_schema.get("type") == "struct"
            and key_schema.get("fields") == []
            and not key_payload
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
                "If your connector is configured with 'schemas.enable=false', "
                "use SchemalessJsonEventConverter or configure DebeziumConfig with schemas.enable=false."
            )
        return envelope["schema"], envelope["payload"]


# ============================================================================
# Scenario 3: Schemaless JSON Event Converter (format=JSON, schemas_enabled=False)
# ============================================================================


class SchemalessJsonEventConverter(EventConverter):
    """
    Dedicated converter for schemaless JSON payloads (`EventFormat.JSON`, `schemas_enabled=False`).
    Infers and unifies PyArrow schemas per destination across batches.
    """

    format: ClassVar[EventFormat] = EventFormat.JSON
    schemas_enabled: ClassVar[bool] = False

    def __init__(
        self,
        *,
        heartbeat_prefix: str = EventConverter.DEFAULT_HEARTBEAT_PREFIX,
    ) -> None:
        super().__init__(heartbeat_prefix=heartbeat_prefix)
        self._schemas: dict[str, TableSchema] = {}

    def convert(self, events: Iterable[ChangeEvent]) -> ChangeBatch:
        ordered_items: list[
            tuple[
                str,
                dict[str, Any] | None,
                dict[str, Any] | None,
                int | None,
                EventKind,
            ]
        ] = []
        rows_by_dest: dict[str, list[dict[str, Any]]] = {}
        keys_by_dest: dict[str, list[dict[str, Any]]] = {}

        for event in events:
            destination = self._destination(event)
            partition = self._partition(event)
            raw_val = event.value()
            raw_key = event.key()
            key_payload: dict[str, Any] | None = (
                _decode_json_object(
                    raw_key, f"CDC record key for destination '{destination}'"
                )
                if raw_key is not None
                else None
            )
            if not key_payload:
                key_payload = None

            payload: dict[str, Any] | None = (
                _decode_json_object(raw_val, f"event for destination '{destination}'")
                if raw_val is not None
                else None
            )
            if payload is None and key_payload is None:
                raise ValueError(
                    f"Tombstone for destination '{destination}' has no key."
                )

            kind = EventKind.classify(destination, payload, self.heartbeat_prefix)
            ordered_items.append((destination, payload, key_payload, partition, kind))
            if kind is EventKind.DATA and payload is not None:
                rows_by_dest.setdefault(destination, []).append(payload)
                if key_payload is not None:
                    keys_by_dest.setdefault(destination, []).append(key_payload)

        schemas_by_dest: dict[str, TableSchema] = {}
        for destination, dest_rows in rows_by_dest.items():
            dest_keys = keys_by_dest.get(destination, [])
            arrow_schema = pa.Table.from_pylist(dest_rows).schema
            key_pa_schema = (
                pa.Table.from_pylist(dest_keys).schema if dest_keys else None
            )
            prev = self._schemas.get(destination)
            if prev is not None:
                arrow_schema = _merge_inferred_schema(prev.arrow_schema, arrow_schema)
                if prev.key_schema is not None and key_pa_schema is not None:
                    key_pa_schema = _merge_inferred_schema(
                        prev.key_schema, key_pa_schema
                    )
                elif key_pa_schema is None:
                    key_pa_schema = prev.key_schema

            pk_names = tuple(key_pa_schema.names) if key_pa_schema is not None else ()
            table_schema = TableSchema(
                destination=destination,
                arrow_schema=arrow_schema,
                primary_keys=pk_names,
                key_schema=key_pa_schema,
            )
            self._schemas[destination] = table_schema
            schemas_by_dest[destination] = table_schema

        return ChangeBatch(
            records=tuple(
                ChangeRecord(
                    destination=destination,
                    value=row,
                    key=key_dict,
                    schema=(
                        schemas_by_dest[destination] if kind is EventKind.DATA else None
                    ),
                    partition=part,
                    kind=kind,
                )
                for destination, row, key_dict, part, kind in ordered_items
            )
        )

    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        return self.convert([event]).records[0]
