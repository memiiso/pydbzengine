from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import cached_property
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


@dataclass(frozen=True, slots=True)
class _CachedSchema:
    value_schema: Any
    key_schema: Any
    primary_keys: tuple[str, ...]
    table_schema: TableSchema

    def matches(
        self,
        value_schema: Any,
        key_schema: Any,
        primary_keys: tuple[str, ...],
    ) -> bool:
        val_eq = value_schema is self.value_schema or value_schema == self.value_schema
        key_eq = key_schema is self.key_schema or key_schema == self.key_schema
        return val_eq and key_eq and primary_keys == self.primary_keys


class EventConverter(ABC):
    """Base contract for schema-driven CDC event converters."""

    DEFAULT_HEARTBEAT_PREFIX: ClassVar[str] = DebeziumConfig.DEFAULT_HEARTBEAT_PREFIX
    format: ClassVar[EventFormat]
    schemas_enabled: ClassVar[bool]

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
        if (
            isinstance(default_variable_scale, bool)
            or not isinstance(default_variable_scale, int)
            or default_variable_scale < 0
            or default_variable_scale > 38
        ):
            raise ValueError(
                f"default_variable_scale must be between 0 and 38, got {default_variable_scale!r}"
            )
        self.heartbeat_prefix: str = heartbeat_prefix.strip()
        self.default_variable_scale: int = default_variable_scale

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
            default_variable_scale=default_variable_scale,
        )

    @cached_property
    def registry(self) -> TypeRegistry:
        return TypeRegistry(default_variable_scale=self.default_variable_scale)

    def convert(self, events: Iterable[ChangeEvent]) -> ChangeBatch:
        """Converts an iterable of raw ChangeEvents into an ordered ChangeBatch."""
        return ChangeBatch(records=tuple(self.convert_event(event) for event in events))

    @abstractmethod
    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        """Converts a single raw ChangeEvent into a ChangeRecord."""

    def _compile_key_spec(
        self,
        key_spec: FieldSpec | None,
    ) -> tuple[DataTypeHandler | None, pa.Schema | None, tuple[str, ...]]:
        if key_spec is None:
            return None, None, ()
        if key_spec.type_name == "struct":
            struct_handler = self.registry.compile_struct(key_spec)
            return (
                struct_handler,
                struct_handler.arrow_schema,
                tuple(f.name for f in struct_handler.fields),
            )
        if key_spec.type_name == "map":
            return self.registry.compile_handler(key_spec), None, ()
        scalar_field = self.registry.compile_field(key_spec)
        return (
            scalar_field.handler,
            pa.schema([scalar_field.arrow_field]),
            (scalar_field.name,),
        )

    def _compile_table_schema(
        self,
        destination: str,
        val_spec: FieldSpec,
        key_spec: FieldSpec | None,
        primary_keys: tuple[str, ...],
    ) -> TableSchema:
        value_handler = self.registry.compile_struct(val_spec)
        arrow_schema = value_handler.arrow_schema
        key_handler, key_pa_schema, key_field_names = self._compile_key_spec(key_spec)
        resolved_pks = primary_keys if primary_keys else key_field_names
        kind = EventKind.classify(
            destination,
            arrow_schema.names,
            self.heartbeat_prefix,
        )
        return TableSchema(
            destination=destination,
            kind=kind,
            arrow_schema=arrow_schema,
            primary_keys=resolved_pks,
            key_schema=key_pa_schema,
            value_handler=value_handler,
            key_handler=key_handler,
        )

    @staticmethod
    def _apply_key_handler(
        key_val: Any,
        key_handler: DataTypeHandler,
        key_schema: pa.Schema | None,
        *,
        is_connect: bool,
    ) -> dict[str, Any]:
        if isinstance(key_handler, StructTypeHandler):
            return (
                key_handler.convert_connect(key_val)
                if is_connect
                else key_handler.convert_json(key_val)
            )
        if isinstance(key_handler, MapTypeHandler):
            converted = (
                key_handler.convert_connect(key_val)
                if is_connect
                else key_handler.convert_json(key_val)
            )
            return {str(k): v for k, v in converted.items()}
        key_name = (
            key_schema[0].name
            if key_schema is not None and len(key_schema) > 0
            else "key"
        )
        if not is_connect and isinstance(key_val, Mapping):
            raw_scalar = key_val.get(key_name)
        else:
            raw_scalar = key_val
        scalar_val = (
            key_handler.convert_connect(raw_scalar)
            if is_connect
            else key_handler.convert_json(raw_scalar)
        )
        return {key_name: scalar_val}

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
        if val is None:
            return None
        if isinstance(val, (bool, float)) or hasattr(val, "booleanValue"):
            raise TypeError(
                f"Expected int for record partition, got {type(val).__name__}: {val!r}"
            )
        if isinstance(val, int):
            return val
        if hasattr(val, "intValue"):
            return int(val.intValue())
        raise TypeError(
            f"Expected int for record partition, got {type(val).__name__}: {val!r}"
        )


# ============================================================================
# Scenario 1: Connect Event Converter (format=CONNECT, schemas_enabled=True)
# ============================================================================


class ConnectEventConverter(EventConverter):
    """
    Dedicated converter for in-memory Java Connect SourceRecords (`EventFormat.CONNECT`).
    Schemas are always enabled and cached per destination with strong Java references.
    """

    format: ClassVar[EventFormat] = EventFormat.CONNECT
    schemas_enabled: ClassVar[bool] = True

    def __init__(
        self,
        *,
        heartbeat_prefix: str = EventConverter.DEFAULT_HEARTBEAT_PREFIX,
        default_variable_scale: int = 10,
    ) -> None:
        super().__init__(
            heartbeat_prefix=heartbeat_prefix,
            default_variable_scale=default_variable_scale,
        )
        self._cache: dict[str, _CachedSchema] = {}

    def compile_connect_schema(
        self,
        destination: str,
        java_value_schema: Any,
        primary_keys: tuple[str, ...] = (),
        java_key_schema: Any = None,
    ) -> TableSchema:
        """
        Compiles a Java Kafka Connect Schema into a TableSchema with bound
        StructTypeHandler trees, holding a strong reference in the destination cache.
        """
        cached = self._cache.get(destination)
        if cached is not None and cached.matches(
            java_value_schema, java_key_schema, primary_keys
        ):
            return cached.table_schema

        val_spec = FieldSpec.from_connect(java_value_schema, field_name="value")
        key_spec = (
            FieldSpec.from_connect(java_key_schema, field_name="key")
            if java_key_schema is not None
            else None
        )
        table_schema = self._compile_table_schema(
            destination=destination,
            val_spec=val_spec,
            key_spec=key_spec,
            primary_keys=primary_keys,
        )
        self._cache[destination] = _CachedSchema(
            value_schema=java_value_schema,
            key_schema=java_key_schema,
            primary_keys=primary_keys,
            table_schema=table_schema,
        )
        return table_schema

    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        destination = self._destination(event)
        partition = self._partition(event)
        source_rec = event.value()

        if source_rec is not None and not hasattr(source_rec, "valueSchema"):
            raise TypeError(
                f"Expected Java Connect SourceRecord with valueSchema() for destination '{destination}', "
                f"got {type(source_rec).__name__}"
            )

        j_val_schema = source_rec.valueSchema() if source_rec is not None else None
        raw_val = source_rec.value() if source_rec is not None else None
        raw_key = source_rec.key() if source_rec is not None else event.key()
        j_key_schema = (
            source_rec.keySchema()
            if source_rec is not None and hasattr(source_rec, "keySchema")
            else None
        )
        if j_key_schema is None and raw_key is not None and hasattr(raw_key, "schema"):
            j_key_schema = raw_key.schema()

        if raw_val is None and raw_key is None:
            raise ValueError(f"Tombstone for destination '{destination}' has no key.")
        if raw_val is not None and j_val_schema is None:
            raise ValueError(
                f"Connect SourceRecord with non-null value is missing valueSchema() for destination '{destination}'"
            )

        table_schema = (
            self.compile_connect_schema(
                destination=destination,
                java_value_schema=j_val_schema,
                primary_keys=(),
                java_key_schema=j_key_schema,
            )
            if j_val_schema is not None
            else None
        )
        converted_key = self._convert_key(
            destination, raw_key, j_key_schema, table_schema
        )
        converted_value = (
            table_schema.value_handler.convert_connect(raw_val)
            if raw_val is not None
            and table_schema is not None
            and table_schema.value_handler is not None
            else None
        )
        return ChangeRecord(
            schema=table_schema,
            value=converted_value,
            key=converted_key,
            partition=partition,
            destination=destination,
        )

    def _convert_key(
        self,
        destination: str,
        raw_key: Any,
        j_key_schema: Any,
        table_schema: TableSchema | None,
    ) -> dict[str, Any] | None:
        if raw_key is None:
            return None
        key_handler = table_schema.key_handler if table_schema is not None else None
        key_pa_schema = table_schema.key_schema if table_schema is not None else None
        if key_handler is None and j_key_schema is not None:
            cached = self._cache.get(destination)
            if (
                cached is not None
                and (
                    j_key_schema is cached.key_schema
                    or j_key_schema == cached.key_schema
                )
                and cached.table_schema.key_handler is not None
            ):
                key_handler = cached.table_schema.key_handler
                key_pa_schema = cached.table_schema.key_schema
            else:
                key_spec = FieldSpec.from_connect(j_key_schema, field_name="key")
                key_handler, key_pa_schema, _ = self._compile_key_spec(key_spec)
        if key_handler is None:
            raise TypeError(
                f"Unsupported Connect key type {type(raw_key).__name__} without compiled key_handler "
                f"for destination '{destination}'"
            )
        return self._apply_key_handler(
            raw_key, key_handler, key_pa_schema, is_connect=True
        )


# ============================================================================
# Scenario 2: Schema-Enabled JSON Event Converter (format=JSON, schemas_enabled=True)
# ============================================================================


class JsonEventConverter(EventConverter):
    """
    Dedicated converter for schema-enabled JSON records (`EventFormat.JSON`, `schemas_enabled=True`).
    Strictly requires `{"schema": ..., "payload": ...}` envelopes on records.
    """

    format: ClassVar[EventFormat] = EventFormat.JSON
    schemas_enabled: ClassVar[bool] = True

    def __init__(
        self,
        *,
        heartbeat_prefix: str = EventConverter.DEFAULT_HEARTBEAT_PREFIX,
        default_variable_scale: int = 10,
    ) -> None:
        super().__init__(
            heartbeat_prefix=heartbeat_prefix,
            default_variable_scale=default_variable_scale,
        )
        self._cache: dict[str, _CachedSchema] = {}

    def compile_json_schema(
        self,
        destination: str,
        schema_block: Mapping[str, Any],
        primary_keys: tuple[str, ...] = (),
        key_schema_block: Mapping[str, Any] | None = None,
    ) -> TableSchema:
        """
        Compiles a Debezium JSON schema dictionary into a TableSchema with bound
        StructTypeHandler trees, memoizing per destination.
        """
        cached = self._cache.get(destination)
        if cached is not None and cached.matches(
            schema_block, key_schema_block, primary_keys
        ):
            return cached.table_schema

        if not isinstance(schema_block, Mapping) or not isinstance(
            schema_block.get("fields"), list
        ):
            raise ValueError(
                f"Invalid Debezium schema block in event for destination '{destination}': "
                "missing 'fields' list."
            )

        val_spec = FieldSpec.from_json(schema_block, field_name="value")
        key_spec = (
            FieldSpec.from_json(key_schema_block, field_name="key")
            if key_schema_block is not None
            else None
        )
        table_schema = self._compile_table_schema(
            destination=destination,
            val_spec=val_spec,
            key_spec=key_spec,
            primary_keys=primary_keys,
        )
        self._cache[destination] = _CachedSchema(
            value_schema=schema_block,
            key_schema=key_schema_block,
            primary_keys=primary_keys,
            table_schema=table_schema,
        )
        return table_schema

    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        destination = self._destination(event)
        partition = self._partition(event)
        raw_val = event.value()
        pk_names, key_schema_block, key_payload = self._parse_json_key(
            event.key(), destination
        )

        schema_block: Any = None
        payload: Any = None
        if raw_val is not None:
            envelope = self.decode_json_object(
                raw_val, f"event for destination '{destination}'"
            )
            if "schema" not in envelope or "payload" not in envelope:
                raise ValueError(
                    f"Event for destination '{destination}' is missing required 'schema' and 'payload' blocks. "
                    "If your connector is configured with 'schemas.enable=false', "
                    "use SchemalessJsonEventConverter or configure DebeziumConfig with schemas.enable=false."
                )
            schema_block = envelope["schema"]
            payload = envelope["payload"]

        if payload is None and key_payload is None:
            raise ValueError(f"Tombstone for destination '{destination}' has no key.")
        if payload is not None:
            if not isinstance(schema_block, Mapping):
                raise ValueError(
                    f"Expected dict 'schema' in event for destination '{destination}', "
                    f"got {type(schema_block).__name__}"
                )
            if not isinstance(payload, Mapping):
                raise ValueError(
                    f"Expected dict 'payload' in event for destination '{destination}', "
                    f"got {type(payload).__name__}"
                )

        table_schema = (
            self.compile_json_schema(
                destination=destination,
                schema_block=schema_block,
                primary_keys=pk_names,
                key_schema_block=key_schema_block,
            )
            if schema_block is not None
            else None
        )
        converted_key = self._convert_key(
            destination, key_payload, key_schema_block, table_schema
        )
        converted_value = (
            table_schema.value_handler.convert_json(payload)
            if payload is not None
            and table_schema is not None
            and table_schema.value_handler is not None
            else None
        )
        return ChangeRecord(
            schema=table_schema,
            value=converted_value,
            key=converted_key,
            partition=partition,
            destination=destination,
        )

    def _convert_key(
        self,
        destination: str,
        key_payload: Any,
        key_schema_block: Mapping[str, Any] | None,
        table_schema: TableSchema | None,
    ) -> dict[str, Any] | None:
        if key_payload is None:
            return None
        key_handler = table_schema.key_handler if table_schema is not None else None
        key_pa_schema = table_schema.key_schema if table_schema is not None else None
        if key_handler is None and key_schema_block is not None:
            cached = self._cache.get(destination)
            if (
                cached is not None
                and key_schema_block == cached.key_schema
                and cached.table_schema.key_handler is not None
            ):
                key_handler = cached.table_schema.key_handler
                key_pa_schema = cached.table_schema.key_schema
            else:
                key_spec = FieldSpec.from_json(key_schema_block, field_name="key")
                key_handler, key_pa_schema, _ = self._compile_key_spec(key_spec)
        if key_handler is None:
            if isinstance(key_payload, Mapping):
                return dict(key_payload)
            return {"key": key_payload}
        return self._apply_key_handler(
            key_payload, key_handler, key_pa_schema, is_connect=False
        )

    @classmethod
    def _parse_json_key(
        cls,
        raw_key: Any,
        destination: str,
    ) -> tuple[tuple[str, ...], dict[str, Any] | None, Any]:
        if raw_key is None:
            return (), None, None
        key_obj = cls.decode_json_object(
            raw_key, f"CDC record key for destination '{destination}'"
        )
        if "schema" not in key_obj or "payload" not in key_obj:
            raise ValueError(
                f"Key for destination '{destination}' is missing required 'schema' and 'payload' blocks."
            )

        values = key_obj["payload"]
        if values is None:
            return (), None, None

        schema = key_obj["schema"]
        if schema is not None:
            if not isinstance(schema, dict):
                raise ValueError(
                    f"Expected dict for key 'schema', got {type(schema).__name__}"
                )
            if schema.get("type") == "struct":
                if not isinstance(values, dict):
                    raise ValueError(
                        f"Expected dict for key 'payload', got {type(values).__name__}"
                    )
                fields = schema.get("fields")
                if not isinstance(fields, list):
                    raise ValueError(
                        f"Expected list for key schema 'fields', got {type(fields).__name__}"
                    )
                if not fields:
                    if not values:
                        return (), None, None
                    raise ValueError(
                        "Key struct schema has empty 'fields' list for non-empty key payload"
                    )
                return (), schema, values
            return (), schema, values

        if not isinstance(values, dict):
            raise ValueError(
                f"Expected dict for key 'payload', got {type(values).__name__}"
            )
        if not values:
            return (), None, None
        return tuple(values.keys()), None, values

    @staticmethod
    def decode_json_object(raw: Any, context: str) -> dict[str, Any]:
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


# ============================================================================
# Scenario 3: Schemaless JSON Event Converter (format=JSON, schemas_enabled=False)
# ============================================================================


class SchemalessJsonEventConverter(EventConverter):
    """
    Dedicated converter for schemaless JSON payloads (`EventFormat.JSON`, `schemas_enabled=False`).
    Infers PyArrow schemas per destination directly via PyArrow (`pa.Table.from_pylist`).
    """

    format: ClassVar[EventFormat] = EventFormat.JSON
    schemas_enabled: ClassVar[bool] = False

    def convert(self, events: Iterable[ChangeEvent]) -> ChangeBatch:
        ordered_items: list[
            tuple[str, dict[str, Any] | None, dict[str, Any] | None, int | None]
        ] = []
        rows_by_dest: dict[str, list[dict[str, Any]]] = {}
        keys_by_dest: dict[str, list[dict[str, Any]]] = {}

        for event in events:
            destination = self._destination(event)
            partition = self._partition(event)
            raw_val = event.value()
            raw_key = event.key()
            key_payload = (
                JsonEventConverter.decode_json_object(
                    raw_key, f"CDC record key for destination '{destination}'"
                )
                if raw_key is not None
                else None
            ) or None

            if raw_val is None:
                if not key_payload:
                    raise ValueError(
                        f"Tombstone for destination '{destination}' has no key."
                    )
                ordered_items.append((destination, None, key_payload, partition))
                continue

            payload = JsonEventConverter.decode_json_object(
                raw_val, f"event for destination '{destination}'"
            )
            ordered_items.append((destination, payload, key_payload, partition))
            rows_by_dest.setdefault(destination, []).append(payload)
            if key_payload:
                keys_by_dest.setdefault(destination, []).append(key_payload)

        schemas_by_dest: dict[str, TableSchema] = {}
        for destination, dest_rows in rows_by_dest.items():
            dest_keys = keys_by_dest.get(destination, [])
            arrow_schema = pa.Table.from_pylist(dest_rows).schema
            key_pa_schema = (
                pa.Table.from_pylist(dest_keys).schema if dest_keys else None
            )
            pk_names = tuple(key_pa_schema.names) if key_pa_schema is not None else ()
            kind = EventKind.classify(
                destination,
                arrow_schema.names,
                self.heartbeat_prefix,
            )
            schemas_by_dest[destination] = TableSchema(
                destination=destination,
                kind=kind,
                arrow_schema=arrow_schema,
                primary_keys=pk_names,
                key_schema=key_pa_schema,
            )

        return ChangeBatch(
            records=tuple(
                ChangeRecord(
                    schema=schemas_by_dest.get(destination),
                    value=row,
                    key=key_dict,
                    partition=part,
                    destination=destination,
                )
                for destination, row, key_dict, part in ordered_items
            )
        )

    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        return self.convert([event]).records[0]
