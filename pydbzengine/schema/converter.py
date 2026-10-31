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
    INFERRED_NULL_META_KEY,
    ChangeBatch,
    ChangeRecord,
    EventKind,
    TableSchema,
)
from pydbzengine.schema.types import (
    DataTypeHandler,
    FieldSpec,
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

    @abstractmethod
    def convert(self, events: Iterable[ChangeEvent]) -> ChangeBatch:
        """Converts an iterable of raw ChangeEvents into an ordered ChangeBatch."""

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
        pa_fields = [f.arrow_field for f in value_handler.fields]
        key_handler, key_pa_schema, key_field_names = self._compile_key_spec(key_spec)
        resolved_pks = primary_keys if primary_keys else key_field_names
        valid_pks = TableSchema.select_primary_keys(resolved_pks, pa_fields)
        kind = EventKind.classify(
            destination,
            [f.name for f in value_handler.fields],
            self.heartbeat_prefix,
        )
        return TableSchema(
            destination=destination,
            kind=kind,
            arrow_schema=value_handler.arrow_schema,
            primary_keys=valid_pks,
            key_schema=key_pa_schema,
            value_handler=value_handler,
            key_handler=key_handler,
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

    @staticmethod
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

    def convert(self, events: Iterable[ChangeEvent]) -> ChangeBatch:
        return ChangeBatch(records=tuple(self.convert_event(event) for event in events))

    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        destination = self._destination(event)
        partition = self._partition(event)
        source_rec = event.value()

        if source_rec is None:
            raw_key = event.key()
            if raw_key is None:
                raise ValueError(
                    f"Tombstone for destination '{destination}' has no key."
                )
            j_key_schema = raw_key.schema() if hasattr(raw_key, "schema") else None
            key_spec = (
                FieldSpec.from_connect(j_key_schema, field_name="key")
                if j_key_schema is not None
                else None
            )
            key_handler, key_pa_schema, _ = self._compile_key_spec(key_spec)
            converted_key = TableSchema.convert_connect_key_with_handler(
                raw_key,
                key_handler=key_handler,
                key_schema=key_pa_schema,
                destination=destination,
            )
            return ChangeRecord(
                schema=None,
                value=None,
                key=converted_key,
                partition=partition,
                destination=destination,
            )

        if not hasattr(source_rec, "valueSchema"):
            raise TypeError(
                f"Expected Java Connect SourceRecord with valueSchema() for destination '{destination}', "
                f"got {type(source_rec).__name__}"
            )
        j_val_schema = source_rec.valueSchema()
        raw_val = source_rec.value()
        j_key_schema = (
            source_rec.keySchema() if hasattr(source_rec, "keySchema") else None
        )
        raw_key = source_rec.key()
        if j_key_schema is None and raw_key is not None and hasattr(raw_key, "schema"):
            j_key_schema = raw_key.schema()

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

        if raw_val is None:
            if raw_key is None:
                raise ValueError(
                    f"Tombstone for destination '{destination}' has no key."
                )
            ts_key: dict[str, Any] | None
            if table_schema is not None:
                ts_key = table_schema.convert_connect_key(raw_key)
            else:
                key_spec = (
                    FieldSpec.from_connect(j_key_schema, field_name="key")
                    if j_key_schema is not None
                    else None
                )
                key_handler, key_pa_schema, _ = self._compile_key_spec(key_spec)
                ts_key = TableSchema.convert_connect_key_with_handler(
                    raw_key,
                    key_handler=key_handler,
                    key_schema=key_pa_schema,
                    destination=destination,
                )
            return ChangeRecord(
                schema=table_schema,
                value=None,
                key=ts_key,
                partition=partition,
                destination=destination,
            )

        if table_schema is None:
            raise ValueError(
                f"Connect SourceRecord with non-null value is missing valueSchema() for destination '{destination}'"
            )
        converted_value = table_schema.convert_connect_value(raw_val)
        return ChangeRecord(
            schema=table_schema,
            value=converted_value,
            key=table_schema.convert_connect_key(raw_key),
            partition=partition,
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

    def convert(self, events: Iterable[ChangeEvent]) -> ChangeBatch:
        return ChangeBatch(records=tuple(self.convert_event(event) for event in events))

    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        destination = self._destination(event)
        partition = self._partition(event)
        raw_val = event.value()
        pk_names, key_schema_block, key_payload = self._parse_json_key(
            event.key(), destination
        )

        if raw_val is None:
            if not key_payload:
                raise ValueError(
                    f"Tombstone for destination '{destination}' has no key."
                )
            key_spec = (
                FieldSpec.from_json(key_schema_block, field_name="key")
                if key_schema_block is not None
                else None
            )
            key_handler, key_pa_schema, _ = self._compile_key_spec(key_spec)
            converted_key = TableSchema.convert_json_key_with_handler(
                key_payload,
                key_handler=key_handler,
                key_schema=key_pa_schema,
            )
            return ChangeRecord(
                schema=None,
                value=None,
                key=converted_key,
                partition=partition,
                destination=destination,
            )

        envelope = self._decode_json_object(
            raw_val, f"event for destination '{destination}'"
        )
        if "schema" not in envelope or "payload" not in envelope:
            raise ValueError(
                f"Event for destination '{destination}' is missing required 'schema' and 'payload' blocks. "
                "If your connector is configured with 'schemas.enable=false' or 'value.converter.schemas.enable=false', "
                "initialize EventConverter with schemas_enabled=False."
            )

        schema_block = envelope["schema"]
        payload = envelope["payload"]

        if payload is None:
            if not key_payload:
                raise ValueError(
                    f"Tombstone for destination '{destination}' has no key."
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
            ts_key: dict[str, Any] | None
            if table_schema is not None:
                ts_key = table_schema.convert_json_key(key_payload)
            else:
                key_spec = (
                    FieldSpec.from_json(key_schema_block, field_name="key")
                    if key_schema_block is not None
                    else None
                )
                key_handler, key_pa_schema, _ = self._compile_key_spec(key_spec)
                ts_key = TableSchema.convert_json_key_with_handler(
                    key_payload,
                    key_handler=key_handler,
                    key_schema=key_pa_schema,
                )
            return ChangeRecord(
                schema=table_schema,
                value=None,
                key=ts_key,
                partition=partition,
                destination=destination,
            )

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

        table_schema = self.compile_json_schema(
            destination=destination,
            schema_block=schema_block,
            primary_keys=pk_names,
            key_schema_block=key_schema_block,
        )
        converted_value = table_schema.convert_json_value(payload)
        return ChangeRecord(
            schema=table_schema,
            value=converted_value,
            key=table_schema.convert_json_key(key_payload),
            partition=partition,
        )

    @classmethod
    def _parse_json_key(
        cls,
        raw_key: Any,
        destination: str,
    ) -> tuple[tuple[str, ...], dict[str, Any] | None, dict[str, Any] | None]:
        if raw_key is None:
            return (), None, None
        key_obj = cls._decode_json_object(
            raw_key, f"CDC record key for destination '{destination}'"
        )
        if "schema" not in key_obj or "payload" not in key_obj:
            raise ValueError(
                f"Key for destination '{destination}' is missing required 'schema' and 'payload' blocks."
            )

        values = key_obj["payload"]
        if values is None:
            return (), None, None
        if not isinstance(values, dict):
            raise ValueError(
                f"Expected dict for key 'payload', got {type(values).__name__}"
            )

        schema = key_obj["schema"]
        if schema is not None:
            if not isinstance(schema, dict):
                raise ValueError(
                    f"Expected dict for key 'schema', got {type(schema).__name__}"
                )
            if schema.get("type") == "struct":
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
            return tuple(values.keys()), schema, values
        return tuple(values.keys()), None, values


# ============================================================================
# Scenario 3: Schemaless JSON Event Converter (format=JSON, schemas_enabled=False)
# ============================================================================


class SchemalessJsonEventConverter(EventConverter):
    """
    Dedicated converter for schemaless JSON payloads (`EventFormat.JSON`, `schemas_enabled=False`).
    Infers PyArrow schemas per destination across batches and sanitizes all-null columns.
    """

    format: ClassVar[EventFormat] = EventFormat.JSON
    schemas_enabled: ClassVar[bool] = False

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
        self._cache: dict[str, TableSchema] = {}

    def convert(self, events: Iterable[ChangeEvent]) -> ChangeBatch:
        ordered_items: list[
            tuple[str, dict[str, Any] | None, dict[str, Any] | None, int | None]
        ] = []
        rows_by_dest: dict[str, list[tuple[dict[str, Any], dict[str, Any] | None]]] = {}
        key_names_by_dest: dict[str, tuple[str, ...]] = {}

        for event in events:
            destination = self._destination(event)
            partition = self._partition(event)
            raw_val = event.value()
            pk_names, key_payload = self._parse_schemaless_key(event.key(), destination)

            if raw_val is None:
                if not key_payload:
                    raise ValueError(
                        f"Tombstone for destination '{destination}' has no key."
                    )
                ordered_items.append((destination, None, key_payload, partition))
                continue

            payload = self._decode_json_object(
                raw_val, f"event for destination '{destination}'"
            )
            row_copy = dict(payload)
            ordered_items.append((destination, row_copy, key_payload, partition))
            rows_by_dest.setdefault(destination, []).append((row_copy, key_payload))
            if destination not in key_names_by_dest:
                key_names_by_dest[destination] = pk_names

        schemas_by_dest: dict[str, TableSchema] = {}
        for destination, items in rows_by_dest.items():
            dest_rows = [item[0] for item in items]
            dest_keys = [item[1] for item in items if item[1]]
            table_schema = self.infer_table_schema(
                destination=destination,
                rows=dest_rows,
                keys=dest_keys,
                key_names=key_names_by_dest[destination],
            )
            self._cache[destination] = table_schema
            schemas_by_dest[destination] = table_schema

        records = [
            (
                ChangeRecord(
                    schema=schemas_by_dest[destination],
                    value=row,
                    key=key_dict,
                    partition=part,
                )
                if row is not None
                else ChangeRecord(
                    schema=self._cache.get(destination),
                    value=None,
                    key=key_dict,
                    partition=part,
                    destination=destination,
                )
            )
            for destination, row, key_dict, part in ordered_items
        ]
        return ChangeBatch(records=tuple(records))

    def convert_event(self, event: ChangeEvent) -> ChangeRecord:
        return self.convert([event]).records[0]

    def infer_table_schema(
        self,
        destination: str,
        rows: list[dict[str, Any]],
        keys: list[dict[str, Any]],
        key_names: tuple[str, ...],
    ) -> TableSchema:
        prev_schema = self._cache.get(destination)
        prev_pa = prev_schema.arrow_schema if prev_schema is not None else None
        prev_key_pa = prev_schema.key_schema if prev_schema is not None else None

        raw_tbl = pa.Table.from_pylist(rows)
        sanitized_pa = self.sanitize_arrow_schema(
            raw_tbl.schema, previous_schema=prev_pa
        )
        valid_pks = TableSchema.select_primary_keys(key_names, list(sanitized_pa))
        kind = EventKind.classify(
            destination,
            sanitized_pa.names,
            self.heartbeat_prefix,
        )
        key_pa_schema = (
            self.sanitize_arrow_schema(
                pa.Table.from_pylist(keys).schema,
                previous_schema=prev_key_pa,
            )
            if keys
            else None
        )
        return TableSchema(
            destination=destination,
            kind=kind,
            arrow_schema=sanitized_pa,
            primary_keys=valid_pks,
            key_schema=key_pa_schema,
        )

    @classmethod
    def sanitize_arrow_schema(
        cls,
        schema: pa.Schema,
        previous_schema: pa.Schema | None = None,
    ) -> pa.Schema:
        """
        Promotes all-null inferred fields (`pa.null()`) using envelope symmetry,
        previously cached concrete types when available, or nullable `pa.string()`
        marked with `debezium.inferred_null` metadata.
        """
        field_map: dict[str, pa.Field] = {f.name: f for f in schema}
        if "op" in field_map and "before" in field_map and "after" in field_map:
            b_type = field_map["before"].type
            a_type = field_map["after"].type
            if pa.types.is_null(b_type) and pa.types.is_struct(a_type):
                field_map["before"] = (
                    field_map["before"].with_type(a_type).with_nullable(True)
                )
            elif pa.types.is_null(a_type) and pa.types.is_struct(b_type):
                field_map["after"] = (
                    field_map["after"].with_type(b_type).with_nullable(True)
                )
            elif (
                pa.types.is_struct(b_type)
                and pa.types.is_struct(a_type)
                and b_type != a_type
            ):
                b_restored = cls._restore_inferred_null(pa.schema(list(b_type)))
                a_restored = cls._restore_inferred_null(pa.schema(list(a_type)))
                unified_row = pa.struct(
                    list(
                        pa.unify_schemas(
                            [b_restored, a_restored],
                            promote_options="permissive",
                        )
                    )
                )
                field_map["before"] = (
                    field_map["before"].with_type(unified_row).with_nullable(True)
                )
                field_map["after"] = (
                    field_map["after"].with_type(unified_row).with_nullable(True)
                )

        prev_map: dict[str, pa.Field] = (
            {f.name: f for f in previous_schema} if previous_schema is not None else {}
        )
        fields = [
            cls._sanitize_field(field_map[name], prev_map.get(name))
            for name in schema.names
        ]
        return pa.schema(fields, metadata=schema.metadata)

    @classmethod
    def _sanitize_field(
        cls,
        field_obj: pa.Field,
        prev_field: pa.Field | None = None,
    ) -> pa.Field:
        dtype = field_obj.type
        if pa.types.is_null(dtype):
            if prev_field is not None and not pa.types.is_null(prev_field.type):
                return prev_field.with_nullable(True)
            return pa.field(
                field_obj.name,
                pa.string(),
                nullable=True,
                metadata={INFERRED_NULL_META_KEY: b"true"},
            )
        if pa.types.is_struct(dtype):
            prev_dtype = (
                prev_field.type
                if prev_field is not None and pa.types.is_struct(prev_field.type)
                else None
            )
            if len(dtype) == 0 and prev_dtype is not None and len(prev_dtype) > 0:
                return field_obj.with_type(prev_dtype).with_nullable(True)
            prev_sub_map: dict[str, pa.Field] = (
                {sf.name: sf for sf in prev_dtype} if prev_dtype is not None else {}
            )
            sub_fields = [
                cls._sanitize_field(sf, prev_sub_map.get(sf.name)) for sf in dtype
            ]
            return field_obj.with_type(pa.struct(sub_fields))
        if pa.types.is_list(dtype):
            prev_val_field = (
                prev_field.type.value_field
                if prev_field is not None and pa.types.is_list(prev_field.type)
                else None
            )
            sanitized_val = cls._sanitize_field(dtype.value_field, prev_val_field)
            return field_obj.with_type(pa.list_(sanitized_val))
        if pa.types.is_map(dtype):
            prev_item_field = (
                prev_field.type.item_field
                if prev_field is not None and pa.types.is_map(prev_field.type)
                else None
            )
            sanitized_item = cls._sanitize_field(dtype.item_field, prev_item_field)
            return field_obj.with_type(
                pa.map_(dtype.key_field, sanitized_item, keys_sorted=dtype.keys_sorted)
            )
        return field_obj

    @classmethod
    def _restore_inferred_null_field(cls, field_obj: pa.Field) -> pa.Field:
        if (
            field_obj.metadata
            and field_obj.metadata.get(INFERRED_NULL_META_KEY) == b"true"
        ):
            return pa.field(field_obj.name, pa.null(), nullable=True)
        if pa.types.is_struct(field_obj.type):
            return field_obj.with_type(
                pa.struct(
                    [cls._restore_inferred_null_field(sf) for sf in field_obj.type]
                )
            )
        if pa.types.is_list(field_obj.type):
            return field_obj.with_type(
                pa.list_(cls._restore_inferred_null_field(field_obj.type.value_field))
            )
        if pa.types.is_map(field_obj.type):
            return field_obj.with_type(
                pa.map_(
                    field_obj.type.key_field,
                    cls._restore_inferred_null_field(field_obj.type.item_field),
                    keys_sorted=field_obj.type.keys_sorted,
                )
            )
        return field_obj

    @classmethod
    def _restore_inferred_null(cls, schema: pa.Schema) -> pa.Schema:
        return pa.schema(
            [cls._restore_inferred_null_field(f) for f in schema],
            metadata=schema.metadata,
        )

    @classmethod
    def _parse_schemaless_key(
        cls,
        raw_key: Any,
        destination: str,
    ) -> tuple[tuple[str, ...], dict[str, Any] | None]:
        if raw_key is None:
            return (), None
        key_obj = cls._decode_json_object(
            raw_key, f"CDC record key for destination '{destination}'"
        )
        if not key_obj:
            return (), None
        return tuple(key_obj.keys()), key_obj
