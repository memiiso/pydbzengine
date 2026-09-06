from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, ClassVar

from pydbzengine.schema.base import BaseSchemaReader
from pydbzengine.schema.models import (
    CanonicalField,
    CanonicalPrimitiveType,
    CanonicalSchema,
    CanonicalType,
)

if TYPE_CHECKING:
    pass


class DebeziumSchemaReader(BaseSchemaReader):
    """
    Strict Debezium Connect JSON Schema reader.
    Extracts canonical schema strictly from the event's embedded Connect schema.
    Fails fast if the record does not contain schema definitions.
    """

    CDC_METADATA_FIELDS = (
        CanonicalField(
            name="_consumed_at",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.TIMESTAMPTZ),
            optional=True,
            doc="Timestamp when event was consumed into target table",
        ),
        CanonicalField(
            name="_dbz_op",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
            optional=True,
            doc="CDC operation type (c, u, d, r)",
        ),
        CanonicalField(
            name="_dbz_ts_ms",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.INT64),
            optional=True,
            doc="Debezium event timestamp in milliseconds",
        ),
        CanonicalField(
            name="_dbz_event_key",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
            optional=True,
            doc="Raw Debezium event key",
        ),
        CanonicalField(
            name="_dbz_event_key_hash",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
            optional=True,
            doc="Hash of the Debezium event key",
        ),
    )

    def __init__(self, include_metadata: bool = True) -> None:
        self.include_metadata = include_metadata

    def can_read(self, record: Any) -> bool:
        if record is None:
            return False
        try:
            val = record.value()
            if not val:
                return False
            if isinstance(val, dict):
                return "schema" in val
            if isinstance(val, (str, bytes)):
                val_str = (
                    val.strip()
                    if isinstance(val, str)
                    else val.decode("utf-8", errors="ignore").strip()
                )
                if not val_str:
                    return False
                parsed = json.loads(val_str)
                return isinstance(parsed, dict) and "schema" in parsed
            return False
        except (AttributeError, TypeError, json.JSONDecodeError):
            return False

    def extract_schema(
        self,
        record: Any,
        flattening_enabled: bool = True,
        include_metadata: bool | None = None,
    ) -> CanonicalSchema:
        with_metadata = (
            self.include_metadata if include_metadata is None else include_metadata
        )
        val = record.value()
        dest = record.destination()
        if not val:
            raise ValueError(
                f"Cannot extract schema from empty event for destination '{dest}'"
            )

        if isinstance(val, dict):
            data = val
        else:
            try:
                data = json.loads(str(val))
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Invalid JSON in event for destination '{dest}': {e}"
                ) from e

        if not isinstance(data, dict) or "schema" not in data:
            raise ValueError(
                f"Event for destination '{dest}' lacks a Debezium Connect 'schema' block. "
                "Strict schema management requires 'converter.schemas.enable=true' or "
                "'value.converter.schemas.enable=true' in connector configuration."
            )

        top_schema = data["schema"]
        if not isinstance(top_schema, dict) or "fields" not in top_schema:
            raise ValueError(
                f"Invalid Debezium schema block in event for destination '{dest}': "
                "missing 'fields' array."
            )

        primary_keys = self._extract_primary_keys(record)
        destination = str(dest or "unknown")

        if flattening_enabled:
            fields = self._extract_flattened_fields(
                top_schema, include_metadata=with_metadata
            )
        else:
            fields = self._extract_envelope_fields(
                top_schema, include_metadata=with_metadata
            )

        return CanonicalSchema(
            identifier=destination,
            fields=tuple(fields),
            primary_keys=tuple(primary_keys),
        )

    def _extract_primary_keys(self, record: Any) -> list[str]:
        raw_key = record.key()
        if not raw_key:
            return []
        try:
            if isinstance(raw_key, dict):
                key_data = raw_key
            else:
                key_data = json.loads(str(raw_key))
            if isinstance(key_data, dict):
                if "schema" in key_data and isinstance(key_data["schema"], dict):
                    schema_fields = key_data["schema"].get("fields", [])
                    return [
                        f["field"]
                        for f in schema_fields
                        if isinstance(f, dict) and "field" in f
                    ]
                if "payload" in key_data and isinstance(key_data["payload"], dict):
                    return list(key_data["payload"].keys())
                return list(key_data.keys())
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            self.logger.debug("Failed to parse event key for primary keys: %s", e)
        return []

    LOGICAL_TYPE_MAP: ClassVar[dict[str, CanonicalPrimitiveType]] = {
        "io.debezium.time.Date": CanonicalPrimitiveType.DATE,
        "org.apache.kafka.connect.data.Date": CanonicalPrimitiveType.DATE,
        "io.debezium.time.IsoDate": CanonicalPrimitiveType.DATE,
        "io.debezium.time.Time": CanonicalPrimitiveType.TIME,
        "io.debezium.time.MicroTime": CanonicalPrimitiveType.TIME,
        "io.debezium.time.NanoTime": CanonicalPrimitiveType.TIME,
        "org.apache.kafka.connect.data.Time": CanonicalPrimitiveType.TIME,
        "io.debezium.time.Timestamp": CanonicalPrimitiveType.TIMESTAMP,
        "io.debezium.time.MicroTimestamp": CanonicalPrimitiveType.TIMESTAMP,
        "io.debezium.time.NanoTimestamp": CanonicalPrimitiveType.TIMESTAMP,
        "org.apache.kafka.connect.data.Timestamp": CanonicalPrimitiveType.TIMESTAMP,
        "io.debezium.time.ZonedTimestamp": CanonicalPrimitiveType.TIMESTAMPTZ,
        "io.debezium.time.IsoTimestamp": CanonicalPrimitiveType.TIMESTAMPTZ,
        "io.debezium.data.Uuid": CanonicalPrimitiveType.UUID,
    }

    PRIMITIVE_TYPE_MAP: ClassVar[dict[str, CanonicalPrimitiveType]] = {
        "boolean": CanonicalPrimitiveType.BOOLEAN,
        "int8": CanonicalPrimitiveType.INT8,
        "int16": CanonicalPrimitiveType.INT16,
        "int32": CanonicalPrimitiveType.INT32,
        "int64": CanonicalPrimitiveType.INT64,
        "float": CanonicalPrimitiveType.FLOAT,
        "float32": CanonicalPrimitiveType.FLOAT,
        "double": CanonicalPrimitiveType.DOUBLE,
        "float64": CanonicalPrimitiveType.DOUBLE,
        "string": CanonicalPrimitiveType.STRING,
        "bytes": CanonicalPrimitiveType.BINARY,
    }

    def _append_metadata_fields(self, fields: list[CanonicalField]) -> None:
        existing_names = {f.name for f in fields}
        for meta in self.CDC_METADATA_FIELDS:
            if meta.name not in existing_names:
                fields.append(meta)

    def _extract_flattened_fields(
        self, top_schema: dict[str, Any], include_metadata: bool = True
    ) -> list[CanonicalField]:
        top_fields = top_schema.get("fields", [])
        raw_fields = self._resolve_envelope_row_fields(top_fields)
        row_fields = [
            self._map_connect_field(f) for f in raw_fields if isinstance(f, dict)
        ]

        if include_metadata:
            self._append_metadata_fields(row_fields)

        return row_fields

    def _resolve_envelope_row_fields(
        self, top_fields: list[Any]
    ) -> list[dict[str, Any]]:
        field_map = {f.get("field"): f for f in top_fields if isinstance(f, dict)}
        if "after" in field_map or "before" in field_map:
            row_struct = field_map.get("after") or field_map.get("before")
            if not isinstance(row_struct, dict) or "fields" not in row_struct:
                raise ValueError(
                    "Debezium envelope missing struct fields definition in 'after'/'before'."
                )
            return row_struct.get("fields", [])
        return top_fields

    def _extract_envelope_fields(
        self, top_schema: dict[str, Any], include_metadata: bool = True
    ) -> list[CanonicalField]:
        top_fields = top_schema.get("fields", [])
        envelope_fields = [
            self._map_connect_field(f) for f in top_fields if isinstance(f, dict)
        ]

        if include_metadata:
            self._append_metadata_fields(envelope_fields)

        return envelope_fields

    def _map_connect_field(self, field_def: dict[str, Any]) -> CanonicalField:
        name = field_def.get("field")
        if not name:
            raise ValueError(f"Encountered field without name in schema: {field_def}")

        optional = bool(field_def.get("optional", True))
        doc = field_def.get("doc")
        canonical_type = self._map_connect_type(field_def)

        return CanonicalField(
            name=name,
            field_type=canonical_type,
            optional=optional,
            doc=doc,
        )

    def _map_connect_type(self, field_def: dict[str, Any]) -> CanonicalType:
        type_str = field_def.get("type", "").lower()
        type_name = field_def.get("name") or ""
        params = field_def.get("parameters") or {}

        if type_name in self.LOGICAL_TYPE_MAP:
            return CanonicalType(primitive=self.LOGICAL_TYPE_MAP[type_name])

        if type_name in (
            "org.apache.kafka.connect.data.Decimal",
            "io.debezium.data.VariableScaleDecimal",
        ):
            scale = int(params.get("scale", 10))
            precision = int(params.get("connect.decimal.precision", 38))
            return CanonicalType(
                primitive=CanonicalPrimitiveType.DECIMAL,
                precision=precision,
                scale=scale,
            )

        if type_str in self.PRIMITIVE_TYPE_MAP:
            return CanonicalType(primitive=self.PRIMITIVE_TYPE_MAP[type_str])

        return self._map_complex_connect_type(field_def, type_str)

    def _map_complex_connect_type(
        self, field_def: dict[str, Any], type_str: str
    ) -> CanonicalType:
        if type_str == "struct":
            sub_fields = [
                self._map_connect_field(sub_f)
                for sub_f in field_def.get("fields", [])
                if isinstance(sub_f, dict)
            ]
            return CanonicalType(
                primitive=CanonicalPrimitiveType.STRUCT,
                fields=tuple(sub_fields),
            )

        if type_str == "array":
            elem_schema = field_def.get("items")
            if not isinstance(elem_schema, dict):
                raise ValueError(
                    f"Array field missing valid 'items' schema: {field_def}"
                )
            return CanonicalType(
                primitive=CanonicalPrimitiveType.LIST,
                element_type=self._map_connect_type(elem_schema),
            )

        if type_str == "map":
            key_schema = field_def.get("keys")
            val_schema = field_def.get("values")
            if not isinstance(key_schema, dict) or not isinstance(val_schema, dict):
                raise ValueError(
                    f"Map field missing valid 'keys' or 'values' schema: {field_def}"
                )
            return CanonicalType(
                primitive=CanonicalPrimitiveType.MAP,
                key_type=self._map_connect_type(key_schema),
                value_type=self._map_connect_type(val_schema),
            )

        type_name = field_def.get("name") or ""
        raise ValueError(
            f"Unsupported Debezium Connect type: '{type_str}' (logical name: '{type_name}')"
        )
