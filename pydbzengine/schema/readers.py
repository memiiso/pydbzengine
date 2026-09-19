from __future__ import annotations

import json
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from pydbzengine.schema.base import BaseSchemaReader
from pydbzengine.schema.models import (
    CanonicalField,
    CanonicalPrimitiveType,
    CanonicalSchema,
    CanonicalType,
)

__all__ = [
    "DebeziumSchemaReader",
    "InferredSchemaReader",
    "KeyReader",
]


class KeyReader:
    """Reads primary key column names and parsed key payloads from CDC record keys."""

    @classmethod
    def extract_primary_keys(cls, record: Any) -> list[str]:
        if (
            hasattr(record, "keySchema")
            and callable(record.keySchema)
            and record.keySchema() is not None
        ):
            ks = record.keySchema()
            if hasattr(ks, "fields") and ks.fields() is not None:
                fields = list(ks.fields())
                if fields:
                    return [str(f.name()) for f in fields]

        raw_key = (
            record.key()
            if hasattr(record, "key") and callable(record.key)
            else getattr(record, "key", None)
        )
        if not raw_key:
            return []
        if not isinstance(raw_key, (dict, str, bytes, bytearray, memoryview)):
            return ["key"]

        key_data = cls.parse_key_payload(raw_key)
        if not key_data:
            return []

        if isinstance(key_data, dict):
            if "schema" in key_data and isinstance(key_data["schema"], dict):
                schema_fields = key_data["schema"].get("fields", [])
                pks = [
                    f["field"]
                    for f in schema_fields
                    if isinstance(f, dict) and "field" in f
                ]
                if pks:
                    return pks
            if "payload" in key_data and isinstance(key_data["payload"], dict):
                return list(key_data["payload"].keys())
            return list(key_data.keys())
        return ["id"] if isinstance(key_data, (int, float)) else ["key"]

    @classmethod
    def parse_key_payload(cls, raw_key: Any) -> dict[str, Any]:
        if raw_key is None:
            return {}
        if isinstance(raw_key, dict):
            if "payload" in raw_key and isinstance(raw_key["payload"], dict):
                return raw_key["payload"]
            return raw_key
        if isinstance(raw_key, (bytes, bytearray, memoryview)):
            try:
                decoded_str = bytes(raw_key).decode("utf-8").strip()
            except UnicodeDecodeError:
                return {"key": str(raw_key)}
            if not decoded_str:
                return {}
            try:
                parsed = json.loads(decoded_str)
                if isinstance(parsed, dict):
                    if "payload" in parsed and isinstance(parsed["payload"], dict):
                        return parsed["payload"]
                    return parsed
                return {"id": parsed}
            except json.JSONDecodeError:
                return {"key": decoded_str}
        if isinstance(raw_key, str):
            s = raw_key.strip()
            if not s:
                return {}
            try:
                parsed = json.loads(s)
                if isinstance(parsed, dict):
                    if "payload" in parsed and isinstance(parsed["payload"], dict):
                        return parsed["payload"]
                    return parsed
                return {"id": parsed}
            except json.JSONDecodeError:
                return {"key": s}
        return {"id": raw_key} if isinstance(raw_key, (int, float)) else {"key": raw_key}


class DebeziumSchemaReader(BaseSchemaReader):
    """
    Strict Debezium Connect JSON Schema reader.
    Extracts canonical schema strictly from the event's embedded Connect schema.
    Fails fast if the record does not contain schema definitions.
    """

    def can_read(self, record: Any) -> bool:
        if record is None:
            return False
        if hasattr(record, "valueSchema") and callable(record.valueSchema):
            if record.valueSchema() is not None:
                return True
        val = (
            record.value()
            if hasattr(record, "value") and callable(record.value)
            else record
        )
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
            if not val_str or '"schema"' not in val_str:
                return False
            try:
                parsed = json.loads(val_str)
                return isinstance(parsed, dict) and "schema" in parsed
            except json.JSONDecodeError:
                return False
        return False

    def extract_schema(
        self,
        record: Any,
        flattening_enabled: bool = True,
    ) -> CanonicalSchema:
        if hasattr(record, "destination") and callable(record.destination):
            destination = str(record.destination())
        elif isinstance(record, dict):
            destination = str(record.get("destination") or "unknown")
        else:
            destination = str(getattr(record, "destination", "unknown"))

        primary_keys = self._extract_primary_keys(record)

        if (
            hasattr(record, "valueSchema")
            and callable(record.valueSchema)
            and record.valueSchema() is not None
        ):
            fields = self._extract_from_java_schema(
                record.valueSchema(), flattening_enabled=flattening_enabled
            )
            return CanonicalSchema(
                identifier=destination,
                fields=tuple(fields),
                primary_keys=tuple(primary_keys),
            )

        if hasattr(record, "value") and callable(record.value):
            val = record.value()
        else:
            val = record
        if not val:
            raise ValueError(
                f"Cannot extract schema from empty event for destination '{destination}'"
            )

        if isinstance(val, dict):
            data = val
        else:
            try:
                data = json.loads(str(val))
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Invalid JSON in event for destination '{destination}': {e}"
                ) from e

        if not isinstance(data, dict) or "schema" not in data:
            raise ValueError(
                f"Event for destination '{destination}' lacks a Debezium Connect 'schema' block. "
                "Strict schema management requires 'converter.schemas.enable=true' or "
                "'value.converter.schemas.enable=true' in connector configuration."
            )

        top_schema = data["schema"]
        if not isinstance(top_schema, dict) or "fields" not in top_schema:
            raise ValueError(
                f"Invalid Debezium schema block in event for destination '{destination}': "
                "missing 'fields' array."
            )

        if flattening_enabled:
            fields = self._extract_flattened_fields(top_schema)
        else:
            fields = self._extract_envelope_fields(top_schema)

        return CanonicalSchema(
            identifier=destination,
            fields=tuple(fields),
            primary_keys=tuple(primary_keys),
        )

    def _extract_from_java_schema(
        self, java_schema: Any, flattening_enabled: bool
    ) -> list[CanonicalField]:
        if not hasattr(java_schema, "fields") or java_schema.fields() is None:
            return []
        j_fields = list(java_schema.fields())
        if flattening_enabled:
            field_map = {str(f.name()): f for f in j_fields}
            if "after" in field_map or "before" in field_map:
                row_field = field_map.get("after") or field_map.get("before")
                row_schema = (
                    row_field.schema()
                    if (row_field is not None and hasattr(row_field, "schema"))
                    else None
                )
                if (
                    row_schema is not None
                    and hasattr(row_schema, "fields")
                    and row_schema.fields() is not None
                ):
                    j_fields = list(row_schema.fields())

        result: list[CanonicalField] = []
        for jf in j_fields:
            field_def = self._java_field_to_dict(jf)
            result.append(self._map_connect_field(field_def))
        return result

    def _java_schema_to_dict(self, j_schema: Any) -> dict[str, Any]:
        if j_schema is None:
            return {"type": "string"}

        j_type = j_schema.type() if hasattr(j_schema, "type") else None
        type_str = (
            (
                str(j_type.name()).lower()
                if hasattr(j_type, "name")
                else str(j_type).lower()
            )
            if j_type is not None
            else "string"
        )

        schema_def: dict[str, Any] = {
            "type": type_str,
            "optional": bool(j_schema.isOptional())
            if hasattr(j_schema, "isOptional")
            else True,
            "doc": str(j_schema.doc())
            if hasattr(j_schema, "doc") and j_schema.doc()
            else None,
            "name": str(j_schema.name())
            if hasattr(j_schema, "name") and j_schema.name()
            else None,
        }
        params = j_schema.parameters() if hasattr(j_schema, "parameters") else None
        schema_def["parameters"] = dict(params) if params else {}

        if (
            type_str == "struct"
            and hasattr(j_schema, "fields")
            and j_schema.fields() is not None
        ):
            schema_def["fields"] = [
                self._java_field_to_dict(sub_f) for sub_f in j_schema.fields()
            ]
        elif (
            type_str == "array"
            and hasattr(j_schema, "valueSchema")
            and j_schema.valueSchema() is not None
        ):
            schema_def["items"] = self._java_schema_to_dict(j_schema.valueSchema())
        elif type_str == "map":
            if hasattr(j_schema, "keySchema") and j_schema.keySchema() is not None:
                schema_def["keys"] = self._java_schema_to_dict(j_schema.keySchema())
            if hasattr(j_schema, "valueSchema") and j_schema.valueSchema() is not None:
                schema_def["values"] = self._java_schema_to_dict(j_schema.valueSchema())

        return schema_def

    def _java_field_to_dict(self, j_field: Any) -> dict[str, Any]:
        name = str(j_field.name())
        jf_schema = j_field.schema() if hasattr(j_field, "schema") else None
        field_def = self._java_schema_to_dict(jf_schema)
        field_def["field"] = name
        return field_def

    def _extract_primary_keys(self, record: Any) -> list[str]:
        return KeyReader.extract_primary_keys(record)

    LOGICAL_TYPE_MAP: ClassVar[dict[str, CanonicalPrimitiveType]] = {
        # Dates
        "io.debezium.time.Date": CanonicalPrimitiveType.DATE,
        "org.apache.kafka.connect.data.Date": CanonicalPrimitiveType.DATE,
        "io.debezium.time.IsoDate": CanonicalPrimitiveType.DATE,
        # Times
        "io.debezium.time.Time": CanonicalPrimitiveType.TIME,
        "io.debezium.time.MicroTime": CanonicalPrimitiveType.TIME,
        "io.debezium.time.NanoTime": CanonicalPrimitiveType.TIME,
        "org.apache.kafka.connect.data.Time": CanonicalPrimitiveType.TIME,
        "io.debezium.time.IsoTime": CanonicalPrimitiveType.TIME,
        "io.debezium.time.ZonedTime": CanonicalPrimitiveType.TIME,
        # Timestamps
        "io.debezium.time.Timestamp": CanonicalPrimitiveType.TIMESTAMP,
        "io.debezium.time.MicroTimestamp": CanonicalPrimitiveType.TIMESTAMP,
        "io.debezium.time.NanoTimestamp": CanonicalPrimitiveType.TIMESTAMP,
        "org.apache.kafka.connect.data.Timestamp": CanonicalPrimitiveType.TIMESTAMP,
        "io.debezium.time.IsoTimestamp": CanonicalPrimitiveType.TIMESTAMP,
        "io.debezium.time.ZonedTimestamp": CanonicalPrimitiveType.TIMESTAMPTZ,
        # Year / Duration / Interval
        "io.debezium.time.Year": CanonicalPrimitiveType.INT32,
        "io.debezium.time.MicroDuration": CanonicalPrimitiveType.INT64,
        "io.debezium.time.Interval": CanonicalPrimitiveType.STRING,
        # Debezium Data Types
        "io.debezium.data.Uuid": CanonicalPrimitiveType.UUID,
        "io.debezium.data.Bits": CanonicalPrimitiveType.BINARY,
        "io.debezium.data.Json": CanonicalPrimitiveType.STRING,
        "io.debezium.data.Xml": CanonicalPrimitiveType.STRING,
        "io.debezium.data.Enum": CanonicalPrimitiveType.STRING,
        "io.debezium.data.EnumSet": CanonicalPrimitiveType.STRING,
        # PostgreSQL / Oracle / MongoDB Connector specific types
        "io.debezium.connector.postgresql.data.Ltree": CanonicalPrimitiveType.STRING,
        "io.debezium.data.Ltree": CanonicalPrimitiveType.STRING,
        "io.debezium.connector.postgresql.data.Hstore": CanonicalPrimitiveType.MAP,
        "io.debezium.data.Hstore": CanonicalPrimitiveType.MAP,
        "io.debezium.connector.oracle.data.IntervalYearToMonth": CanonicalPrimitiveType.INT32,
        "io.debezium.connector.oracle.data.IntervalDayToSecond": CanonicalPrimitiveType.INT64,
        "io.debezium.time.IntervalYearToMonth": CanonicalPrimitiveType.INT32,
        "io.debezium.time.IntervalDayToSecond": CanonicalPrimitiveType.INT64,
        "io.debezium.connector.mongodb.data.BsonTimestamp": CanonicalPrimitiveType.TIMESTAMP,
        "io.debezium.data.BsonTimestamp": CanonicalPrimitiveType.TIMESTAMP,
        "io.debezium.connector.mongodb.data.ObjectId": CanonicalPrimitiveType.STRING,
        "io.debezium.data.ObjectId": CanonicalPrimitiveType.STRING,
    }

    PRIMITIVE_TYPE_MAP: ClassVar[dict[str, CanonicalPrimitiveType]] = {
        # Booleans
        "boolean": CanonicalPrimitiveType.BOOLEAN,
        "bool": CanonicalPrimitiveType.BOOLEAN,
        # Integers
        "int8": CanonicalPrimitiveType.INT8,
        "byte": CanonicalPrimitiveType.INT8,
        "tinyint": CanonicalPrimitiveType.INT8,
        "int16": CanonicalPrimitiveType.INT16,
        "short": CanonicalPrimitiveType.INT16,
        "smallint": CanonicalPrimitiveType.INT16,
        "int32": CanonicalPrimitiveType.INT32,
        "int": CanonicalPrimitiveType.INT32,
        "integer": CanonicalPrimitiveType.INT32,
        "int64": CanonicalPrimitiveType.INT64,
        "long": CanonicalPrimitiveType.INT64,
        "bigint": CanonicalPrimitiveType.INT64,
        # Floating point
        "float": CanonicalPrimitiveType.FLOAT,
        "float4": CanonicalPrimitiveType.FLOAT,
        "float8": CanonicalPrimitiveType.FLOAT,
        "float16": CanonicalPrimitiveType.FLOAT,
        "float32": CanonicalPrimitiveType.FLOAT,
        "real": CanonicalPrimitiveType.FLOAT,
        "double": CanonicalPrimitiveType.DOUBLE,
        "float64": CanonicalPrimitiveType.DOUBLE,
        # Strings & Text
        "string": CanonicalPrimitiveType.STRING,
        "text": CanonicalPrimitiveType.STRING,
        "varchar": CanonicalPrimitiveType.STRING,
        "char": CanonicalPrimitiveType.STRING,
        "character": CanonicalPrimitiveType.STRING,
        "nvarchar": CanonicalPrimitiveType.STRING,
        "clob": CanonicalPrimitiveType.STRING,
        "json": CanonicalPrimitiveType.STRING,
        "jsonb": CanonicalPrimitiveType.STRING,
        "xml": CanonicalPrimitiveType.STRING,
        # Binaries
        "bytes": CanonicalPrimitiveType.BINARY,
        "binary": CanonicalPrimitiveType.BINARY,
        "fixed": CanonicalPrimitiveType.BINARY,
        "blob": CanonicalPrimitiveType.BINARY,
        "varbinary": CanonicalPrimitiveType.BINARY,
        # UUID
        "uuid": CanonicalPrimitiveType.UUID,
        # Temporal primitives
        "date": CanonicalPrimitiveType.DATE,
        "time": CanonicalPrimitiveType.TIME,
        "timestamp": CanonicalPrimitiveType.TIMESTAMP,
        "timestamptz": CanonicalPrimitiveType.TIMESTAMPTZ,
        "timestamp_tz": CanonicalPrimitiveType.TIMESTAMPTZ,
        "timestamp_ltz": CanonicalPrimitiveType.TIMESTAMPTZ,
    }

    def _extract_flattened_fields(
        self, top_schema: dict[str, Any]
    ) -> list[CanonicalField]:
        top_fields = top_schema.get("fields", [])
        raw_fields = self._resolve_envelope_row_fields(top_fields)
        return [self._map_connect_field(f) for f in raw_fields if isinstance(f, dict)]

    def _resolve_envelope_row_fields(
        self, top_fields: list[Any]
    ) -> list[dict[str, Any]]:
        # Handle wrapped envelope (e.g. single struct field named 'line' containing 'after'/'before')
        if len(top_fields) == 1 and isinstance(top_fields[0], dict):
            single = top_fields[0]
            if single.get("type") == "struct" and isinstance(single.get("fields"), list):
                inner_fields = [f for f in single["fields"] if isinstance(f, dict)]
                inner_names = {f.get("field") for f in inner_fields}
                if "after" in inner_names or "before" in inner_names:
                    top_fields = inner_fields

        field_map = {f.get("field"): f for f in top_fields if isinstance(f, dict)}
        if "after" in field_map or "before" in field_map:
            row_struct = field_map.get("after") or field_map.get("before")
            if not isinstance(row_struct, dict) or "fields" not in row_struct:
                raise ValueError(
                    "Debezium envelope missing struct fields definition in 'after'/'before'."
                )
            fields = row_struct.get("fields")
            return (
                [f for f in fields if isinstance(f, dict)]
                if isinstance(fields, list)
                else []
            )
        return [f for f in top_fields if isinstance(f, dict)]

    def _extract_envelope_fields(
        self, top_schema: dict[str, Any]
    ) -> list[CanonicalField]:
        top_fields = top_schema.get("fields", [])
        return [self._map_connect_field(f) for f in top_fields if isinstance(f, dict)]

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
        field_name = field_def.get("field") or ""
        params = field_def.get("parameters") or {}

        if type_name in self.LOGICAL_TYPE_MAP:
            precision: int | None = None
            if type_name in (
                "io.debezium.time.Timestamp",
                "org.apache.kafka.connect.data.Timestamp",
                "io.debezium.time.Time",
                "org.apache.kafka.connect.data.Time",
            ):
                precision = 3
            elif type_name in (
                "io.debezium.time.MicroTimestamp",
                "io.debezium.time.MicroTime",
            ):
                precision = 6
            elif type_name in (
                "io.debezium.time.NanoTimestamp",
                "io.debezium.time.NanoTime",
            ):
                precision = 9
            elif type_name == "io.debezium.time.ZonedTimestamp":
                precision = 6

            # Handle Hstore as Map of String to String if not struct
            if type_name in ("io.debezium.connector.postgresql.data.Hstore", "io.debezium.data.Hstore"):
                return CanonicalType(
                    primitive=CanonicalPrimitiveType.MAP,
                    key_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
                    value_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
                    logical_type=type_name,
                )

            return CanonicalType(
                primitive=self.LOGICAL_TYPE_MAP[type_name],
                precision=precision,
                logical_type=type_name,
            )

        if type_name in (
            "org.apache.kafka.connect.data.Decimal",
            "io.debezium.data.VariableScaleDecimal",
        ) or type_str in ("decimal", "numeric", "number"):
            scale_val = params.get("scale")
            scale = (
                int(scale_val)
                if scale_val is not None and str(scale_val).strip() != ""
                else 0
            )
            precision_val = (
                params.get("connect.decimal.precision")
                or params.get("precision")
            )
            precision = (
                int(precision_val)
                if precision_val is not None and str(precision_val).strip() != ""
                else 38
            )
            if precision <= 0:
                precision = 38
            if scale < 0 or scale > precision:
                raise ValueError(
                    f"Decimal scale ({scale}) must be between 0 and precision ({precision}) for field '{field_name}'"
                )

            return CanonicalType(
                primitive=CanonicalPrimitiveType.DECIMAL,
                precision=precision,
                scale=scale,
                logical_type=type_name or "org.apache.kafka.connect.data.Decimal",
            )

        # Debezium metadata timestamp fields: __ts_ms, __source_ts_ms, ts_ms
        if (
            type_str in ("int64", "long")
            and not type_name
            and field_name in ("__ts_ms", "__source_ts_ms", "ts_ms")
        ):
            return CanonicalType(
                primitive=CanonicalPrimitiveType.TIMESTAMPTZ,
                precision=3,
                logical_type="io.debezium.time.ZonedTimestamp",
            )

        if type_str in self.PRIMITIVE_TYPE_MAP:
            return CanonicalType(
                primitive=self.PRIMITIVE_TYPE_MAP[type_str],
                logical_type=type_name or None,
            )

        return self._map_complex_connect_type(field_def, type_str)

    def _map_complex_connect_type(
        self, field_def: dict[str, Any], type_str: str
    ) -> CanonicalType:
        type_name = field_def.get("name") or ""

        if type_str in ("struct", "object"):
            raw_fields = field_def.get("fields", [])
            sub_fields = [
                self._map_connect_field(sub_f)
                for sub_f in raw_fields
                if isinstance(sub_f, dict)
            ]
            # Spatial / Geometry support: synthesize fields if empty
            if not sub_fields and type_name in (
                "io.debezium.data.geometry.Geometry",
                "io.debezium.data.geometry.Geography",
            ):
                sub_fields = [
                    CanonicalField(
                        name="wkb",
                        field_type=CanonicalType(primitive=CanonicalPrimitiveType.BINARY),
                    ),
                    CanonicalField(
                        name="srid",
                        field_type=CanonicalType(primitive=CanonicalPrimitiveType.INT32),
                    ),
                ]
            elif not sub_fields and type_name == "io.debezium.data.geometry.Point":
                sub_fields = [
                    CanonicalField(
                        name="x",
                        field_type=CanonicalType(primitive=CanonicalPrimitiveType.DOUBLE),
                    ),
                    CanonicalField(
                        name="y",
                        field_type=CanonicalType(primitive=CanonicalPrimitiveType.DOUBLE),
                    ),
                    CanonicalField(
                        name="srid",
                        field_type=CanonicalType(primitive=CanonicalPrimitiveType.INT32),
                    ),
                ]

            return CanonicalType(
                primitive=CanonicalPrimitiveType.STRUCT,
                fields=tuple(sub_fields) if sub_fields else None,
                logical_type=type_name or None,
            )

        if type_str in ("array", "list"):
            elem_schema = field_def.get("items") or field_def.get("item")
            if not isinstance(elem_schema, dict):
                raise ValueError(
                    f"Array field missing valid 'items' schema: {field_def}"
                )
            return CanonicalType(
                primitive=CanonicalPrimitiveType.LIST,
                element_type=self._map_connect_type(elem_schema),
                logical_type=type_name or None,
            )

        if type_str in ("map", "dict"):
            key_schema = field_def.get("keys") or field_def.get("key")
            val_schema = field_def.get("values") or field_def.get("value")
            if not isinstance(key_schema, dict) or not isinstance(val_schema, dict):
                raise ValueError(
                    f"Map field missing valid 'keys' or 'values' schema: {field_def}"
                )
            return CanonicalType(
                primitive=CanonicalPrimitiveType.MAP,
                key_type=self._map_connect_type(key_schema),
                value_type=self._map_connect_type(val_schema),
                logical_type=type_name or None,
            )

        raise ValueError(
            f"Unsupported Debezium Connect type: '{type_str}' (logical name: '{type_name}')"
        )


class InferredSchemaReader(BaseSchemaReader):
    """
    Extracts canonical schemas dynamically by inferring types from schemaless row payloads.

    Provides a clean, dedicated BaseSchemaReader strategy for schemaless CDC streams.
    """

    def can_read(self, record: Any) -> bool:
        if record is None:
            return False
        val = (
            record.value()
            if hasattr(record, "value") and callable(record.value)
            else getattr(record, "value", record)
        )
        if isinstance(val, dict):
            payload = val.get("payload", val)
            row = payload.get("after") or payload.get("before") or payload
            return isinstance(row, dict) and bool(row)
        return False

    def extract_schema(
        self,
        record: Any,
        flattening_enabled: bool = True,
    ) -> CanonicalSchema:
        dest = (
            record.destination()
            if hasattr(record, "destination") and callable(record.destination)
            else getattr(record, "destination", "unknown")
        )
        destination = str(dest or "unknown")
        val = (
            record.value()
            if hasattr(record, "value") and callable(record.value)
            else getattr(record, "value", record)
        )
        if not val:
            raise ValueError(
                f"Cannot infer schema from empty record for destination '{destination}'"
            )
        data = val if isinstance(val, dict) else {}
        payload = data.get("payload", data) if isinstance(data, dict) else {}
        row = payload.get("after") or payload.get("before") or payload
        if not isinstance(row, dict) or not row:
            raise ValueError(
                f"No dictionary row available to infer schema for destination '{destination}'"
            )
        primary_keys = KeyReader.extract_primary_keys(record)
        return self.infer_schema(destination, row, primary_keys=tuple(primary_keys))

    @classmethod
    def infer_schema(
        cls,
        identifier: str,
        row: dict[str, Any],
        primary_keys: tuple[str, ...] = (),
    ) -> CanonicalSchema:
        fields = tuple(
            CanonicalField(
                name=k,
                field_type=cls.infer_type(v),
                optional=True,
            )
            for k, v in row.items()
        )
        return CanonicalSchema(
            identifier=identifier,
            fields=fields,
            primary_keys=primary_keys,
        )

    @classmethod
    def infer_type(cls, val: Any) -> CanonicalType:
        if isinstance(val, bool):
            return CanonicalType(primitive=CanonicalPrimitiveType.BOOLEAN)
        if isinstance(val, timedelta):
            return CanonicalType(
                primitive=CanonicalPrimitiveType.INT64,
                logical_type="io.debezium.time.MicroDuration",
            )
        if isinstance(val, int):
            return CanonicalType(primitive=CanonicalPrimitiveType.INT64)
        if isinstance(val, float):
            return CanonicalType(primitive=CanonicalPrimitiveType.DOUBLE)
        if isinstance(val, Decimal):
            exp = val.as_tuple().exponent
            scale = min(38, max(0, abs(exp) if isinstance(exp, int) else 2))
            return CanonicalType(
                primitive=CanonicalPrimitiveType.DECIMAL, precision=38, scale=scale
            )
        if isinstance(val, datetime):
            return (
                CanonicalType(primitive=CanonicalPrimitiveType.TIMESTAMPTZ)
                if val.tzinfo is not None
                else CanonicalType(primitive=CanonicalPrimitiveType.TIMESTAMP)
            )
        if isinstance(val, date):
            return CanonicalType(primitive=CanonicalPrimitiveType.DATE)
        if isinstance(val, time):
            return CanonicalType(primitive=CanonicalPrimitiveType.TIME)
        if isinstance(val, uuid.UUID):
            return CanonicalType(primitive=CanonicalPrimitiveType.UUID)
        if isinstance(val, (bytes, bytearray, memoryview)):
            return CanonicalType(primitive=CanonicalPrimitiveType.BINARY)
        if isinstance(val, dict) and val:
            sub_fields = tuple(
                CanonicalField(name=sk, field_type=cls.infer_type(sv))
                for sk, sv in val.items()
            )
            return CanonicalType(primitive=CanonicalPrimitiveType.STRUCT, fields=sub_fields)
        if isinstance(val, (list, tuple)) and val:
            elem_type = cls.infer_type(val[0])
            return CanonicalType(
                primitive=CanonicalPrimitiveType.LIST, element_type=elem_type
            )
        return CanonicalType(primitive=CanonicalPrimitiveType.STRING)

