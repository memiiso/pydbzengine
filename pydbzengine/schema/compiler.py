from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, ClassVar

import pyarrow as pa

from pydbzengine.schema.coercers import (
    coerce_binary,
    coerce_date_iso,
    coerce_time_iso,
    coerce_timestamp_iso_naive,
    coerce_timestamp_iso_utc,
    make_decimal_coercer,
    make_list_coercer,
    make_map_coercer,
    make_struct_coercer,
    make_variable_scale_decimal_coercer,
)
from pydbzengine.schema.models import TableSchema

__all__ = [
    "ConnectSchemaCompiler",
]

# Parameterless Debezium & Kafka Connect logical types by Type Family
_FIXED_LOGICAL_TYPES: dict[str, tuple[pa.DataType, Callable[[Any], Any] | None]] = {
    # Dates (epoch days)
    "io.debezium.time.Date": (pa.date32(), None),
    "org.apache.kafka.connect.data.Date": (pa.date32(), coerce_date_iso),
    "io.debezium.time.IsoDate": (pa.date32(), coerce_date_iso),
    # Times (strict resolution alignment)
    "io.debezium.time.Time": (pa.time32("ms"), None),
    "org.apache.kafka.connect.data.Time": (pa.time32("ms"), None),
    "io.debezium.time.MicroTime": (pa.time64("us"), None),
    "io.debezium.time.IsoTime": (pa.time64("us"), coerce_time_iso),
    "io.debezium.time.ZonedTime": (pa.time64("us"), coerce_time_iso),
    "io.debezium.time.NanoTime": (pa.time64("ns"), None),
    # Timestamps (strict resolution alignment)
    "io.debezium.time.Timestamp": (pa.timestamp("ms"), None),
    "org.apache.kafka.connect.data.Timestamp": (pa.timestamp("ms"), None),
    "io.debezium.connector.mongodb.data.BsonTimestamp": (pa.timestamp("ms"), None),
    "io.debezium.data.BsonTimestamp": (pa.timestamp("ms"), None),
    "io.debezium.time.MicroTimestamp": (pa.timestamp("us"), None),
    "io.debezium.time.IsoTimestamp": (pa.timestamp("us"), coerce_timestamp_iso_naive),
    "io.debezium.time.NanoTimestamp": (pa.timestamp("ns"), None),
    "io.debezium.time.ZonedTimestamp": (
        pa.timestamp("us", tz="UTC"),
        coerce_timestamp_iso_utc,
    ),
    # Durations, Intervals, Year
    "io.debezium.time.Year": (pa.int32(), None),
    "io.debezium.connector.oracle.data.IntervalYearToMonth": (pa.int32(), None),
    "io.debezium.time.IntervalYearToMonth": (pa.int32(), None),
    "io.debezium.time.MicroDuration": (pa.int64(), None),
    "io.debezium.time.NanoDuration": (pa.int64(), None),
    "io.debezium.connector.oracle.data.IntervalDayToSecond": (pa.int64(), None),
    "io.debezium.time.IntervalDayToSecond": (pa.int64(), None),
    "io.debezium.time.Interval": (pa.string(), None),
    # Text, Enums, UUID, JSON, XML, Ltree, ObjectId
    "io.debezium.data.Uuid": (pa.string(), None),
    "io.debezium.data.Json": (pa.string(), None),
    "io.debezium.data.Xml": (pa.string(), None),
    "io.debezium.data.Enum": (pa.string(), None),
    "io.debezium.data.EnumSet": (pa.string(), None),
    "io.debezium.connector.postgresql.data.Ltree": (pa.string(), None),
    "io.debezium.data.Ltree": (pa.string(), None),
    "io.debezium.connector.mongodb.data.ObjectId": (pa.string(), None),
    "io.debezium.data.ObjectId": (pa.string(), None),
    # Binaries & Bit sets
    "io.debezium.data.Bits": (pa.binary(), coerce_binary),
}


class ConnectSchemaCompiler:
    """
    Compiles a Debezium / Kafka Connect schema dictionary into a `TableSchema`
    matching its exact wire schema structure without envelope mutation.
    """

    PRIMITIVE_ARROW_MAP: ClassVar[dict[str, pa.DataType]] = {
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "float": pa.float32(),
        "float32": pa.float32(),
        "double": pa.float64(),
        "float64": pa.float64(),
        "boolean": pa.bool_(),
        "string": pa.string(),
        "bytes": pa.binary(),
    }

    def __init__(self, *, default_variable_scale: int = 10) -> None:
        self.default_variable_scale = default_variable_scale
        self._cache: dict[str, tuple[dict[str, Any], tuple[str, ...], TableSchema]] = {}

    def compile(
        self,
        destination: str,
        top_schema: dict[str, Any],
        primary_keys: tuple[str, ...] = (),
    ) -> TableSchema:
        """
        Compiles a Debezium Connect schema dictionary into a `TableSchema`,
        memoizing the last compiled schema per destination.
        """
        cached = self._cache.get(destination)
        if cached is not None:
            last_top_schema, last_pks, last_table_schema = cached
            if (
                top_schema is last_top_schema or top_schema == last_top_schema
            ) and primary_keys == last_pks:
                return last_table_schema

        if not isinstance(top_schema, dict) or not isinstance(
            top_schema.get("fields"), list
        ):
            raise ValueError(
                f"Invalid Debezium schema block in event for destination '{destination}': "
                "missing 'fields' list."
            )

        pa_fields: list[pa.Field] = []
        coercers: list[tuple[str, Callable[[Any], Any]]] = []

        for f_def in top_schema["fields"]:
            if not isinstance(f_def, dict):
                raise ValueError(
                    f"Expected dict in schema 'fields', got {type(f_def).__name__}"
                )
            pa_f, coerce_fn = self._compile_connect_field(f_def)
            pa_fields.append(pa_f)
            if coerce_fn is not None:
                coercers.append((pa_f.name, coerce_fn))

        candidate_fields: dict[str, pa.DataType] = {f.name: f.type for f in pa_fields}
        for f in pa_fields:
            if f.name in ("after", "before") and pa.types.is_struct(f.type):
                for sub_f in f.type:
                    candidate_fields.setdefault(sub_f.name, sub_f.type)

        valid_pks = tuple(pk for pk in primary_keys if pk in candidate_fields)
        for pk in valid_pks:
            pk_type = candidate_fields[pk]
            if (
                pa.types.is_list(pk_type)
                or pa.types.is_large_list(pk_type)
                or pa.types.is_map(pk_type)
            ):
                raise ValueError(
                    f"Complex field '{pk}' of type {pk_type} cannot be part of the primary key."
                )

        table_schema = TableSchema(
            destination=destination,
            arrow_schema=pa.schema(pa_fields),
            primary_keys=valid_pks,
            row_coercers=tuple(coercers),
        )
        self._cache[destination] = (top_schema, primary_keys, table_schema)
        return table_schema

    def _compile_connect_field(
        self, field_def: dict[str, Any]
    ) -> tuple[pa.Field, Callable[[Any], Any] | None]:
        name = field_def.get("field")
        if not name or not isinstance(name, str):
            raise ValueError(
                f"Encountered field without valid name in schema: {field_def}"
            )

        optional = bool(field_def.get("optional", True))
        doc = field_def.get("doc")
        logical_name = field_def.get("name")

        pa_type, coerce_fn = self._compile_connect_type(field_def)

        metadata: dict[bytes, bytes] = {}
        if doc:
            metadata[b"doc"] = str(doc).encode("utf-8")
        if logical_name:
            metadata[b"debezium.logical_type"] = str(logical_name).encode("utf-8")

        pa_f = pa.field(
            name,
            pa_type,
            nullable=optional,
            metadata=metadata if metadata else None,
        )
        return pa_f, coerce_fn

    def _compile_connect_type(
        self, field_def: dict[str, Any]
    ) -> tuple[pa.DataType, Callable[[Any], Any] | None]:
        type_str = str(field_def.get("type", "")).lower()
        raw_name = field_def.get("name")
        type_name = str(raw_name) if raw_name is not None else ""
        raw_field = field_def.get("field")
        field_name = str(raw_field) if raw_field is not None else ""
        raw_params = field_def.get("parameters")
        if raw_params is not None and not isinstance(raw_params, Mapping):
            raise ValueError(
                f"Expected Mapping for schema 'parameters' in field '{field_name}', "
                f"got {type(raw_params).__name__}"
            )
        params: Mapping[str, Any] = raw_params if raw_params is not None else {}

        # 1. Fixed Debezium & Connect logical types
        if type_name in _FIXED_LOGICAL_TYPES:
            return _FIXED_LOGICAL_TYPES[type_name]

        # 2. Logical Decimals
        if type_name == "io.debezium.data.VariableScaleDecimal":
            scale_val = params.get("scale")
            scale = (
                int(scale_val)
                if scale_val is not None and str(scale_val).strip() != ""
                else self.default_variable_scale
            )
            precision = self._resolve_decimal_precision(params, scale, field_name)
            return (
                pa.decimal128(precision, scale),
                make_variable_scale_decimal_coercer(default_scale=scale),
            )
        if type_name == "org.apache.kafka.connect.data.Decimal":
            scale_val = params.get("scale")
            scale = (
                int(scale_val)
                if scale_val is not None and str(scale_val).strip() != ""
                else self.default_variable_scale
            )
            precision = self._resolve_decimal_precision(params, scale, field_name)
            return pa.decimal128(precision, scale), make_decimal_coercer(scale)

        # 3. Hstore
        if type_name in (
            "io.debezium.connector.postgresql.data.Hstore",
            "io.debezium.data.Hstore",
        ):
            if type_str == "string":
                return pa.string(), None
            return pa.map_(pa.string(), pa.string()), None

        # 4. Standard Primitives (the 9 Kafka Connect base types)
        if type_str in self.PRIMITIVE_ARROW_MAP:
            pa_type = self.PRIMITIVE_ARROW_MAP[type_str]
            if pa.types.is_binary(pa_type):
                return pa_type, coerce_binary
            return pa_type, None

        # 5. Complex / Nested Types (struct, array, map)
        if type_str == "struct":
            raw_fields = field_def.get("fields", [])
            if not isinstance(raw_fields, list):
                raise ValueError(
                    f"Expected 'fields' list in struct schema, got {type(raw_fields).__name__}: {field_def}"
                )
            sub_pa_fields: list[pa.Field] = []
            sub_coercers: list[tuple[str, Callable[[Any], Any]]] = []

            for sub_f in raw_fields:
                if not isinstance(sub_f, dict):
                    raise ValueError(
                        f"Expected dict in struct 'fields', got {type(sub_f).__name__}"
                    )
                pa_f, fn = self._compile_connect_field(sub_f)
                sub_pa_fields.append(pa_f)
                if fn is not None:
                    sub_coercers.append((pa_f.name, fn))

            if not sub_pa_fields:
                if type_name in (
                    "io.debezium.data.geometry.Geometry",
                    "io.debezium.data.geometry.Geography",
                ):
                    sub_pa_fields = [
                        pa.field("wkb", pa.binary()),
                        pa.field("srid", pa.int32()),
                    ]
                    sub_coercers = [("wkb", coerce_binary)]
                elif type_name == "io.debezium.data.geometry.Point":
                    sub_pa_fields = [
                        pa.field("x", pa.float64()),
                        pa.field("y", pa.float64()),
                        pa.field("srid", pa.int32()),
                    ]
                else:
                    raise ValueError(
                        f"Struct field '{field_name}' has no sub-fields: {field_def}"
                    )

            struct_coercer = (
                make_struct_coercer(tuple(sub_coercers)) if sub_coercers else None
            )
            return pa.struct(sub_pa_fields), struct_coercer

        if type_str == "array":
            elem_schema = (
                field_def["items"] if "items" in field_def else field_def.get("item")
            )
            if not isinstance(elem_schema, dict):
                raise ValueError(
                    f"Array field missing valid 'items' schema: {field_def}"
                )
            elem_type, elem_coercer = self._compile_connect_type(elem_schema)
            list_coercer = (
                make_list_coercer(elem_coercer) if elem_coercer is not None else None
            )
            return pa.list_(elem_type), list_coercer

        if type_str == "map":
            key_schema = (
                field_def["keys"] if "keys" in field_def else field_def.get("key")
            )
            val_schema = (
                field_def["values"] if "values" in field_def else field_def.get("value")
            )
            if not isinstance(key_schema, dict) or not isinstance(val_schema, dict):
                raise ValueError(
                    f"Map field missing valid 'keys' or 'values' schema: {field_def}"
                )
            key_type, key_coercer = self._compile_connect_type(key_schema)
            if pa.types.is_nested(key_type):
                raise ValueError(
                    f"Invalid key type for map field '{field_name}': {key_type}. "
                    "Map keys cannot be struct, array, or map."
                )
            val_type, val_coercer = self._compile_connect_type(val_schema)
            map_coercer = (
                make_map_coercer(key_coercer, val_coercer)
                if (key_coercer is not None or val_coercer is not None)
                else None
            )
            return pa.map_(key_type, val_type), map_coercer

        raise ValueError(
            f"Unsupported Debezium Connect type: '{type_str}' (logical name: '{type_name}')"
        )

    @staticmethod
    def _resolve_decimal_precision(
        params: Mapping[str, Any], scale: int, field_name: str
    ) -> int:
        precision_val = (
            params["connect.decimal.precision"]
            if "connect.decimal.precision" in params
            else params.get("precision")
        )
        precision = (
            int(precision_val)
            if precision_val is not None and str(precision_val).strip() != ""
            else 38
        )
        if precision < 1 or precision > 38:
            raise ValueError(
                f"Decimal precision must be between 1 and 38, got {precision} for field '{field_name}'"
            )
        if scale < 0 or scale > precision:
            raise ValueError(
                f"Decimal scale ({scale}) must be between 0 and precision ({precision}) for field '{field_name}'"
            )
        return precision
