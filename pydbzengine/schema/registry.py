from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

import pyarrow as pa

from pydbzengine.schema.handlers import (
    ArrayTypeHandler,
    BinaryTypeHandler,
    BooleanTypeHandler,
    DataTypeHandler,
    DecimalTypeHandler,
    FloatTypeHandler,
    IntTypeHandler,
    MapTypeHandler,
    SchemaField,
    StringTypeHandler,
    StructTypeHandler,
    VariableScaleDecimalHandler,
)
from pydbzengine.schema.spec import FieldSpec
from pydbzengine.schema.temporal import (
    DateTypeHandler,
    TimestampTypeHandler,
    TimeTypeHandler,
)

__all__ = [
    "TypeRegistry",
]


class TypeRegistry:
    """
    Compiles FieldSpec definitions into bound DataTypeHandler and SchemaField
    instances across primitive, semantic logical, and nested Connect types.
    """

    PRIMITIVE_HANDLERS: ClassVar[dict[str, DataTypeHandler]] = {
        "int8": IntTypeHandler(pa.int8()),
        "int16": IntTypeHandler(pa.int16()),
        "int32": IntTypeHandler(pa.int32()),
        "int64": IntTypeHandler(pa.int64()),
        "float": FloatTypeHandler(pa.float32()),
        "float32": FloatTypeHandler(pa.float32()),
        "double": FloatTypeHandler(pa.float64()),
        "float64": FloatTypeHandler(pa.float64()),
        "boolean": BooleanTypeHandler(),
        "string": StringTypeHandler(),
        "bytes": BinaryTypeHandler(),
    }

    # Logical types that change the target Arrow type or wire decoding semantics.
    # Each entry pairs the required Kafka Connect wire primitive with its handler.
    SEMANTIC_LOGICAL_HANDLERS: ClassVar[dict[str, tuple[str, DataTypeHandler]]] = {
        # Dates
        "io.debezium.time.Date": ("int32", DateTypeHandler()),
        "org.apache.kafka.connect.data.Date": ("int32", DateTypeHandler()),
        "io.debezium.time.IsoDate": ("string", DateTypeHandler()),
        # Times
        "io.debezium.time.Time": ("int32", TimeTypeHandler(unit="ms")),
        "org.apache.kafka.connect.data.Time": ("int32", TimeTypeHandler(unit="ms")),
        "io.debezium.time.MicroTime": ("int64", TimeTypeHandler(unit="us")),
        "io.debezium.time.IsoTime": ("string", TimeTypeHandler(unit="us")),
        "io.debezium.time.ZonedTime": ("string", TimeTypeHandler(unit="us")),
        "io.debezium.time.NanoTime": ("int64", TimeTypeHandler(unit="ns")),
        # Timestamps
        "io.debezium.time.Timestamp": ("int64", TimestampTypeHandler(unit="ms")),
        "org.apache.kafka.connect.data.Timestamp": (
            "int64",
            TimestampTypeHandler(unit="ms"),
        ),
        "io.debezium.time.MicroTimestamp": ("int64", TimestampTypeHandler(unit="us")),
        "io.debezium.time.IsoTimestamp": ("string", TimestampTypeHandler(unit="us")),
        "io.debezium.time.NanoTimestamp": ("int64", TimestampTypeHandler(unit="ns")),
        "io.debezium.time.ZonedTimestamp": (
            "string",
            TimestampTypeHandler(unit="us", tz="UTC"),
        ),
    }

    @staticmethod
    def validate_variable_scale(default_variable_scale: int) -> int:
        if (
            isinstance(default_variable_scale, bool)
            or not isinstance(default_variable_scale, int)
            or default_variable_scale < 0
            or default_variable_scale > 38
        ):
            raise ValueError(
                f"default_variable_scale must be between 0 and 38, got {default_variable_scale!r}"
            )
        return default_variable_scale

    def __init__(self, *, default_variable_scale: int = 10) -> None:
        self.default_variable_scale = self.validate_variable_scale(
            default_variable_scale
        )

    def compile_field(self, spec: FieldSpec) -> SchemaField:
        handler = self.compile_handler(spec)
        return SchemaField(
            name=spec.name,
            handler=handler,
            nullable=spec.optional,
            doc=spec.doc,
            logical_name=spec.logical_name,
        )

    def compile_struct(self, spec: FieldSpec) -> StructTypeHandler:
        handler = self.compile_handler(spec)
        if not isinstance(handler, StructTypeHandler):
            raise ValueError(
                f"Expected struct schema for '{spec.name}', got '{spec.type_name}'"
            )
        return handler

    @staticmethod
    def _require_wire_type(spec: FieldSpec, expected_type: str) -> None:
        if spec.type_name != expected_type:
            raise ValueError(
                f"Expected wire type '{expected_type}' for logical type "
                f"'{spec.logical_name}' in field '{spec.name}', got '{spec.type_name}'"
            )

    def compile_handler(self, spec: FieldSpec) -> DataTypeHandler:
        logical_name = spec.logical_name
        type_str = spec.type_name

        # 1. Fixed semantic logical types (Dates, Times, Timestamps)
        if logical_name is not None and logical_name in self.SEMANTIC_LOGICAL_HANDLERS:
            expected_type, handler = self.SEMANTIC_LOGICAL_HANDLERS[logical_name]
            self._require_wire_type(spec, expected_type)
            return handler

        # 2. Parameterized Decimals
        if logical_name == "io.debezium.data.VariableScaleDecimal":
            self._require_wire_type(spec, "struct")
            precision, scale = self._resolve_decimal_params(
                spec.parameters, require_scale=False
            )
            return VariableScaleDecimalHandler(precision=precision, scale=scale)

        if logical_name == "org.apache.kafka.connect.data.Decimal":
            self._require_wire_type(spec, "bytes")
            precision, scale = self._resolve_decimal_params(
                spec.parameters, require_scale=True
            )
            return DecimalTypeHandler(precision=precision, scale=scale)

        # 3. Standard Scalar Primitives & Binary
        if type_str in self.PRIMITIVE_HANDLERS:
            return self.PRIMITIVE_HANDLERS[type_str]

        # 4. Generic Complex / Nested Types (including Geometry, Point, Vectors, Hstore)
        if type_str == "struct":
            if not spec.fields:
                raise ValueError(f"Struct field '{spec.name}' has no sub-fields")
            return StructTypeHandler(
                tuple(self.compile_field(sub_f) for sub_f in spec.fields)
            )

        if type_str == "array":
            if spec.items is None:
                raise ValueError(f"Array field '{spec.name}' missing 'items' schema")
            return ArrayTypeHandler(self.compile_field(spec.items))

        if type_str == "map":
            if spec.keys is None or spec.values is None:
                raise ValueError(
                    f"Map field '{spec.name}' missing 'keys' or 'values' schema"
                )
            key_field = self.compile_field(spec.keys)
            val_field = self.compile_field(spec.values)
            return MapTypeHandler(key_field=key_field, value_field=val_field)

        raise ValueError(
            f"Unsupported Debezium Connect type: '{type_str}' (logical name: '{logical_name}')"
        )

    def _resolve_decimal_params(
        self,
        params: Mapping[str, str],
        *,
        require_scale: bool,
    ) -> tuple[int, int]:
        if "connect.decimal.precision" in params:
            precision = int(params["connect.decimal.precision"])
        elif "precision" in params:
            precision = int(params["precision"])
        else:
            precision = 38
        if "scale" in params:
            scale = int(params["scale"])
        elif require_scale:
            raise ValueError("Missing required 'scale' parameter in Decimal schema")
        else:
            scale = min(self.default_variable_scale, max(precision, 0))
        return precision, scale
