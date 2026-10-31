from __future__ import annotations

import base64
import binascii
import decimal
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal
from functools import cached_property
from typing import Any, ClassVar, Literal

import pyarrow as pa

__all__ = [
    "ArrayTypeHandler",
    "BinaryTypeHandler",
    "BooleanTypeHandler",
    "DataTypeHandler",
    "DateTypeHandler",
    "DecimalTypeHandler",
    "DenseVectorHandler",
    "FieldSpec",
    "FloatTypeHandler",
    "GeometryTypeHandler",
    "IntTypeHandler",
    "MapTypeHandler",
    "PointTypeHandler",
    "SchemaField",
    "SparseVectorHandler",
    "StringTypeHandler",
    "StructTypeHandler",
    "TemporalIntervalHandler",
    "TimeTypeHandler",
    "TimestampTypeHandler",
    "TypeRegistry",
    "VariableScaleDecimalHandler",
]

TimeUnit = Literal["ms", "us", "ns"]


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """Normalized field schema specification parsed from JSON or Java Connect Schema."""

    name: str
    type_name: str
    optional: bool = True
    logical_name: str | None = None
    doc: str | None = None
    parameters: Mapping[str, str] = field(default_factory=dict)
    fields: tuple[FieldSpec, ...] = ()
    items: FieldSpec | None = None
    keys: FieldSpec | None = None
    values: FieldSpec | None = None

    @classmethod
    def from_json(
        cls,
        raw: Mapping[str, Any],
        *,
        field_name: str | None = None,
    ) -> FieldSpec:
        if not isinstance(raw, Mapping):
            raise ValueError(
                f"Expected dict in schema definition, got {type(raw).__name__}"
            )
        resolved_name = field_name if field_name is not None else raw.get("field")
        if (
            not resolved_name
            or not isinstance(resolved_name, str)
            or not resolved_name.strip()
        ):
            raise ValueError(f"Encountered field without valid name in schema: {raw}")

        raw_type = raw.get("type")
        if not isinstance(raw_type, str) or not raw_type.strip():
            raise ValueError(
                f"Missing or invalid 'type' in schema for field '{resolved_name}': {raw}"
            )
        type_str = raw_type.strip().lower()

        optional = raw.get("optional", True)
        if not isinstance(optional, bool):
            raise ValueError(
                f"Expected bool for 'optional' in schema for field '{resolved_name}', "
                f"got {type(optional).__name__}"
            )
        doc = raw.get("doc")
        if doc is not None and not isinstance(doc, str):
            raise ValueError(
                f"Expected str for 'doc' in schema for field '{resolved_name}', "
                f"got {type(doc).__name__}"
            )
        raw_logical = raw.get("name")
        if raw_logical is not None and not isinstance(raw_logical, str):
            raise ValueError(
                f"Expected str for 'name' in schema for field '{resolved_name}', "
                f"got {type(raw_logical).__name__}"
            )
        logical_name = (
            raw_logical.strip()
            if raw_logical is not None and raw_logical.strip()
            else None
        )

        raw_params = raw.get("parameters")
        if raw_params is not None and not isinstance(raw_params, Mapping):
            raise ValueError(
                f"Expected Mapping for schema 'parameters' in field '{resolved_name}', "
                f"got {type(raw_params).__name__}"
            )
        parameters: dict[str, str] = (
            {str(k): str(v) for k, v in raw_params.items()}
            if raw_params is not None
            else {}
        )

        sub_fields: tuple[FieldSpec, ...] = ()
        if "fields" in raw:
            raw_fields = raw["fields"]
            if not isinstance(raw_fields, list):
                raise ValueError(
                    f"Expected 'fields' list in struct schema for '{resolved_name}', "
                    f"got {type(raw_fields).__name__}"
                )
            sub_fields = tuple(cls.from_json(f) for f in raw_fields)

        items_spec: FieldSpec | None = None
        if type_str == "array":
            raw_items = raw.get("items")
            if raw_items is not None:
                if not isinstance(raw_items, Mapping):
                    raise ValueError(
                        f"Array field '{resolved_name}' has invalid 'items' schema: {raw}"
                    )
                items_spec = cls.from_json(raw_items, field_name="item")
            elif logical_name not in (
                "io.debezium.data.FloatVector",
                "io.debezium.data.DoubleVector",
            ):
                raise ValueError(
                    f"Array field '{resolved_name}' missing valid 'items' schema: {raw}"
                )

        keys_spec: FieldSpec | None = None
        values_spec: FieldSpec | None = None
        if type_str == "map":
            raw_keys = raw.get("keys")
            if raw_keys is not None:
                if not isinstance(raw_keys, Mapping):
                    raise ValueError(
                        f"Map field '{resolved_name}' has invalid 'keys' schema: {raw}"
                    )
                keys_spec = cls.from_json(raw_keys, field_name="key")
            raw_values = raw.get("values")
            if raw_values is not None:
                if not isinstance(raw_values, Mapping):
                    raise ValueError(
                        f"Map field '{resolved_name}' has invalid 'values' schema: {raw}"
                    )
                values_spec = cls.from_json(raw_values, field_name="value")

        return cls(
            name=resolved_name,
            type_name=type_str,
            optional=optional,
            logical_name=logical_name,
            doc=doc,
            parameters=parameters,
            fields=sub_fields,
            items=items_spec,
            keys=keys_spec,
            values=values_spec,
        )

    @classmethod
    def from_connect(
        cls,
        j_schema: Any,
        *,
        field_name: str = "root",
    ) -> FieldSpec:
        if j_schema is None:
            raise ValueError(
                f"Java Connect schema cannot be None for field '{field_name}'"
            )
        j_type = j_schema.type()
        if j_type is None:
            raise ValueError(
                f"Java Connect schema type() cannot be None for field '{field_name}'"
            )
        type_str = str(j_type.name()).strip().lower()
        if not type_str:
            raise ValueError(
                f"Java Connect schema type name cannot be empty for field '{field_name}'"
            )

        optional = bool(j_schema.isOptional())
        raw_doc = j_schema.doc()
        doc = str(raw_doc) if raw_doc else None
        raw_logical = j_schema.name()
        logical_name = str(raw_logical) if raw_logical else None
        raw_params = j_schema.parameters()
        parameters = (
            {str(k): str(v) for k, v in raw_params.items()}
            if raw_params is not None
            else {}
        )

        sub_fields: tuple[FieldSpec, ...] = ()
        items_spec: FieldSpec | None = None
        keys_spec: FieldSpec | None = None
        values_spec: FieldSpec | None = None

        if type_str == "struct":
            j_fields = j_schema.fields()
            if j_fields is None:
                raise ValueError(
                    f"Java Connect STRUCT schema missing fields() for '{field_name}'"
                )
            sub_fields = tuple(
                cls.from_connect(sub_f.schema(), field_name=str(sub_f.name()))
                for sub_f in j_fields
            )
        elif type_str == "array":
            val_schema = j_schema.valueSchema()
            if val_schema is None:
                raise ValueError(
                    f"Java Connect ARRAY schema missing valueSchema() for '{field_name}'"
                )
            items_spec = cls.from_connect(val_schema, field_name="item")
        elif type_str == "map":
            key_schema = j_schema.keySchema()
            val_schema = j_schema.valueSchema()
            if key_schema is None or val_schema is None:
                raise ValueError(
                    f"Java Connect MAP schema missing keySchema() or valueSchema() for '{field_name}'"
                )
            keys_spec = cls.from_connect(key_schema, field_name="key")
            values_spec = cls.from_connect(val_schema, field_name="value")

        return cls(
            name=field_name,
            type_name=type_str,
            optional=optional,
            logical_name=logical_name,
            doc=doc,
            parameters=parameters,
            fields=sub_fields,
            items=items_spec,
            keys=keys_spec,
            values=values_spec,
        )


class DataTypeHandler(ABC):
    """Base contract for compiled field data type handlers."""

    @property
    @abstractmethod
    def arrow_type(self) -> pa.DataType:
        """The compiled PyArrow data type for this field."""

    @abstractmethod
    def convert_json(self, value: Any) -> Any:
        """Converts a non-null JSON wire value using this handler's compiled field schema."""

    @abstractmethod
    def convert_connect(self, value: Any) -> Any:
        """Converts a non-null Java Connect value using this handler's compiled field schema."""


@dataclass(frozen=True)
class SchemaField:
    """Compiled field pairing field metadata with its bound DataTypeHandler."""

    name: str
    handler: DataTypeHandler
    nullable: bool = True
    doc: str | None = None
    logical_name: str | None = None

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("SchemaField name cannot be empty.")

    @cached_property
    def arrow_field(self) -> pa.Field:
        metadata: dict[bytes, bytes] = {}
        if self.doc:
            metadata[b"doc"] = self.doc.encode("utf-8")
        if self.logical_name:
            metadata[b"debezium.logical_type"] = self.logical_name.encode("utf-8")
        return pa.field(
            self.name,
            self.handler.arrow_type,
            nullable=self.nullable,
            metadata=metadata if metadata else None,
        )

    @property
    def arrow_type(self) -> pa.DataType:
        return self.handler.arrow_type

    def convert_json(self, value: Any) -> Any:
        if value is None:
            return None
        return self.handler.convert_json(value)

    def convert_connect(self, value: Any) -> Any:
        if value is None:
            return None
        return self.handler.convert_connect(value)


# ============================================================================
# Family 1: Scalar Primitives & Temporal Intervals
# ============================================================================


@dataclass(frozen=True, slots=True)
class BooleanTypeHandler(DataTypeHandler):
    """Handles boolean fields."""

    @property
    def arrow_type(self) -> pa.DataType:
        return pa.bool_()

    def convert_json(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        raise TypeError(f"Expected bool, got {type(value).__name__}: {value!r}")

    def convert_connect(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if hasattr(value, "booleanValue"):
            return bool(value.booleanValue())
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} to bool: {value!r}"
        )


@dataclass(frozen=True, slots=True)
class IntTypeHandler(DataTypeHandler):
    """Handles signed integer fields (int8, int16, int32, int64)."""

    arrow_type: pa.DataType

    def __post_init__(self) -> None:
        if not pa.types.is_integer(self.arrow_type):
            raise ValueError(f"Expected integer Arrow type, got: {self.arrow_type}")

    @staticmethod
    def decode_json_int(value: Any) -> int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        raise TypeError(f"Expected int, got {type(value).__name__}: {value!r}")

    @staticmethod
    def decode_connect_int(value: Any) -> int:
        if isinstance(value, (bool, float)) or hasattr(value, "booleanValue"):
            raise TypeError(f"Cannot convert {type(value).__name__} {value!r} to int")
        if isinstance(value, int):
            return value
        if hasattr(value, "longValue"):
            return int(value.longValue())
        if hasattr(value, "intValue"):
            return int(value.intValue())
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} to int: {value!r}"
        )

    def convert_json(self, value: Any) -> int:
        return self.decode_json_int(value)

    def convert_connect(self, value: Any) -> int:
        return self.decode_connect_int(value)


@dataclass(frozen=True, slots=True)
class FloatTypeHandler(DataTypeHandler):
    """Handles floating-point fields (float32, float64)."""

    arrow_type: pa.DataType

    def __post_init__(self) -> None:
        if not pa.types.is_floating(self.arrow_type):
            raise ValueError(f"Expected floating Arrow type, got: {self.arrow_type}")

    def convert_json(self, value: Any) -> float:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        raise TypeError(f"Expected float, got {type(value).__name__}: {value!r}")

    def convert_connect(self, value: Any) -> float:
        if isinstance(value, bool) or hasattr(value, "booleanValue"):
            raise TypeError(f"Cannot convert boolean {value!r} to float")
        if isinstance(value, (int, float)):
            return float(value)
        if hasattr(value, "doubleValue"):
            return float(value.doubleValue())
        if hasattr(value, "floatValue"):
            return float(value.floatValue())
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} to float: {value!r}"
        )


@dataclass(frozen=True, slots=True)
class StringTypeHandler(DataTypeHandler):
    """Handles UTF-8 strings and string-backed logical types (Uuid, Json, Xml, Enum, Ltree, etc.)."""

    @property
    def arrow_type(self) -> pa.DataType:
        return pa.string()

    @staticmethod
    def decode_json_str(value: Any) -> str:
        if isinstance(value, str):
            return value
        raise TypeError(f"Expected str, got {type(value).__name__}: {value!r}")

    @staticmethod
    def decode_connect_str(value: Any) -> str:
        if isinstance(value, str) or type(value).__name__ == "java.lang.String":
            return str(value)
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} to str: {value!r}"
        )

    def convert_json(self, value: Any) -> str:
        return self.decode_json_str(value)

    def convert_connect(self, value: Any) -> str:
        return self.decode_connect_str(value)


@dataclass(frozen=True, slots=True)
class TemporalIntervalHandler(DataTypeHandler):
    """Handles Debezium temporal duration, interval, and year logical types."""

    arrow_type: pa.DataType

    def __post_init__(self) -> None:
        if not (
            pa.types.is_integer(self.arrow_type) or pa.types.is_string(self.arrow_type)
        ):
            raise ValueError(
                f"Expected integer or string Arrow type for TemporalIntervalHandler, got: {self.arrow_type}"
            )

    def convert_json(self, value: Any) -> int | str:
        if pa.types.is_integer(self.arrow_type):
            return IntTypeHandler.decode_json_int(value)
        return StringTypeHandler.decode_json_str(value)

    def convert_connect(self, value: Any) -> int | str:
        if pa.types.is_integer(self.arrow_type):
            return IntTypeHandler.decode_connect_int(value)
        return StringTypeHandler.decode_connect_str(value)


# ============================================================================
# Family 2: Binary & Bits
# ============================================================================


@dataclass(frozen=True, slots=True)
class BinaryTypeHandler(DataTypeHandler):
    """Handles binary and bit-set types (bytes, io.debezium.data.Bits)."""

    @property
    def arrow_type(self) -> pa.DataType:
        return pa.binary()

    @staticmethod
    def unwrap_connect_bytes(value: Any) -> bytes | None:
        if isinstance(value, bytes):
            return value
        if isinstance(value, (bytearray, memoryview)):
            return bytes(value)
        if type(value).__name__ == "byte[]":
            return bytes(b & 0xFF for b in value)
        if hasattr(value, "array"):
            arr = value.array()
            raw = (
                bytes(arr)
                if isinstance(arr, (bytes, bytearray, memoryview))
                else bytes(b & 0xFF for b in arr)
            )
            if hasattr(value, "position") and hasattr(value, "remaining"):
                offset = int(value.position())
                if hasattr(value, "arrayOffset"):
                    offset += int(value.arrayOffset())
                length = int(value.remaining())
                return raw[offset : offset + length]
            return raw
        return None

    def convert_json(self, value: Any) -> bytes:
        if not isinstance(value, str):
            raise TypeError(
                f"Expected Base64 or hex str for JSON binary field, got {type(value).__name__}: {value!r}"
            )
        s = value.strip()
        if s.startswith(("0x", "0X")):
            return bytes.fromhex(s[2:])
        try:
            return base64.b64decode(s, validate=True)
        except (binascii.Error, ValueError) as err:
            raise ValueError(
                f"Invalid base64 string for binary field: {value!r}"
            ) from err

    def convert_connect(self, value: Any) -> bytes:
        raw_bytes = self.unwrap_connect_bytes(value)
        if raw_bytes is not None:
            return raw_bytes
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} into bytes: {value!r}"
        )


# ============================================================================
# Family 3: Decimals (Fixed-Scale & Variable-Scale)
# ============================================================================


@dataclass(frozen=True, slots=True)
class DecimalTypeHandler(DataTypeHandler):
    """Handles fixed-scale Kafka Connect Decimal fields."""

    _NUMERIC_LITERAL_RE: ClassVar[re.Pattern[str]] = re.compile(
        r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$"
    )

    precision: int = 38
    scale: int = 10

    def __post_init__(self) -> None:
        self.validate_bounds(self.precision, self.scale)

    @staticmethod
    def validate_bounds(precision: int, scale: int) -> None:
        if isinstance(precision, bool) or precision < 1 or precision > 38:
            raise ValueError(
                f"Decimal precision must be between 1 and 38, got {precision!r}"
            )
        if isinstance(scale, bool) or scale < 0 or scale > precision:
            raise ValueError(
                f"Decimal scale ({scale!r}) must be between 0 and precision ({precision})"
            )

    @staticmethod
    def _scale_int(int_val: int, scale: int) -> Decimal:
        if scale == 0:
            return Decimal(int_val)
        if scale < 0:
            return Decimal(int_val * (10 ** (-scale)))
        sign = 0 if int_val >= 0 else 1
        digits = tuple(int(d) for d in str(abs(int_val)))
        return Decimal((sign, digits, -scale))

    @staticmethod
    def quantize(dec: Decimal, scale: int) -> Decimal:
        if not dec.is_finite():
            raise ValueError(f"Cannot quantize non-finite Decimal value: {dec!r}")
        exp = dec.as_tuple().exponent
        assert isinstance(exp, int)
        if -exp > scale:
            quantizer = Decimal((0, (1,), -scale))
            return dec.quantize(quantizer, rounding=decimal.ROUND_HALF_UP)
        return dec

    @classmethod
    def decode_json(cls, raw: Any, scale: int = 0) -> Decimal:
        """Decodes a JSON decimal wire value (Base64 bytes, numeric literal str, int, or finite float)."""
        if raw is None:
            raise ValueError("Cannot decode None as Decimal")
        if isinstance(raw, bool):
            raise TypeError(f"Cannot decode boolean {raw!r} as Decimal")
        if isinstance(raw, int):
            return Decimal(raw)
        if isinstance(raw, float):
            dec = Decimal(str(raw))
            if not dec.is_finite():
                raise ValueError(f"Cannot decode non-finite float as Decimal: {raw!r}")
            return dec
        if not isinstance(raw, str):
            raise TypeError(
                f"Cannot decode JSON {type(raw).__name__} as Decimal: {raw!r}"
            )

        raw_str = raw.strip()
        if not raw_str:
            raise ValueError("Cannot decode empty string as Decimal")

        if cls._NUMERIC_LITERAL_RE.match(raw_str):
            try:
                return Decimal(raw_str)
            except decimal.InvalidOperation as err:
                raise ValueError(f"Malformed decimal string: {raw_str!r}") from err

        try:
            decoded_bytes = base64.b64decode(raw_str, validate=True)
        except (binascii.Error, ValueError) as err:
            raise ValueError(f"Malformed decimal string: {raw_str!r}") from err

        int_val = int.from_bytes(decoded_bytes, byteorder="big", signed=True)
        return cls._scale_int(int_val, scale)

    @classmethod
    def decode_connect(cls, raw: Any, scale: int = 0) -> Decimal:
        """Decodes a Java Connect decimal value (BigDecimal, Python Decimal, or unscaled byte array/buffer)."""
        if raw is None:
            raise ValueError("Cannot decode None as Decimal")
        if isinstance(raw, Decimal):
            if not raw.is_finite():
                raise ValueError(f"Cannot decode non-finite Decimal value: {raw!r}")
            return raw
        if hasattr(raw, "toPlainString"):
            return Decimal(str(raw.toPlainString()))
        if isinstance(raw, bool) or hasattr(raw, "booleanValue"):
            raise TypeError(f"Cannot decode boolean {raw!r} as Decimal")
        raw_bytes = BinaryTypeHandler.unwrap_connect_bytes(raw)
        if raw_bytes is not None:
            int_val = int.from_bytes(raw_bytes, byteorder="big", signed=True)
            return cls._scale_int(int_val, scale)
        raise TypeError(
            f"Cannot decode Connect {type(raw).__name__} as Decimal: {raw!r}"
        )

    @property
    def arrow_type(self) -> pa.DataType:
        return pa.decimal128(self.precision, self.scale)

    def convert_json(self, value: Any) -> Decimal:
        return self.quantize(self.decode_json(value, scale=self.scale), self.scale)

    def convert_connect(self, value: Any) -> Decimal:
        return self.quantize(self.decode_connect(value, scale=self.scale), self.scale)


@dataclass(frozen=True, slots=True)
class VariableScaleDecimalHandler(DataTypeHandler):
    """Handles Debezium VariableScaleDecimal struct fields ({scale: int, value: bytes})."""

    precision: int = 38
    scale: int = 10

    def __post_init__(self) -> None:
        DecimalTypeHandler.validate_bounds(self.precision, self.scale)

    @property
    def arrow_type(self) -> pa.DataType:
        return pa.decimal128(self.precision, self.scale)

    def convert_json(self, value: Any) -> Decimal | None:
        if not isinstance(value, Mapping):
            raise TypeError(
                f"Expected Mapping for VariableScaleDecimal, got {type(value).__name__}: {value!r}"
            )
        if "value" not in value:
            raise ValueError(f"VariableScaleDecimal missing 'value' field: {value!r}")
        raw_val = value["value"]
        if raw_val is None:
            return None
        if "scale" not in value or value["scale"] is None:
            raise ValueError(
                f"VariableScaleDecimal missing required 'scale' field: {value!r}"
            )
        scale_val = IntTypeHandler.decode_json_int(value["scale"])
        return DecimalTypeHandler.quantize(
            DecimalTypeHandler.decode_json(raw_val, scale=scale_val),
            self.scale,
        )

    def convert_connect(self, value: Any) -> Decimal | None:
        getter = StructTypeHandler.get_connect_field_getter(value)
        if getter is None:
            raise TypeError(
                f"Expected Connect Struct for VariableScaleDecimal, "
                f"got {type(value).__name__}: {value!r}"
            )
        raw_val = getter("value")
        if raw_val is None:
            return None
        raw_scale = getter("scale")
        if raw_scale is None:
            raise ValueError(
                "VariableScaleDecimal Connect Struct missing required 'scale' value"
            )
        val_scale = IntTypeHandler.decode_connect_int(raw_scale)
        return DecimalTypeHandler.quantize(
            DecimalTypeHandler.decode_connect(raw_val, scale=val_scale),
            self.scale,
        )


# ============================================================================
# Family 4: Temporals (Dates, Times, Timestamps)
# ============================================================================


@dataclass(frozen=True, slots=True)
class DateTypeHandler(DataTypeHandler):
    """Handles Debezium and Connect Date types (epoch days or ISO-8601 date strings)."""

    @property
    def arrow_type(self) -> pa.DataType:
        return pa.date32()

    @staticmethod
    def parse_iso_date(val: str) -> date:
        s = val.strip()
        if "T" in s or "t" in s or " " in s:
            return TimestampTypeHandler.parse_iso_timestamp(s, tz_aware=False).date()
        if s.endswith(("Z", "z")):
            s = s[:-1]
        try:
            return date.fromisoformat(s)
        except ValueError as e:
            raise ValueError(f"Invalid date string format: {val!r}") from e

    def convert_json(self, value: Any) -> date | int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str):
            return self.parse_iso_date(value)
        raise TypeError(f"Cannot decode {type(value).__name__} into date: {value!r}")

    def convert_connect(self, value: Any) -> date | int:
        if isinstance(value, (bool, float)) or hasattr(value, "booleanValue"):
            raise TypeError(
                f"Cannot convert {type(value).__name__} {value!r} into date"
            )
        if isinstance(value, int):
            return value
        if hasattr(value, "toEpochDay"):
            return int(value.toEpochDay())
        if hasattr(value, "getTime"):
            return int(value.getTime()) // 86_400_000
        if hasattr(value, "intValue"):
            return int(value.intValue())
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str) or type(value).__name__ == "java.lang.String":
            return self.parse_iso_date(str(value))
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} into date: {value!r}"
        )


@dataclass(frozen=True, slots=True)
class TimeTypeHandler(DataTypeHandler):
    """Handles Debezium and Connect Time types (ms, us, ns, or ISO-8601 time strings)."""

    unit: TimeUnit = "us"

    def __post_init__(self) -> None:
        if self.unit not in ("ms", "us", "ns"):
            raise ValueError(f"Unsupported time unit: {self.unit!r}")

    @property
    def arrow_type(self) -> pa.DataType:
        if self.unit == "ms":
            return pa.time32("ms")
        return pa.time64(self.unit)

    @staticmethod
    def parse_iso_time(val: str) -> time:
        s = val.strip()
        if s.endswith(("Z", "z")):
            s = s[:-1] + "+00:00"
        s = TimestampTypeHandler.truncate_fraction_to_micros(s)
        try:
            return time.fromisoformat(s)
        except ValueError as e:
            raise ValueError(f"Invalid time string format: {val!r}") from e

    @classmethod
    def parse_iso_time_nanos(cls, val: str) -> int:
        t = cls.parse_iso_time(val)
        total_us = (
            t.hour * 3_600 + t.minute * 60 + t.second
        ) * 1_000_000 + t.microsecond
        return total_us * 1_000 + TimestampTypeHandler._extract_sub_micro_nanos(val)

    def convert_json(self, value: Any) -> time | int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str):
            if self.unit == "ns":
                return self.parse_iso_time_nanos(value)
            return self.parse_iso_time(value)
        raise TypeError(f"Cannot decode {type(value).__name__} into time: {value!r}")

    def convert_connect(self, value: Any) -> time | int:
        if isinstance(value, (bool, float)) or hasattr(value, "booleanValue"):
            raise TypeError(
                f"Cannot convert {type(value).__name__} {value!r} into time"
            )
        if isinstance(value, int):
            return value
        if hasattr(value, "toNanoOfDay"):
            return TimestampTypeHandler.scale_ns_to_unit(
                int(value.toNanoOfDay()), self.unit
            )
        if hasattr(value, "getTime"):
            return TimestampTypeHandler.scale_ms_to_unit(
                int(value.getTime()), self.unit
            )
        if hasattr(value, "longValue"):
            return int(value.longValue())
        if hasattr(value, "intValue"):
            return int(value.intValue())
        if isinstance(value, datetime):
            return value.time()
        if isinstance(value, time):
            return value
        if isinstance(value, str) or type(value).__name__ == "java.lang.String":
            s = str(value)
            if self.unit == "ns":
                return self.parse_iso_time_nanos(s)
            return self.parse_iso_time(s)
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} into time: {value!r}"
        )


@dataclass(frozen=True, slots=True)
class TimestampTypeHandler(DataTypeHandler):
    """Handles Debezium and Connect Timestamp types (ms, us, ns, naive or UTC-zoned)."""

    unit: TimeUnit = "us"
    tz: str | None = None

    def __post_init__(self) -> None:
        if self.unit not in ("ms", "us", "ns"):
            raise ValueError(f"Unsupported timestamp unit: {self.unit!r}")

    @property
    def arrow_type(self) -> pa.DataType:
        return pa.timestamp(self.unit, tz=self.tz)

    @staticmethod
    def truncate_fraction_to_micros(s: str) -> str:
        if "." not in s:
            return s
        prefix, remainder = s.split(".", 1)
        i = 0
        while i < len(remainder) and remainder[i].isdigit():
            i += 1
        if i == 0:
            return s
        return f"{prefix}.{remainder[:i][:6].ljust(6, '0')}{remainder[i:]}"

    @staticmethod
    def _extract_sub_micro_nanos(s: str) -> int:
        if "." not in s:
            return 0
        _, remainder = s.split(".", 1)
        i = 0
        while i < len(remainder) and remainder[i].isdigit():
            i += 1
        if i <= 6:
            return 0
        return int(remainder[6 : min(i, 9)].ljust(3, "0"))

    @staticmethod
    def scale_ms_to_unit(ms: int, unit: TimeUnit) -> int:
        if unit == "ms":
            return ms
        if unit == "us":
            return ms * 1_000
        if unit == "ns":
            return ms * 1_000_000
        raise ValueError(f"Unsupported time unit: {unit!r}")

    @staticmethod
    def scale_ns_to_unit(ns: int, unit: TimeUnit) -> int:
        if unit == "ms":
            return ns // 1_000_000
        if unit == "us":
            return ns // 1_000
        if unit == "ns":
            return ns
        raise ValueError(f"Unsupported time unit: {unit!r}")

    @classmethod
    def parse_iso_timestamp(cls, val: str, tz_aware: bool) -> datetime:
        s = val.strip().replace("Z", "+00:00").replace("z", "+00:00")
        s = cls.truncate_fraction_to_micros(s)
        try:
            dt = datetime.fromisoformat(s)
            if tz_aware and dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            if not tz_aware and dt.tzinfo is not None:
                return dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except ValueError as e:
            raise ValueError(f"Invalid timestamp ISO string format: {val!r}") from e

    @classmethod
    def parse_iso_timestamp_nanos(cls, val: str) -> int:
        dt = cls.parse_iso_timestamp(val, tz_aware=True).astimezone(timezone.utc)
        delta = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)
        epoch_us = (
            delta.days * 86_400 + delta.seconds
        ) * 1_000_000 + delta.microseconds
        return epoch_us * 1_000 + cls._extract_sub_micro_nanos(val)

    def convert_json(self, value: Any) -> datetime | int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str):
            if self.unit == "ns":
                return self.parse_iso_timestamp_nanos(value)
            return self.parse_iso_timestamp(value, tz_aware=self.tz is not None)
        raise TypeError(
            f"Cannot decode {type(value).__name__} into timestamp: {value!r}"
        )

    def convert_connect(self, value: Any) -> datetime | int:
        if isinstance(value, (bool, float)) or hasattr(value, "booleanValue"):
            raise TypeError(
                f"Cannot convert {type(value).__name__} {value!r} into timestamp"
            )
        if isinstance(value, int):
            return value
        if hasattr(value, "toInstant"):
            value = value.toInstant()
        if hasattr(value, "getEpochSecond") and hasattr(value, "getNano"):
            total_ns = int(value.getEpochSecond()) * 1_000_000_000 + int(
                value.getNano()
            )
            return self.scale_ns_to_unit(total_ns, self.unit)
        if hasattr(value, "getTime"):
            ms = int(value.getTime())
            if hasattr(value, "getNanos"):
                total_ns = (ms // 1_000) * 1_000_000_000 + int(value.getNanos())
                return self.scale_ns_to_unit(total_ns, self.unit)
            return self.scale_ms_to_unit(ms, self.unit)
        if hasattr(value, "longValue"):
            return int(value.longValue())
        if isinstance(value, datetime):
            if self.tz is not None:
                return (
                    value.replace(tzinfo=timezone.utc)
                    if value.tzinfo is None
                    else value
                )
            return (
                value.astimezone(timezone.utc).replace(tzinfo=None)
                if value.tzinfo
                else value
            )
        if isinstance(value, str) or type(value).__name__ == "java.lang.String":
            s = str(value)
            if self.unit == "ns":
                return self.parse_iso_timestamp_nanos(s)
            return self.parse_iso_timestamp(s, tz_aware=self.tz is not None)
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} into timestamp: {value!r}"
        )


# ============================================================================
# Family 5: Complex, Nested, Geospatial, & Vector Types
# ============================================================================


@dataclass(frozen=True)
class StructTypeHandler(DataTypeHandler):
    """Handles Kafka Connect struct schemas and nested record objects."""

    fields: tuple[SchemaField, ...] = ()

    @cached_property
    def arrow_type(self) -> pa.StructType:
        return pa.struct([f.arrow_field for f in self.fields])

    @cached_property
    def arrow_schema(self) -> pa.Schema:
        return pa.schema([f.arrow_field for f in self.fields])

    @staticmethod
    def get_connect_field_getter(value: Any) -> Callable[[str], Any] | None:
        if hasattr(value, "getWithoutDefault"):
            return value.getWithoutDefault  # type: ignore[no-any-return]
        if hasattr(value, "get"):
            return value.get  # type: ignore[no-any-return]
        return None

    def convert_json(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise TypeError(
                f"Expected dict or Mapping for struct field, got {type(value).__name__}: {value!r}"
            )
        return {f.name: f.convert_json(value.get(f.name)) for f in self.fields}

    def convert_connect(self, value: Any) -> dict[str, Any]:
        getter = self.get_connect_field_getter(value)
        if getter is None:
            raise TypeError(
                f"Expected Connect Struct for struct field, got {type(value).__name__}: {value!r}"
            )
        return {f.name: f.convert_connect(getter(f.name)) for f in self.fields}


@dataclass(frozen=True)
class ArrayTypeHandler(DataTypeHandler):
    """Handles Kafka Connect array schemas."""

    item_field: SchemaField

    @cached_property
    def arrow_type(self) -> pa.ListType:
        return pa.list_(self.item_field.arrow_type)

    def convert_json(self, value: Any) -> list[Any]:
        if not isinstance(value, Sequence) or isinstance(
            value, (str, bytes, bytearray, memoryview)
        ):
            raise TypeError(
                f"Expected sequence for array field, got {type(value).__name__}: {value!r}"
            )
        return [self.item_field.convert_json(x) for x in value]

    def convert_connect(self, value: Any) -> list[Any]:
        if isinstance(value, (str, bytes, bytearray, memoryview)) or not hasattr(
            value, "__iter__"
        ):
            raise TypeError(
                f"Expected iterable sequence for Connect array field, got {type(value).__name__}: {value!r}"
            )
        return [self.item_field.convert_connect(x) for x in value]


@dataclass(frozen=True)
class MapTypeHandler(DataTypeHandler):
    """Handles Kafka Connect map and PostgreSQL Hstore schemas."""

    key_field: SchemaField
    value_field: SchemaField

    def __post_init__(self) -> None:
        if pa.types.is_nested(self.key_field.arrow_type):
            raise ValueError(
                f"Invalid key type for map field: {self.key_field.arrow_type}. "
                "Map keys cannot be struct, array, or map."
            )

    @cached_property
    def arrow_type(self) -> pa.MapType:
        return pa.map_(self.key_field.arrow_type, self.value_field.arrow_type)

    def _convert_json_key(self, raw_key: Any) -> Any:
        if raw_key is None:
            raise ValueError("Map key cannot be None")
        if isinstance(raw_key, str):
            key_dtype = self.key_field.arrow_type
            if pa.types.is_integer(key_dtype):
                raw_key = int(raw_key)
            elif pa.types.is_floating(key_dtype):
                raw_key = float(raw_key)
            elif pa.types.is_boolean(key_dtype):
                norm = raw_key.strip().lower()
                if norm in ("true", "false"):
                    raw_key = norm == "true"
        return self.key_field.handler.convert_json(raw_key)

    def _convert_connect_key(self, raw_key: Any) -> Any:
        if raw_key is None:
            raise ValueError("Map key cannot be None")
        return self.key_field.handler.convert_connect(raw_key)

    def convert_json(self, value: Any) -> dict[Any, Any]:
        if isinstance(value, Mapping):
            return {
                self._convert_json_key(k): self.value_field.convert_json(v)
                for k, v in value.items()
            }
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray, memoryview)
        ):
            result: dict[Any, Any] = {}
            for item in value:
                if (
                    not isinstance(item, Sequence)
                    or isinstance(item, (str, bytes, bytearray, memoryview))
                    or len(item) != 2
                ):
                    raise ValueError(
                        f"Expected [key, value] pair in map array, got {item!r}"
                    )
                result[self._convert_json_key(item[0])] = self.value_field.convert_json(
                    item[1]
                )
            return result
        raise TypeError(
            f"Expected Mapping or sequence of [key, value] pairs for map field, "
            f"got {type(value).__name__}: {value!r}"
        )

    def convert_connect(self, value: Any) -> dict[Any, Any]:
        if hasattr(value, "keySet"):
            return {
                self._convert_connect_key(k): self.value_field.convert_connect(
                    value.get(k)
                )
                for k in value.keySet()
            }
        if isinstance(value, Mapping):
            return {
                self._convert_connect_key(k): self.value_field.convert_connect(v)
                for k, v in value.items()
            }
        raise TypeError(
            f"Expected Mapping for Connect map field, got {type(value).__name__}: {value!r}"
        )


@dataclass(frozen=True)
class GeometryTypeHandler(StructTypeHandler):
    """Handles Debezium Geometry and Geography logical struct types ({wkb: bytes, srid: int32})."""

    fields: tuple[SchemaField, ...] = (
        SchemaField("wkb", BinaryTypeHandler()),
        SchemaField("srid", IntTypeHandler(pa.int32())),
    )


@dataclass(frozen=True)
class PointTypeHandler(StructTypeHandler):
    """Handles Debezium Point logical struct type ({x: float64, y: float64, wkb: bytes, srid: int32})."""

    fields: tuple[SchemaField, ...] = (
        SchemaField("x", FloatTypeHandler(pa.float64()), nullable=False),
        SchemaField("y", FloatTypeHandler(pa.float64()), nullable=False),
        SchemaField("wkb", BinaryTypeHandler(), nullable=True),
        SchemaField("srid", IntTypeHandler(pa.int32()), nullable=True),
    )


@dataclass(frozen=True)
class DenseVectorHandler(ArrayTypeHandler):
    """Handles Debezium dense vector logical types (FloatVector, DoubleVector)."""


@dataclass(frozen=True)
class SparseVectorHandler(StructTypeHandler):
    """Handles Debezium SparseDoubleVector logical type ({dimensions: int16, vector: map<int16, float64>})."""

    fields: tuple[SchemaField, ...] = (
        SchemaField("dimensions", IntTypeHandler(pa.int16())),
        SchemaField(
            "vector",
            MapTypeHandler(
                key_field=SchemaField(
                    "key", IntTypeHandler(pa.int16()), nullable=False
                ),
                value_field=SchemaField(
                    "value", FloatTypeHandler(pa.float64()), nullable=True
                ),
            ),
        ),
    )


# ============================================================================
# Central TypeRegistry
# ============================================================================


class TypeRegistry:
    """
    Central registry that compiles FieldSpec definitions into bound DataTypeHandler
    and SchemaField instances across all data type families.
    """

    STRING_HANDLER: ClassVar[StringTypeHandler] = StringTypeHandler()
    BOOL_HANDLER: ClassVar[BooleanTypeHandler] = BooleanTypeHandler()
    BINARY_HANDLER: ClassVar[BinaryTypeHandler] = BinaryTypeHandler()

    PRIMITIVE_HANDLERS: ClassVar[dict[str, DataTypeHandler]] = {
        "int8": IntTypeHandler(pa.int8()),
        "int16": IntTypeHandler(pa.int16()),
        "int32": IntTypeHandler(pa.int32()),
        "int64": IntTypeHandler(pa.int64()),
        "float": FloatTypeHandler(pa.float32()),
        "float32": FloatTypeHandler(pa.float32()),
        "double": FloatTypeHandler(pa.float64()),
        "float64": FloatTypeHandler(pa.float64()),
        "boolean": BOOL_HANDLER,
        "string": STRING_HANDLER,
        "bytes": BINARY_HANDLER,
    }

    HSTORE_MAP_HANDLER: ClassVar[MapTypeHandler] = MapTypeHandler(
        key_field=SchemaField("key", STRING_HANDLER, nullable=False),
        value_field=SchemaField("value", STRING_HANDLER, nullable=True),
    )

    FIXED_LOGICAL_HANDLERS: ClassVar[dict[str, DataTypeHandler]] = {
        # Dates
        "io.debezium.time.Date": DateTypeHandler(),
        "org.apache.kafka.connect.data.Date": DateTypeHandler(),
        "io.debezium.time.IsoDate": DateTypeHandler(),
        # Times
        "io.debezium.time.Time": TimeTypeHandler(unit="ms"),
        "org.apache.kafka.connect.data.Time": TimeTypeHandler(unit="ms"),
        "io.debezium.time.MicroTime": TimeTypeHandler(unit="us"),
        "io.debezium.time.IsoTime": TimeTypeHandler(unit="us"),
        "io.debezium.time.ZonedTime": TimeTypeHandler(unit="us"),
        "io.debezium.time.NanoTime": TimeTypeHandler(unit="ns"),
        # Timestamps
        "io.debezium.time.Timestamp": TimestampTypeHandler(unit="ms"),
        "org.apache.kafka.connect.data.Timestamp": TimestampTypeHandler(unit="ms"),
        "io.debezium.connector.mongodb.data.BsonTimestamp": TimestampTypeHandler(
            unit="ms"
        ),
        "io.debezium.data.BsonTimestamp": TimestampTypeHandler(unit="ms"),
        "io.debezium.time.MicroTimestamp": TimestampTypeHandler(unit="us"),
        "io.debezium.time.IsoTimestamp": TimestampTypeHandler(unit="us"),
        "io.debezium.time.NanoTimestamp": TimestampTypeHandler(unit="ns"),
        "io.debezium.time.ZonedTimestamp": TimestampTypeHandler(unit="us", tz="UTC"),
        # Durations, Intervals, Year
        "io.debezium.time.Year": TemporalIntervalHandler(pa.int32()),
        "io.debezium.connector.oracle.data.IntervalYearToMonth": TemporalIntervalHandler(
            pa.int32()
        ),
        "io.debezium.time.IntervalYearToMonth": TemporalIntervalHandler(pa.int32()),
        "io.debezium.time.MicroDuration": TemporalIntervalHandler(pa.int64()),
        "io.debezium.time.NanoDuration": TemporalIntervalHandler(pa.int64()),
        "io.debezium.connector.oracle.data.IntervalDayToSecond": TemporalIntervalHandler(
            pa.int64()
        ),
        "io.debezium.time.IntervalDayToSecond": TemporalIntervalHandler(pa.int64()),
        "io.debezium.time.Interval": TemporalIntervalHandler(pa.string()),
        # Text, Enums, UUID, JSON, XML, Ltree, ObjectId, Tsvector
        "io.debezium.data.Uuid": STRING_HANDLER,
        "io.debezium.data.Json": STRING_HANDLER,
        "io.debezium.data.Xml": STRING_HANDLER,
        "io.debezium.data.Enum": STRING_HANDLER,
        "io.debezium.data.EnumSet": STRING_HANDLER,
        "io.debezium.connector.postgresql.data.Ltree": STRING_HANDLER,
        "io.debezium.data.Ltree": STRING_HANDLER,
        "io.debezium.connector.mongodb.data.ObjectId": STRING_HANDLER,
        "io.debezium.data.ObjectId": STRING_HANDLER,
        "io.debezium.data.Tsvector": STRING_HANDLER,
        # Binaries & Bit sets
        "io.debezium.data.Bits": BINARY_HANDLER,
        # Geospatial Geometry & Point
        "io.debezium.data.geometry.Geometry": GeometryTypeHandler(),
        "io.debezium.data.geometry.Geography": GeometryTypeHandler(),
        "io.debezium.data.geometry.Point": PointTypeHandler(),
        # Dense & Sparse Vectors
        "io.debezium.data.SparseDoubleVector": SparseVectorHandler(),
        "io.debezium.data.FloatVector": DenseVectorHandler(
            SchemaField("item", FloatTypeHandler(pa.float32()))
        ),
        "io.debezium.data.DoubleVector": DenseVectorHandler(
            SchemaField("item", FloatTypeHandler(pa.float64()))
        ),
    }

    def __init__(self, *, default_variable_scale: int = 10) -> None:
        if (
            isinstance(default_variable_scale, bool)
            or default_variable_scale < 0
            or default_variable_scale > 38
        ):
            raise ValueError(
                f"default_variable_scale must be between 0 and 38, got {default_variable_scale!r}"
            )
        self.default_variable_scale = default_variable_scale

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

    def compile_handler(self, spec: FieldSpec) -> DataTypeHandler:
        logical_name = spec.logical_name
        type_str = spec.type_name

        # 1. Fixed Debezium & Connect logical types (including Geometry, Point, Vectors)
        if logical_name is not None and logical_name in self.FIXED_LOGICAL_HANDLERS:
            return self.FIXED_LOGICAL_HANDLERS[logical_name]

        # 2. Parameterized Decimals
        if logical_name == "io.debezium.data.VariableScaleDecimal":
            precision, scale = self._resolve_decimal_params(
                spec.parameters, require_scale=False
            )
            return VariableScaleDecimalHandler(precision=precision, scale=scale)

        if logical_name == "org.apache.kafka.connect.data.Decimal":
            precision, scale = self._resolve_decimal_params(
                spec.parameters, require_scale=True
            )
            return DecimalTypeHandler(precision=precision, scale=scale)

        # 3. Hstore
        if logical_name in (
            "io.debezium.connector.postgresql.data.Hstore",
            "io.debezium.data.Hstore",
        ):
            if type_str == "string":
                return self.STRING_HANDLER
            if type_str == "map":
                return self.HSTORE_MAP_HANDLER
            raise ValueError(
                f"Unsupported type '{type_str}' for Hstore field '{spec.name}'"
            )

        # 4. Standard Scalar Primitives & Binary
        if type_str in self.PRIMITIVE_HANDLERS:
            return self.PRIMITIVE_HANDLERS[type_str]

        # 5. Generic Complex / Nested Types
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
        if "scale" in params:
            scale = int(params["scale"])
        elif require_scale:
            raise ValueError("Missing required 'scale' parameter in Decimal schema")
        else:
            scale = self.default_variable_scale
        if "connect.decimal.precision" in params:
            precision = int(params["connect.decimal.precision"])
        elif "precision" in params:
            precision = int(params["precision"])
        else:
            precision = 38
        return precision, scale
