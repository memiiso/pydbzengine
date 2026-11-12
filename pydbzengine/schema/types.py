from __future__ import annotations

import base64
import binascii
import decimal
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal
from types import MappingProxyType
from typing import Any, ClassVar, Literal

import pyarrow as pa

__all__ = [
    "ArrayTypeHandler",
    "BinaryTypeHandler",
    "BooleanTypeHandler",
    "DataTypeHandler",
    "DateTypeHandler",
    "DecimalTypeHandler",
    "FieldSpec",
    "FloatTypeHandler",
    "IntTypeHandler",
    "MapTypeHandler",
    "SchemaField",
    "StringTypeHandler",
    "StructTypeHandler",
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

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError(
                f"FieldSpec name must be a non-empty string, got {self.name!r}"
            )
        stripped_name = self.name.strip()
        if stripped_name != self.name:
            object.__setattr__(self, "name", stripped_name)

        if not isinstance(self.type_name, str) or not self.type_name.strip():
            raise ValueError(
                f"FieldSpec type_name must be a non-empty string for field '{self.name}', got {self.type_name!r}"
            )
        norm_type = self.type_name.strip().lower()
        if norm_type != self.type_name:
            object.__setattr__(self, "type_name", norm_type)

        if not isinstance(self.optional, bool):
            raise ValueError(
                f"Expected bool for schema 'optional' in field '{self.name}', got {type(self.optional).__name__}"
            )
        if self.logical_name is not None:
            if not isinstance(self.logical_name, str):
                raise ValueError(
                    f"Expected str for schema 'name' in field '{self.name}', got {type(self.logical_name).__name__}"
                )
            norm_logical = self.logical_name.strip() or None
            if norm_logical != self.logical_name:
                object.__setattr__(self, "logical_name", norm_logical)
        if self.doc is not None and not isinstance(self.doc, str):
            raise ValueError(
                f"Expected str for schema 'doc' in field '{self.name}', got {type(self.doc).__name__}"
            )
        if not isinstance(self.parameters, Mapping):
            raise ValueError(
                f"Expected mapping for schema 'parameters' in field '{self.name}', got {type(self.parameters).__name__}"
            )
        sorted_params = {
            str(k): str(v)
            for k, v in sorted(self.parameters.items(), key=lambda kv: str(kv[0]))
        }
        object.__setattr__(self, "parameters", MappingProxyType(sorted_params))

    def __hash__(self) -> int:
        return hash(
            (
                self.name,
                self.type_name,
                self.optional,
                self.logical_name,
                self.doc,
                tuple(self.parameters.items()),
                self.fields,
                self.items,
                self.keys,
                self.values,
            )
        )

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
        raw_params = raw.get("parameters")
        raw_fields = raw.get("fields")
        if raw_fields is not None and not isinstance(raw_fields, list):
            raise ValueError(
                f"Expected list for schema 'fields' in field '{resolved_name}', got {type(raw_fields).__name__}"
            )
        sub_fields = tuple(cls.from_json(f) for f in raw_fields) if raw_fields else ()

        raw_items = raw.get("items")
        items_spec = (
            cls.from_json(raw_items, field_name="item")
            if type_str == "array" and raw_items is not None
            else None
        )
        raw_keys = raw.get("keys")
        keys_spec = (
            cls.from_json(raw_keys, field_name="key")
            if type_str == "map" and raw_keys is not None
            else None
        )
        raw_values = raw.get("values")
        values_spec = (
            cls.from_json(raw_values, field_name="value")
            if type_str == "map" and raw_values is not None
            else None
        )

        return cls(
            name=resolved_name,
            type_name=type_str,
            optional=raw.get("optional", True),
            logical_name=raw.get("name"),
            doc=raw.get("doc"),
            parameters={} if raw_params is None else raw_params,
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
        raw_type_name = j_type.name()
        type_str = (
            str(raw_type_name).strip().lower() if raw_type_name is not None else ""
        )
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
            if j_fields is not None:
                parsed_sub_fields: list[FieldSpec] = []
                for sub_f in j_fields:
                    raw_sub_name = sub_f.name() if sub_f is not None else None
                    if raw_sub_name is None or not str(raw_sub_name).strip():
                        raise ValueError(
                            f"Java Connect struct field name cannot be empty in '{field_name}'"
                        )
                    parsed_sub_fields.append(
                        cls.from_connect(sub_f.schema(), field_name=str(raw_sub_name))
                    )
                sub_fields = tuple(parsed_sub_fields)
        elif type_str == "array":
            val_schema = j_schema.valueSchema()
            if val_schema is not None:
                items_spec = cls.from_connect(val_schema, field_name="item")
        elif type_str == "map":
            key_schema = j_schema.keySchema()
            val_schema = j_schema.valueSchema()
            if key_schema is not None:
                keys_spec = cls.from_connect(key_schema, field_name="key")
            if val_schema is not None:
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

    __slots__ = ()

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


@dataclass(frozen=True, slots=True)
class SchemaField:
    """Compiled field pairing field metadata with its bound DataTypeHandler."""

    name: str
    handler: DataTypeHandler
    nullable: bool = True
    doc: str | None = None
    logical_name: str | None = None
    arrow_field: pa.Field = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("SchemaField name cannot be empty.")
        stripped = self.name.strip()
        if stripped != self.name:
            object.__setattr__(self, "name", stripped)
        metadata: dict[bytes, bytes] = {}
        if self.doc:
            metadata[b"doc"] = self.doc.encode("utf-8")
        if self.logical_name:
            metadata[b"debezium.logical_type"] = self.logical_name.encode("utf-8")
        object.__setattr__(
            self,
            "arrow_field",
            pa.field(
                self.name,
                self.handler.arrow_type,
                nullable=self.nullable,
                metadata=metadata if metadata else None,
            ),
        )

    @property
    def arrow_type(self) -> pa.DataType:
        return self.handler.arrow_type

    def convert_json(self, value: Any) -> Any:
        if value is None:
            if not self.nullable:
                raise ValueError(
                    f"Non-nullable field '{self.name}' received null value."
                )
            return None
        try:
            converted = self.handler.convert_json(value)
        except TypeError as err:
            raise TypeError(f"Field '{self.name}': {err}") from err
        except ValueError as err:
            raise ValueError(f"Field '{self.name}': {err}") from err
        if converted is None and not self.nullable:
            raise ValueError(f"Non-nullable field '{self.name}' received null value.")
        return converted

    def convert_connect(self, value: Any) -> Any:
        if value is None:
            if not self.nullable:
                raise ValueError(
                    f"Non-nullable field '{self.name}' received null value."
                )
            return None
        try:
            converted = self.handler.convert_connect(value)
        except TypeError as err:
            raise TypeError(f"Field '{self.name}': {err}") from err
        except ValueError as err:
            raise ValueError(f"Field '{self.name}': {err}") from err
        if converted is None and not self.nullable:
            raise ValueError(f"Non-nullable field '{self.name}' received null value.")
        return converted


# ============================================================================
# Family 1: Scalar Primitives & Temporal Intervals
# ============================================================================


@dataclass(frozen=True, slots=True)
class BooleanTypeHandler(DataTypeHandler):
    """Handles boolean fields."""

    _ARROW_TYPE: ClassVar[pa.DataType] = pa.bool_()

    @property
    def arrow_type(self) -> pa.DataType:
        return self._ARROW_TYPE

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
    _min: int = field(init=False, repr=False, compare=False)
    _max: int = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not pa.types.is_integer(self.arrow_type):
            raise ValueError(f"Expected integer Arrow type, got: {self.arrow_type}")
        bits = self.arrow_type.bit_width
        if pa.types.is_signed_integer(self.arrow_type):
            min_val, max_val = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
        else:
            min_val, max_val = 0, (1 << bits) - 1
        object.__setattr__(self, "_min", min_val)
        object.__setattr__(self, "_max", max_val)

    def _validate_bounds(self, val: int) -> int:
        if val < self._min or val > self._max:
            raise ValueError(
                f"Integer value {val} out of bounds [{self._min}, {self._max}] for {self.arrow_type}"
            )
        return val

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
        return self._validate_bounds(self.decode_json_int(value))

    def convert_connect(self, value: Any) -> int:
        return self._validate_bounds(self.decode_connect_int(value))


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

    _ARROW_TYPE: ClassVar[pa.DataType] = pa.string()

    @property
    def arrow_type(self) -> pa.DataType:
        return self._ARROW_TYPE

    def convert_json(self, value: Any) -> str:
        if isinstance(value, str):
            return value
        raise TypeError(f"Expected str, got {type(value).__name__}: {value!r}")

    def convert_connect(self, value: Any) -> str:
        if isinstance(value, str) or type(value).__name__ == "java.lang.String":
            return str(value)
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} to str: {value!r}"
        )


# ============================================================================
# Family 2: Binary & Bits
# ============================================================================


@dataclass(frozen=True, slots=True)
class BinaryTypeHandler(DataTypeHandler):
    """Handles binary and bit-set types (bytes, io.debezium.data.Bits)."""

    _ARROW_TYPE: ClassVar[pa.DataType] = pa.binary()

    @property
    def arrow_type(self) -> pa.DataType:
        return self._ARROW_TYPE

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
                f"Expected Base64 str for JSON binary field, got {type(value).__name__}: {value!r}"
            )
        try:
            return base64.b64decode(value.strip(), validate=True)
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

    _DECIMAL128_CTX: ClassVar[decimal.Context] = decimal.Context(
        prec=38, rounding=decimal.ROUND_HALF_UP
    )

    precision: int = 38
    scale: int = 10
    _arrow_type: pa.DataType = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self.validate_bounds(self.precision, self.scale)
        object.__setattr__(
            self, "_arrow_type", pa.decimal128(self.precision, self.scale)
        )

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
        if int_val == 0:
            return Decimal(0)
        tup = Decimal(int_val).as_tuple()
        return Decimal((tup.sign, tup.digits, -scale))

    @classmethod
    def quantize(cls, dec: Decimal, scale: int, precision: int = 38) -> Decimal:
        tup = dec.as_tuple()
        if not isinstance(tup.exponent, int):
            raise ValueError(f"Cannot quantize non-finite Decimal value: {dec!r}")
        if dec == 0:
            return Decimal((0, (0,), -scale))
        if -tup.exponent > scale:
            quantizer = Decimal((0, (1,), -scale))
            try:
                dec = dec.quantize(quantizer, context=cls._DECIMAL128_CTX)
            except decimal.InvalidOperation as err:
                raise ValueError(
                    f"Decimal value {dec!r} exceeds maximum precision ({precision})"
                ) from err
            tup = dec.as_tuple()
        if (
            dec != 0
            and isinstance(tup.exponent, int)
            and len(tup.digits) + tup.exponent + scale > precision
        ):
            raise ValueError(
                f"Decimal value {dec!r} exceeds configured precision ({precision}, scale={scale})"
            )
        return dec

    @classmethod
    def decode_json(cls, raw: Any, scale: int = 0) -> Decimal:
        """Decodes a JSON decimal wire value (RFC-4648 Base64 bytes, int, finite float, or finite Decimal)."""
        if raw is None:
            raise ValueError("Cannot decode None as Decimal")
        if isinstance(raw, bool):
            raise TypeError(f"Cannot decode boolean {raw!r} as Decimal")
        if isinstance(raw, Decimal):
            if not raw.is_finite():
                raise ValueError(f"Cannot decode non-finite Decimal value: {raw!r}")
            return raw
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

        try:
            decoded_bytes = base64.b64decode(raw_str, validate=True)
        except (binascii.Error, ValueError) as err:
            raise ValueError(f"Malformed Base64 decimal string: {raw_str!r}") from err

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
        return self._arrow_type

    def convert_json(self, value: Any) -> Decimal:
        return self.quantize(
            self.decode_json(value, scale=self.scale),
            self.scale,
            self.precision,
        )

    def convert_connect(self, value: Any) -> Decimal:
        return self.quantize(
            self.decode_connect(value, scale=self.scale),
            self.scale,
            self.precision,
        )


@dataclass(frozen=True, slots=True)
class VariableScaleDecimalHandler(DataTypeHandler):
    """Handles Debezium VariableScaleDecimal struct fields ({scale: int, value: bytes})."""

    precision: int = 38
    scale: int = 10
    _arrow_type: pa.DataType = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        DecimalTypeHandler.validate_bounds(self.precision, self.scale)
        object.__setattr__(
            self, "_arrow_type", pa.decimal128(self.precision, self.scale)
        )

    @property
    def arrow_type(self) -> pa.DataType:
        return self._arrow_type

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
            self.precision,
        )

    def convert_connect(self, value: Any) -> Decimal | None:
        reader = StructTypeHandler.connect_field_reader(value, "VariableScaleDecimal")
        raw_val = reader("value")
        if raw_val is None:
            return None
        raw_scale = reader("scale")
        if raw_scale is None:
            raise ValueError(
                "VariableScaleDecimal Connect Struct missing required 'scale' value"
            )
        val_scale = IntTypeHandler.decode_connect_int(raw_scale)
        return DecimalTypeHandler.quantize(
            DecimalTypeHandler.decode_connect(raw_val, scale=val_scale),
            self.scale,
            self.precision,
        )


# ============================================================================
# Family 4: Temporals (Dates, Times, Timestamps)
# ============================================================================


def _truncate_fraction_to_micros(s: str) -> str:
    if "." not in s:
        return s
    prefix, remainder = s.split(".", 1)
    i = 0
    while i < len(remainder) and remainder[i].isdigit():
        i += 1
    if i == 0:
        return s
    return f"{prefix}.{remainder[:i][:6].ljust(6, '0')}{remainder[i:]}"


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


def _scale_ms_to_unit(ms: int, unit: TimeUnit) -> int:
    if unit == "ms":
        return ms
    if unit == "us":
        return ms * 1_000
    if unit == "ns":
        return ms * 1_000_000
    raise ValueError(f"Unsupported time unit: {unit!r}")


def _scale_ns_to_unit(ns: int, unit: TimeUnit) -> int:
    if unit == "ms":
        return ns // 1_000_000
    if unit == "us":
        return ns // 1_000
    if unit == "ns":
        return ns
    raise ValueError(f"Unsupported time unit: {unit!r}")


@dataclass(frozen=True, slots=True)
class DateTypeHandler(DataTypeHandler):
    """Handles Debezium and Connect Date types (epoch days or ISO-8601 date strings)."""

    _ARROW_TYPE: ClassVar[pa.DataType] = pa.date32()
    _MIN_DAYS: ClassVar[int] = -(1 << 31)
    _MAX_DAYS: ClassVar[int] = (1 << 31) - 1

    @property
    def arrow_type(self) -> pa.DataType:
        return self._ARROW_TYPE

    @classmethod
    def _validate_days(cls, days: int) -> int:
        if days < cls._MIN_DAYS or days > cls._MAX_DAYS:
            raise ValueError(
                f"Date epoch-day value {days} out of bounds [{cls._MIN_DAYS}, {cls._MAX_DAYS}] for date32"
            )
        return days

    @staticmethod
    def parse_iso_date(val: str) -> date:
        s = val.strip()
        if s.endswith(("Z", "z")):
            s = s[:-1]
        try:
            return date.fromisoformat(s)
        except ValueError as e:
            raise ValueError(f"Invalid date string format: {val!r}") from e

    def convert_json(self, value: Any) -> date | int:
        if isinstance(value, int) and not isinstance(value, bool):
            return self._validate_days(value)
        if isinstance(value, str):
            return self.parse_iso_date(value)
        raise TypeError(f"Cannot decode {type(value).__name__} into date: {value!r}")

    def convert_connect(self, value: Any) -> date | int:
        if isinstance(value, (bool, float)) or hasattr(value, "booleanValue"):
            raise TypeError(
                f"Cannot convert {type(value).__name__} {value!r} into date"
            )
        if isinstance(value, int):
            return self._validate_days(value)
        if hasattr(value, "toEpochDay"):
            return self._validate_days(int(value.toEpochDay()))
        if hasattr(value, "getTime"):
            return self._validate_days(int(value.getTime()) // 86_400_000)
        if hasattr(value, "intValue"):
            return self._validate_days(int(value.intValue()))
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

    _MAX_UNITS: ClassVar[dict[TimeUnit, int]] = {
        "ms": 86_400_000,
        "us": 86_400_000_000,
        "ns": 86_400_000_000_000,
    }

    unit: TimeUnit = "us"
    _arrow_type: pa.DataType = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.unit not in ("ms", "us", "ns"):
            raise ValueError(f"Unsupported time unit: {self.unit!r}")
        dtype = pa.time32("ms") if self.unit == "ms" else pa.time64(self.unit)
        object.__setattr__(self, "_arrow_type", dtype)

    @property
    def arrow_type(self) -> pa.DataType:
        return self._arrow_type

    def _validate_offset(self, offset: int) -> int:
        limit = self._MAX_UNITS[self.unit]
        if offset < 0 or offset >= limit:
            raise ValueError(
                f"Time value {offset} is out of [0, {limit}) bounds for unit '{self.unit}'"
            )
        return offset

    @staticmethod
    def parse_iso_time(val: str) -> time:
        s = val.strip()
        if s.endswith(("Z", "z")):
            s = s[:-1] + "+00:00"
        s = _truncate_fraction_to_micros(s)
        try:
            t = time.fromisoformat(s)
        except ValueError as e:
            raise ValueError(f"Invalid time string format: {val!r}") from e
        if t.tzinfo is not None:
            return datetime.combine(date(1970, 1, 1), t).astimezone(timezone.utc).time()
        return t

    @classmethod
    def parse_iso_time_nanos(cls, val: str) -> int:
        t = cls.parse_iso_time(val)
        total_us = (
            t.hour * 3_600 + t.minute * 60 + t.second
        ) * 1_000_000 + t.microsecond
        return total_us * 1_000 + _extract_sub_micro_nanos(val)

    def convert_json(self, value: Any) -> time | int:
        if isinstance(value, int) and not isinstance(value, bool):
            return self._validate_offset(value)
        if isinstance(value, str):
            if self.unit == "ns":
                return self._validate_offset(self.parse_iso_time_nanos(value))
            return self.parse_iso_time(value)
        raise TypeError(f"Cannot decode {type(value).__name__} into time: {value!r}")

    def convert_connect(self, value: Any) -> time | int:
        if isinstance(value, (bool, float)) or hasattr(value, "booleanValue"):
            raise TypeError(
                f"Cannot convert {type(value).__name__} {value!r} into time"
            )
        if isinstance(value, int):
            return self._validate_offset(value)
        if hasattr(value, "toNanoOfDay"):
            return self._validate_offset(
                _scale_ns_to_unit(int(value.toNanoOfDay()), self.unit)
            )
        if hasattr(value, "getTime"):
            return self._validate_offset(
                _scale_ms_to_unit(int(value.getTime()), self.unit)
            )
        if hasattr(value, "longValue"):
            return self._validate_offset(int(value.longValue()))
        if hasattr(value, "intValue"):
            return self._validate_offset(int(value.intValue()))
        if isinstance(value, datetime):
            return value.time()
        if isinstance(value, time):
            return value
        if isinstance(value, str) or type(value).__name__ == "java.lang.String":
            return self.convert_json(str(value))
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} into time: {value!r}"
        )


@dataclass(frozen=True, slots=True)
class TimestampTypeHandler(DataTypeHandler):
    """Handles Debezium and Connect Timestamp types (ms, us, ns, naive or UTC-zoned)."""

    unit: TimeUnit = "us"
    tz: str | None = None
    _arrow_type: pa.DataType = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.unit not in ("ms", "us", "ns"):
            raise ValueError(f"Unsupported timestamp unit: {self.unit!r}")
        object.__setattr__(self, "_arrow_type", pa.timestamp(self.unit, tz=self.tz))

    @property
    def arrow_type(self) -> pa.DataType:
        return self._arrow_type

    @classmethod
    def parse_iso_timestamp(cls, val: str, tz_aware: bool) -> datetime:
        s = val.strip().replace("Z", "+00:00").replace("z", "+00:00")
        s = _truncate_fraction_to_micros(s)
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
        return epoch_us * 1_000 + _extract_sub_micro_nanos(val)

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
            return _scale_ns_to_unit(total_ns, self.unit)
        if hasattr(value, "getTime"):
            ms = int(value.getTime())
            if hasattr(value, "getNanos"):
                total_ns = (ms // 1_000) * 1_000_000_000 + int(value.getNanos())
                return _scale_ns_to_unit(total_ns, self.unit)
            return _scale_ms_to_unit(ms, self.unit)
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


@dataclass(frozen=True, slots=True)
class StructTypeHandler(DataTypeHandler):
    """Handles Kafka Connect struct schemas and nested record objects."""

    fields: tuple[SchemaField, ...] = ()
    _arrow_type: pa.StructType = field(init=False, repr=False, compare=False)
    arrow_schema: pa.Schema = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        pa_fields = [f.arrow_field for f in self.fields]
        object.__setattr__(self, "_arrow_type", pa.struct(pa_fields))
        object.__setattr__(self, "arrow_schema", pa.schema(pa_fields))

    @property
    def arrow_type(self) -> pa.StructType:
        return self._arrow_type

    @staticmethod
    def connect_field_reader(value: Any, context: str) -> Any:
        if not isinstance(value, Mapping) and not hasattr(value, "keySet"):
            if hasattr(value, "getWithoutDefault"):
                return value.getWithoutDefault
            if hasattr(value, "get"):
                return value.get
        raise TypeError(
            f"Expected Connect Struct for {context}, got {type(value).__name__}: {value!r}"
        )

    def convert_json(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise TypeError(
                f"Expected dict or Mapping for struct field, got {type(value).__name__}: {value!r}"
            )
        return {f.name: f.convert_json(value.get(f.name)) for f in self.fields}

    def convert_connect(self, value: Any) -> dict[str, Any]:
        reader = self.connect_field_reader(value, "struct field")
        return {f.name: f.convert_connect(reader(f.name)) for f in self.fields}


@dataclass(frozen=True, slots=True)
class ArrayTypeHandler(DataTypeHandler):
    """Handles Kafka Connect array schemas."""

    item_field: SchemaField
    _arrow_type: pa.ListType = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_arrow_type", pa.list_(self.item_field.arrow_field))

    @property
    def arrow_type(self) -> pa.ListType:
        return self._arrow_type

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


@dataclass(frozen=True, slots=True)
class MapTypeHandler(DataTypeHandler):
    """Handles Kafka Connect map and PostgreSQL Hstore schemas."""

    key_field: SchemaField
    value_field: SchemaField
    _arrow_type: pa.MapType = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if pa.types.is_nested(self.key_field.arrow_type):
            raise ValueError(
                f"Invalid key type for map field: {self.key_field.arrow_type}. "
                "Map keys cannot be struct, array, or map."
            )
        object.__setattr__(
            self,
            "_arrow_type",
            pa.map_(
                self.key_field.arrow_field.with_nullable(False),
                self.value_field.arrow_field,
            ),
        )

    @property
    def arrow_type(self) -> pa.MapType:
        return self._arrow_type

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
        converted = self.key_field.handler.convert_json(raw_key)
        if converted is None:
            raise ValueError("Map key cannot be None")
        return converted

    def _convert_connect_key(self, raw_key: Any) -> Any:
        if raw_key is None:
            raise ValueError("Map key cannot be None")
        converted = self.key_field.handler.convert_connect(raw_key)
        if converted is None:
            raise ValueError("Map key cannot be None")
        return converted

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
    INT16_HANDLER: ClassVar[IntTypeHandler] = IntTypeHandler(pa.int16())
    INT32_HANDLER: ClassVar[IntTypeHandler] = IntTypeHandler(pa.int32())
    INT64_HANDLER: ClassVar[IntTypeHandler] = IntTypeHandler(pa.int64())
    FLOAT32_HANDLER: ClassVar[FloatTypeHandler] = FloatTypeHandler(pa.float32())
    FLOAT64_HANDLER: ClassVar[FloatTypeHandler] = FloatTypeHandler(pa.float64())

    PRIMITIVE_HANDLERS: ClassVar[dict[str, DataTypeHandler]] = {
        "int8": IntTypeHandler(pa.int8()),
        "int16": INT16_HANDLER,
        "int32": INT32_HANDLER,
        "int64": INT64_HANDLER,
        "float": FLOAT32_HANDLER,
        "float32": FLOAT32_HANDLER,
        "double": FLOAT64_HANDLER,
        "float64": FLOAT64_HANDLER,
        "boolean": BOOL_HANDLER,
        "string": STRING_HANDLER,
        "bytes": BINARY_HANDLER,
    }

    HSTORE_MAP_HANDLER: ClassVar[MapTypeHandler] = MapTypeHandler(
        key_field=SchemaField("key", STRING_HANDLER, nullable=False),
        value_field=SchemaField("value", STRING_HANDLER, nullable=True),
    )

    GEOMETRY_HANDLER: ClassVar[StructTypeHandler] = StructTypeHandler(
        (
            SchemaField("wkb", BINARY_HANDLER, nullable=False),
            SchemaField("srid", INT32_HANDLER, nullable=True),
        )
    )

    POINT_HANDLER: ClassVar[StructTypeHandler] = StructTypeHandler(
        (
            SchemaField("x", FLOAT64_HANDLER, nullable=False),
            SchemaField("y", FLOAT64_HANDLER, nullable=False),
            SchemaField("wkb", BINARY_HANDLER, nullable=True),
            SchemaField("srid", INT32_HANDLER, nullable=True),
        )
    )

    SPARSE_VECTOR_HANDLER: ClassVar[StructTypeHandler] = StructTypeHandler(
        (
            SchemaField("dimensions", INT16_HANDLER, nullable=False),
            SchemaField(
                "vector",
                MapTypeHandler(
                    key_field=SchemaField("key", INT16_HANDLER, nullable=False),
                    value_field=SchemaField("value", FLOAT64_HANDLER, nullable=False),
                ),
                nullable=False,
            ),
        )
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
        "io.debezium.time.Year": INT32_HANDLER,
        "io.debezium.connector.oracle.data.IntervalYearToMonth": INT32_HANDLER,
        "io.debezium.time.IntervalYearToMonth": INT32_HANDLER,
        "io.debezium.time.MicroDuration": INT64_HANDLER,
        "io.debezium.time.NanoDuration": INT64_HANDLER,
        "io.debezium.connector.oracle.data.IntervalDayToSecond": INT64_HANDLER,
        "io.debezium.time.IntervalDayToSecond": INT64_HANDLER,
        "io.debezium.time.Interval": STRING_HANDLER,
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
        "io.debezium.data.geometry.Geometry": GEOMETRY_HANDLER,
        "io.debezium.data.geometry.Geography": GEOMETRY_HANDLER,
        "io.debezium.data.geometry.Point": POINT_HANDLER,
        # Dense & Sparse Vectors
        "io.debezium.data.SparseDoubleVector": SPARSE_VECTOR_HANDLER,
        "io.debezium.data.FloatVector": ArrayTypeHandler(
            SchemaField("item", FLOAT32_HANDLER, nullable=False)
        ),
        "io.debezium.data.DoubleVector": ArrayTypeHandler(
            SchemaField("item", FLOAT64_HANDLER, nullable=False)
        ),
    }

    LOGICAL_WIRE_TYPES: ClassVar[dict[str, str]] = {
        "io.debezium.time.Date": "int32",
        "org.apache.kafka.connect.data.Date": "int32",
        "io.debezium.time.IsoDate": "string",
        "io.debezium.time.Time": "int32",
        "org.apache.kafka.connect.data.Time": "int32",
        "io.debezium.time.MicroTime": "int64",
        "io.debezium.time.IsoTime": "string",
        "io.debezium.time.ZonedTime": "string",
        "io.debezium.time.NanoTime": "int64",
        "io.debezium.time.Timestamp": "int64",
        "org.apache.kafka.connect.data.Timestamp": "int64",
        "io.debezium.connector.mongodb.data.BsonTimestamp": "int64",
        "io.debezium.data.BsonTimestamp": "int64",
        "io.debezium.time.MicroTimestamp": "int64",
        "io.debezium.time.IsoTimestamp": "string",
        "io.debezium.time.NanoTimestamp": "int64",
        "io.debezium.time.ZonedTimestamp": "string",
        "io.debezium.time.Year": "int32",
        "io.debezium.connector.oracle.data.IntervalYearToMonth": "int32",
        "io.debezium.time.IntervalYearToMonth": "int32",
        "io.debezium.time.MicroDuration": "int64",
        "io.debezium.time.NanoDuration": "int64",
        "io.debezium.connector.oracle.data.IntervalDayToSecond": "int64",
        "io.debezium.time.IntervalDayToSecond": "int64",
        "io.debezium.time.Interval": "string",
        "io.debezium.data.Uuid": "string",
        "io.debezium.data.Json": "string",
        "io.debezium.data.Xml": "string",
        "io.debezium.data.Enum": "string",
        "io.debezium.data.EnumSet": "string",
        "io.debezium.connector.postgresql.data.Ltree": "string",
        "io.debezium.data.Ltree": "string",
        "io.debezium.connector.mongodb.data.ObjectId": "string",
        "io.debezium.data.ObjectId": "string",
        "io.debezium.data.Tsvector": "string",
        "io.debezium.data.Bits": "bytes",
        "io.debezium.data.geometry.Geometry": "struct",
        "io.debezium.data.geometry.Geography": "struct",
        "io.debezium.data.geometry.Point": "struct",
        "io.debezium.data.SparseDoubleVector": "struct",
        "io.debezium.data.FloatVector": "array",
        "io.debezium.data.DoubleVector": "array",
        "io.debezium.data.VariableScaleDecimal": "struct",
        "org.apache.kafka.connect.data.Decimal": "bytes",
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

    def compile_handler(self, spec: FieldSpec) -> DataTypeHandler:
        logical_name = spec.logical_name
        type_str = spec.type_name

        if logical_name is not None and logical_name in self.LOGICAL_WIRE_TYPES:
            expected_type = self.LOGICAL_WIRE_TYPES[logical_name]
            if type_str != expected_type:
                raise ValueError(
                    f"Expected wire type '{expected_type}' for logical type "
                    f"'{logical_name}' in field '{spec.name}', got '{type_str}'"
                )

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
