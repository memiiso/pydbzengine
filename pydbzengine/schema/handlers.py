from __future__ import annotations

import base64
import binascii
import decimal
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, ClassVar

import pyarrow as pa

__all__ = [
    "ArrayTypeHandler",
    "BinaryTypeHandler",
    "BooleanTypeHandler",
    "DataTypeHandler",
    "DecimalTypeHandler",
    "FloatTypeHandler",
    "IntTypeHandler",
    "MapTypeHandler",
    "SchemaField",
    "StringTypeHandler",
    "StructTypeHandler",
    "VariableScaleDecimalHandler",
]


def is_non_integer_connect_value(value: Any) -> bool:
    """Returns True for boolean, floating-point, or decimal Python/Java values."""
    return (
        isinstance(value, (bool, float, Decimal))
        or hasattr(value, "booleanValue")
        or hasattr(value, "toPlainString")
        or hasattr(value, "isNaN")
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
    """Handles integer fields (int8, int16, int32, int64)."""

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
        if is_non_integer_connect_value(value):
            raise TypeError(f"Cannot convert {type(value).__name__} {value!r} to int")
        if isinstance(value, int):
            return int(value)
        if hasattr(value, "bitLength"):
            return int(str(value))
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
        if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
            return float(value)
        raise TypeError(f"Expected float, got {type(value).__name__}: {value!r}")

    def convert_connect(self, value: Any) -> float:
        if isinstance(value, bool) or hasattr(value, "booleanValue"):
            raise TypeError(f"Cannot convert boolean {value!r} to float")
        if isinstance(value, (int, float, Decimal)):
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
    """Handles UTF-8 strings and string-backed logical types."""

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


@dataclass(frozen=True, slots=True)
class BinaryTypeHandler(DataTypeHandler):
    """Handles binary and bit-set types (bytes, io.debezium.data.Bits)."""

    _ARROW_TYPE: ClassVar[pa.DataType] = pa.binary()

    @property
    def arrow_type(self) -> pa.DataType:
        return self._ARROW_TYPE

    @staticmethod
    def _array_to_bytes(arr: Any) -> bytes:
        if (
            isinstance(arr, (bytes, bytearray, memoryview))
            or type(arr).__name__ == "byte[]"
        ):
            return bytes(arr)
        return bytes(b & 0xFF for b in arr)

    @classmethod
    def unwrap_connect_bytes(cls, value: Any) -> bytes | None:
        if isinstance(value, bytes):
            return value
        if isinstance(value, (bytearray, memoryview)):
            return bytes(value)
        if type(value).__name__ == "byte[]":
            return bytes(value)
        if hasattr(value, "hasArray"):
            if value.hasArray():
                raw = cls._array_to_bytes(value.array())
                offset = int(value.position()) + int(value.arrayOffset())
                length = int(value.remaining())
                return raw[offset : offset + length]
            dup = value.duplicate()
            length = int(dup.remaining())
            if length == 0:
                return b""
            if hasattr(value, "allocate"):
                buf = value.allocate(length).array()
                dup.get(buf)
                return cls._array_to_bytes(buf)
            return bytes(int(dup.get()) & 0xFF for _ in range(length))
        if hasattr(value, "array"):
            raw = cls._array_to_bytes(value.array())
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
            if not raw_bytes:
                raise ValueError("Cannot decode empty bytes as Decimal")
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
    """Handles Kafka Connect map schemas."""

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
                if norm == "true":
                    raw_key = True
                elif norm == "false":
                    raw_key = False
                else:
                    raise ValueError(f"Invalid boolean map key: {raw_key!r}")
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
