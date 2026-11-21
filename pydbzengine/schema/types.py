from __future__ import annotations

import decimal
from collections.abc import Mapping
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Literal

import pyarrow as pa

__all__ = [
    "DATE32_MAX",
    "DATE32_MIN",
    "INT64_MAX",
    "INT64_MIN",
    "PRIMITIVE_ARROW_TYPES",
    "TEMPORAL_LOGICAL_TYPES",
    "TIME_MAX_UNITS",
    "TimeUnit",
    "build_arrow_field",
    "build_map_arrow_type",
    "bytes_to_decimal",
    "check_date_days",
    "check_int_bounds",
    "check_time_offset",
    "check_timestamp_epoch",
    "int_bounds",
    "parse_iso_date",
    "parse_iso_time",
    "parse_iso_timestamp",
    "quantize_decimal",
    "resolve_decimal_params",
    "resolve_scalar_arrow_type",
    "scale_ms_to_unit",
    "validate_decimal_bounds",
    "validate_variable_scale",
]

TimeUnit = Literal["ms", "us", "ns"]

DATE32_MIN: int = -(1 << 31)
DATE32_MAX: int = (1 << 31) - 1
INT64_MIN: int = -(1 << 63)
INT64_MAX: int = (1 << 63) - 1

_DECIMAL128_CTX: decimal.Context = decimal.Context(
    prec=38, rounding=decimal.ROUND_HALF_UP
)
_EPOCH_UTC: datetime = datetime(1970, 1, 1, tzinfo=timezone.utc)

TIME_MAX_UNITS: dict[TimeUnit, int] = {
    "ms": 86_400_000,
    "us": 86_400_000_000,
    "ns": 86_400_000_000_000,
}
_MS_SCALE_FACTORS: dict[TimeUnit, int] = {"ms": 1, "us": 1_000, "ns": 1_000_000}

PRIMITIVE_ARROW_TYPES: dict[str, pa.DataType] = {
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

# Semantic Debezium & Kafka Connect temporal logical types:
# maps logical_name -> (required Connect wire type, target Arrow DataType).
TEMPORAL_LOGICAL_TYPES: dict[str, tuple[str, pa.DataType]] = {
    # Dates
    "io.debezium.time.Date": ("int32", pa.date32()),
    "org.apache.kafka.connect.data.Date": ("int32", pa.date32()),
    "io.debezium.time.IsoDate": ("string", pa.date32()),
    # Times
    "io.debezium.time.Time": ("int32", pa.time32("ms")),
    "org.apache.kafka.connect.data.Time": ("int32", pa.time32("ms")),
    "io.debezium.time.MicroTime": ("int64", pa.time64("us")),
    "io.debezium.time.IsoTime": ("string", pa.time64("us")),
    "io.debezium.time.ZonedTime": ("string", pa.time64("us")),
    "io.debezium.time.NanoTime": ("int64", pa.time64("ns")),
    # Timestamps
    "io.debezium.time.Timestamp": ("int64", pa.timestamp("ms")),
    "org.apache.kafka.connect.data.Timestamp": ("int64", pa.timestamp("ms")),
    "io.debezium.time.MicroTimestamp": ("int64", pa.timestamp("us")),
    "io.debezium.time.IsoTimestamp": ("string", pa.timestamp("us")),
    "io.debezium.time.NanoTimestamp": ("int64", pa.timestamp("ns")),
    "io.debezium.time.ZonedTimestamp": ("string", pa.timestamp("us", tz="UTC")),
}


def validate_variable_scale(scale: int) -> int:
    if isinstance(scale, bool) or not isinstance(scale, int) or not (0 <= scale <= 38):
        raise ValueError(
            f"default_variable_scale must be between 0 and 38, got {scale!r}"
        )
    return scale


def validate_decimal_bounds(precision: int, scale: int) -> None:
    if (
        isinstance(precision, bool)
        or not isinstance(precision, int)
        or not (1 <= precision <= 38)
    ):
        raise ValueError(
            f"Decimal precision must be between 1 and 38, got {precision!r}"
        )
    if (
        isinstance(scale, bool)
        or not isinstance(scale, int)
        or not (0 <= scale <= precision)
    ):
        raise ValueError(
            f"Decimal scale ({scale!r}) must be between 0 and precision ({precision})"
        )


def resolve_decimal_params(
    params: Mapping[str, str],
    *,
    require_scale: bool,
    default_variable_scale: int,
) -> tuple[int, int]:
    if not require_scale:
        validate_variable_scale(default_variable_scale)

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
        scale = min(default_variable_scale, precision)

    validate_decimal_bounds(precision, scale)
    return precision, scale


def _require_wire_type(
    field_name: str,
    logical_name: str,
    actual_type: str,
    expected_type: str,
) -> None:
    if actual_type != expected_type:
        raise ValueError(
            f"Expected wire type '{expected_type}' for logical type "
            f"'{logical_name}' in field '{field_name}', got '{actual_type}'"
        )


def resolve_scalar_arrow_type(
    field_name: str,
    type_name: str,
    logical_name: str | None,
    parameters: Mapping[str, str],
    *,
    default_variable_scale: int = 10,
) -> pa.DataType:
    """Resolves a non-container Connect/Debezium field specification to its PyArrow DataType."""
    if logical_name is not None:
        if logical_name in TEMPORAL_LOGICAL_TYPES:
            expected_type, arrow_type = TEMPORAL_LOGICAL_TYPES[logical_name]
            _require_wire_type(field_name, logical_name, type_name, expected_type)
            return arrow_type
        if logical_name == "io.debezium.data.VariableScaleDecimal":
            _require_wire_type(field_name, logical_name, type_name, "struct")
            precision, scale = resolve_decimal_params(
                parameters,
                require_scale=False,
                default_variable_scale=default_variable_scale,
            )
            return pa.decimal128(precision, scale)
        if logical_name == "org.apache.kafka.connect.data.Decimal":
            _require_wire_type(field_name, logical_name, type_name, "bytes")
            precision, scale = resolve_decimal_params(
                parameters,
                require_scale=True,
                default_variable_scale=default_variable_scale,
            )
            return pa.decimal128(precision, scale)

    if type_name in PRIMITIVE_ARROW_TYPES:
        return PRIMITIVE_ARROW_TYPES[type_name]

    raise ValueError(
        f"Unsupported Debezium Connect type: '{type_name}' (logical name: '{logical_name}')"
    )


def build_arrow_field(
    name: str,
    arrow_type: pa.DataType,
    *,
    nullable: bool = True,
    doc: str | None = None,
    logical_name: str | None = None,
) -> pa.Field:
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"Field name must be a non-empty string, got {name!r}")
    if not isinstance(nullable, bool):
        raise ValueError(
            f"Expected bool for 'nullable' in field '{name}', got {type(nullable).__name__}"
        )
    metadata: dict[bytes, bytes] = {}
    if doc:
        metadata[b"doc"] = doc.encode("utf-8")
    if logical_name:
        metadata[b"debezium.logical_type"] = logical_name.encode("utf-8")
    return pa.field(
        name.strip(),
        arrow_type,
        nullable=nullable,
        metadata=metadata or None,
    )


def build_map_arrow_type(key_field: pa.Field, value_field: pa.Field) -> pa.MapType:
    if pa.types.is_nested(key_field.type):
        raise ValueError(
            f"Invalid key type for map field: {key_field.type}. "
            "Map keys cannot be struct, array, or map."
        )
    return pa.map_(key_field.with_nullable(False), value_field)


def int_bounds(arrow_type: pa.DataType) -> tuple[int, int]:
    if not pa.types.is_integer(arrow_type):
        raise ValueError(f"Expected Arrow integer type, got {arrow_type}")
    bits = arrow_type.bit_width
    if pa.types.is_signed_integer(arrow_type):
        return -(1 << (bits - 1)), (1 << (bits - 1)) - 1
    return 0, (1 << bits) - 1


def check_int_bounds(
    val: int, min_val: int, max_val: int, arrow_type: pa.DataType
) -> int:
    if val < min_val or val > max_val:
        raise ValueError(
            f"Integer value {val} out of bounds [{min_val}, {max_val}] for {arrow_type}"
        )
    return val


def check_date_days(days: int) -> int:
    if days < DATE32_MIN or days > DATE32_MAX:
        raise ValueError(
            f"Date epoch-day value {days} out of bounds [{DATE32_MIN}, {DATE32_MAX}] for date32"
        )
    return days


def check_time_offset(offset: int, unit: TimeUnit) -> int:
    limit = TIME_MAX_UNITS[unit]
    if offset < 0 or offset >= limit:
        raise ValueError(
            f"Time value {offset} is out of [0, {limit}) bounds for unit '{unit}'"
        )
    return offset


def check_timestamp_epoch(val: int, unit: TimeUnit) -> int:
    if val < INT64_MIN or val > INT64_MAX:
        raise ValueError(
            f"Timestamp value {val} out of int64 bounds [{INT64_MIN}, {INT64_MAX}] for unit '{unit}'"
        )
    return val


def scale_ms_to_unit(ms: int, unit: TimeUnit) -> int:
    return ms * _MS_SCALE_FACTORS[unit]


def bytes_to_decimal(raw_bytes: bytes, scale: int) -> Decimal:
    if not raw_bytes:
        raise ValueError("Cannot decode empty bytes as Decimal")
    int_val = int.from_bytes(raw_bytes, byteorder="big", signed=True)
    if int_val == 0:
        return Decimal((0, (0,), -scale))
    tup = Decimal(int_val).as_tuple()
    return Decimal((tup.sign, tup.digits, -scale))


def quantize_decimal(dec: Decimal, scale: int, precision: int = 38) -> Decimal:
    tup = dec.as_tuple()
    if not isinstance(tup.exponent, int):
        raise ValueError(f"Cannot quantize non-finite Decimal value: {dec!r}")
    if tup.exponent != -scale:
        try:
            dec = dec.quantize(Decimal((0, (1,), -scale)), context=_DECIMAL128_CTX)
        except decimal.InvalidOperation as err:
            raise ValueError(
                f"Decimal value {dec!r} exceeds configured precision ({precision}, scale={scale})"
            ) from err
        tup = dec.as_tuple()
    if dec == 0:
        return Decimal((0, (0,), -scale))
    if len(tup.digits) > precision:
        raise ValueError(
            f"Decimal value {dec!r} exceeds configured precision ({precision}, scale={scale})"
        )
    return dec


def _split_iso_fraction(s: str) -> tuple[str, int]:
    """Normalizes fractional seconds to 6 digits for Python fromisoformat and returns sub-microsecond nanos."""
    if "." not in s:
        return s, 0
    prefix, remainder = s.split(".", 1)
    i = 0
    while i < len(remainder) and remainder[i].isdigit():
        i += 1
    if i == 0:
        return s, 0
    frac = remainder[:i]
    sub_micro_nanos = int(frac[6 : min(i, 9)].ljust(3, "0")) if i > 6 else 0
    return f"{prefix}.{frac[:6].ljust(6, '0')}{remainder[i:]}", sub_micro_nanos


def parse_iso_date(val: str) -> date:
    s = val.strip()
    if s.endswith(("Z", "z")):
        s = s[:-1]
    try:
        return date.fromisoformat(s)
    except ValueError as err:
        raise ValueError(f"Invalid date string format: {val!r}") from err


def parse_iso_time(val: str, unit: TimeUnit = "us") -> time | int:
    s = val.strip()
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    norm, sub_micro_nanos = _split_iso_fraction(s)
    try:
        t = time.fromisoformat(norm)
    except ValueError as err:
        raise ValueError(f"Invalid time string format: {val!r}") from err
    if t.tzinfo is not None:
        t = datetime.combine(date(1970, 1, 1), t).astimezone(timezone.utc).time()
    if unit == "ns":
        total_us = (
            t.hour * 3_600 + t.minute * 60 + t.second
        ) * 1_000_000 + t.microsecond
        return total_us * 1_000 + sub_micro_nanos
    return t


def parse_iso_timestamp(
    val: str,
    unit: TimeUnit = "us",
    tz: str | None = None,
) -> datetime | int:
    s = val.strip()
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    norm, sub_micro_nanos = _split_iso_fraction(s)
    try:
        dt = datetime.fromisoformat(norm)
    except ValueError as err:
        raise ValueError(f"Invalid timestamp ISO string format: {val!r}") from err
    if unit == "ns":
        dt_utc = (
            dt.replace(tzinfo=timezone.utc)
            if dt.tzinfo is None
            else dt.astimezone(timezone.utc)
        )
        delta = dt_utc - _EPOCH_UTC
        epoch_us = (
            delta.days * 86_400 + delta.seconds
        ) * 1_000_000 + delta.microseconds
        return check_timestamp_epoch(epoch_us * 1_000 + sub_micro_nanos, "ns")
    if tz is not None:
        return (
            dt.replace(tzinfo=timezone.utc)
            if dt.tzinfo is None
            else dt.astimezone(timezone.utc)
        )
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt
