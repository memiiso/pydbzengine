from __future__ import annotations

import base64
import binascii
import decimal
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

__all__ = [
    "coerce_binary",
    "coerce_date_iso",
    "coerce_time_iso",
    "coerce_timestamp_iso_naive",
    "coerce_timestamp_iso_utc",
    "decode_decimal",
    "decode_variable_scale_decimal",
    "make_decimal_coercer",
    "make_list_coercer",
    "make_map_coercer",
    "make_struct_coercer",
    "make_variable_scale_decimal_coercer",
]

_NUMERIC_LITERAL_RE = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")


def _truncate_fraction_to_micros(s: str) -> str:
    """Truncates sub-microsecond fractional digits (>6) in an ISO date/time string."""
    if "." not in s:
        return s
    prefix, remainder = s.split(".", 1)
    i = 0
    while i < len(remainder) and remainder[i].isdigit():
        i += 1
    if i == 0:
        return s
    return f"{prefix}.{remainder[: min(i, 6)]}{remainder[i:]}"


def _scale_int_to_decimal(int_val: int, scale: int) -> Decimal:
    """Converts a signed unscaled integer to a scaled Decimal without float precision loss."""
    if scale == 0:
        return Decimal(int_val)
    sign = 0 if int_val >= 0 else 1
    digits = tuple(int(d) for d in str(abs(int_val)))
    return Decimal((sign, digits, -scale))


def decode_decimal(raw: Any, scale: int = 0) -> Decimal:
    """
    Decodes Debezium / Kafka Connect Decimal wire values (Base64-encoded big-endian
    two's-complement bytes, raw byte buffers, or numeric literals) into a Python Decimal.
    """
    if scale < 0:
        raise ValueError(f"Decimal scale must be non-negative, got {scale}")
    if raw is None:
        raise ValueError("Cannot decode None as Decimal")
    if isinstance(raw, Decimal):
        return raw
    if isinstance(raw, bool):
        raise TypeError(f"Cannot decode boolean {raw!r} as Decimal")
    if isinstance(raw, int):
        return Decimal(raw)
    if isinstance(raw, float):
        return Decimal(str(raw))
    if isinstance(raw, (bytes, bytearray, memoryview)):
        int_val = int.from_bytes(bytes(raw), byteorder="big", signed=True)
        return _scale_int_to_decimal(int_val, scale)
    if not isinstance(raw, str):
        raise TypeError(f"Cannot decode {type(raw).__name__} as Decimal: {raw!r}")

    raw_str = raw.strip()
    if not raw_str:
        raise ValueError("Cannot decode empty string as Decimal")

    if _NUMERIC_LITERAL_RE.match(raw_str):
        try:
            return Decimal(raw_str)
        except decimal.InvalidOperation as err:
            raise ValueError(f"Malformed decimal string: {raw_str!r}") from err

    try:
        raw_bytes = base64.b64decode(raw_str, validate=True)
    except (binascii.Error, ValueError) as err:
        raise ValueError(f"Malformed decimal string: {raw_str!r}") from err

    int_val = int.from_bytes(raw_bytes, byteorder="big", signed=True)
    return _scale_int_to_decimal(int_val, scale)


def make_decimal_coercer(scale: int) -> Callable[[Any], Decimal]:
    """
    Creates a bound decimal coercer for `scale`, rounding values with more fractional
    digits than `scale` using ROUND_HALF_UP for Arrow decimal128 compatibility.
    """
    quantizer = Decimal((0, (1,), -scale))

    def _coerce(val: Any) -> Decimal:
        dec = decode_decimal(val, scale=scale)
        exp = dec.as_tuple().exponent
        if isinstance(exp, int) and -exp > scale:
            return dec.quantize(quantizer, rounding=decimal.ROUND_HALF_UP)
        return dec

    return _coerce


def decode_variable_scale_decimal(val: Any, default_scale: int = 10) -> Decimal | None:
    """Decodes an io.debezium.data.VariableScaleDecimal mapping {"scale": int, "value": b64/bytes}."""
    if isinstance(val, Mapping):
        if "value" not in val:
            raise ValueError(f"VariableScaleDecimal missing 'value' field: {val!r}")
        raw_val = val["value"]
        if raw_val is None:
            return None
        scale_val = val.get("scale")
        scale = int(scale_val) if scale_val is not None else default_scale
        return decode_decimal(raw_val, scale=scale)

    raise TypeError(
        f"Expected Mapping for VariableScaleDecimal, got {type(val).__name__}: {val!r}"
    )


def make_variable_scale_decimal_coercer(
    default_scale: int = 10,
) -> Callable[[Any], Decimal | None]:
    """
    Creates a bound VariableScaleDecimal coercer that quantizes values exceeding
    `default_scale` for Arrow `decimal128(38, default_scale)` compatibility.
    """
    quantizer = Decimal((0, (1,), -default_scale))

    def _coerce(val: Any) -> Decimal | None:
        dec = decode_variable_scale_decimal(val, default_scale=default_scale)
        if dec is None:
            return None
        exp = dec.as_tuple().exponent
        if isinstance(exp, int) and -exp > default_scale:
            return dec.quantize(quantizer, rounding=decimal.ROUND_HALF_UP)
        return dec

    return _coerce


def coerce_binary(val: Any) -> bytes:
    """Decodes Base64 strings, hex-encoded strings, or byte-like buffers into bytes."""
    if isinstance(val, bytes):
        return val
    if isinstance(val, (bytearray, memoryview)):
        return bytes(val)
    if isinstance(val, (list, tuple)):
        return bytes(val)
    if isinstance(val, str):
        s = val.strip()
        if s.startswith(("0x", "0X")):
            return bytes.fromhex(s[2:])
        try:
            return base64.b64decode(s, validate=True)
        except (binascii.Error, ValueError) as err:
            raise ValueError(
                f"Invalid base64 string for binary field: {val!r}"
            ) from err
    raise TypeError(f"Cannot decode {type(val).__name__} into bytes: {val!r}")


def coerce_date_iso(val: Any) -> date | int:
    """Coerces ISO date/datetime strings, date objects, or epoch-day integers into a date or int."""
    if isinstance(val, int) and not isinstance(val, bool):
        return int(val)
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date) and not isinstance(val, bool):
        return val
    if isinstance(val, str):
        s = val.strip()
        if "T" in s or "t" in s or " " in s:
            return _parse_iso_timestamp(s, tz_aware=False).date()
        if s.endswith(("Z", "z")):
            s = s[:-1]
        try:
            return date.fromisoformat(s)
        except ValueError as e:
            raise ValueError(f"Invalid date string format: {val!r}") from e
    raise TypeError(f"Cannot decode {type(val).__name__} into date: {val!r}")


def coerce_time_iso(val: Any) -> time | int:
    """Coerces an ISO time string or microsecond int into a datetime.time or us int."""
    if isinstance(val, int) and not isinstance(val, bool):
        return int(val)
    if isinstance(val, datetime):
        return val.time()
    if isinstance(val, time):
        return val
    if isinstance(val, str):
        s = val.strip()
        has_z = s.endswith(("Z", "z"))
        if has_z:
            s = s[:-1] + "+00:00"
        s = _truncate_fraction_to_micros(s)
        try:
            t = time.fromisoformat(s)
            if has_z and t.tzinfo is None:
                return t.replace(tzinfo=timezone.utc)
            return t
        except ValueError as e:
            raise ValueError(f"Invalid time string format: {val!r}") from e
    raise TypeError(f"Cannot decode {type(val).__name__} into time: {val!r}")


def _parse_iso_timestamp(val: str, tz_aware: bool) -> datetime:
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


def coerce_timestamp_iso_naive(val: Any) -> datetime | int:
    """Coerces an ISO-8601 string into a timezone-naive UTC datetime for pa.timestamp('us')."""
    if isinstance(val, int) and not isinstance(val, bool):
        return int(val)
    if isinstance(val, datetime):
        return val.astimezone(timezone.utc).replace(tzinfo=None) if val.tzinfo else val
    if isinstance(val, str):
        return _parse_iso_timestamp(val, tz_aware=False)
    raise TypeError(f"Cannot decode {type(val).__name__} into timestamp: {val!r}")


def coerce_timestamp_iso_utc(val: Any) -> datetime | int:
    """Coerces an ISO-8601 string into a UTC datetime for pa.timestamp('us', tz='UTC')."""
    if isinstance(val, int) and not isinstance(val, bool):
        return int(val)
    if isinstance(val, datetime):
        return val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val
    if isinstance(val, str):
        return _parse_iso_timestamp(val, tz_aware=True)
    raise TypeError(f"Cannot decode {type(val).__name__} into timestamp: {val!r}")


def make_struct_coercer(
    sub_coercers: tuple[tuple[str, Callable[[Any], Any]], ...],
) -> Callable[[Any], Any]:
    """Creates an in-place sparse coercer for a nested struct dictionary."""

    def _coerce_struct(val: Any) -> Any:
        if isinstance(val, dict):
            for col_name, fn in sub_coercers:
                sub_val = val.get(col_name)
                if sub_val is not None:
                    val[col_name] = fn(sub_val)
            return val
        if isinstance(val, Mapping):
            d = dict(val)
            for col_name, fn in sub_coercers:
                sub_val = d.get(col_name)
                if sub_val is not None:
                    d[col_name] = fn(sub_val)
            return d
        raise TypeError(
            f"Expected dict or Mapping for struct field, got {type(val).__name__}: {val!r}"
        )

    return _coerce_struct


def make_list_coercer(elem_coercer: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Creates a list coercer that applies `elem_coercer` to non-null elements."""

    def _coerce_list(val: Any) -> Any:
        if not isinstance(val, Sequence) or isinstance(
            val, (str, bytes, bytearray, memoryview)
        ):
            raise TypeError(
                f"Expected sequence for array field, got {type(val).__name__}: {val!r}"
            )
        return [elem_coercer(x) if x is not None else None for x in val]

    return _coerce_list


def make_map_coercer(
    key_coercer: Callable[[Any], Any] | None,
    val_coercer: Callable[[Any], Any] | None,
) -> Callable[[Any], Any]:
    """Creates a map coercer when map keys or values require wire coercion."""

    def _coerce_map(val: Any) -> Any:
        if not isinstance(val, Mapping):
            raise TypeError(
                f"Expected Mapping for map field, got {type(val).__name__}: {val!r}"
            )
        return {
            (key_coercer(k) if (key_coercer and k is not None) else k): (
                val_coercer(v) if (val_coercer and v is not None) else v
            )
            for k, v in val.items()
        }

    return _coerce_map
