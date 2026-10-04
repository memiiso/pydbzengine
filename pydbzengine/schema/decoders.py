from __future__ import annotations

import base64
import binascii
import decimal
import json
import re
import struct
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

from pydbzengine.schema.models import (
    CanonicalField,
    CanonicalPrimitiveType,
    CanonicalSchema,
    CanonicalType,
)

__all__ = [
    "DEFAULT_VALUE_DECODER",
    "ComplexDecoder",
    "ConnectValueDecoder",
    "NumericDecoder",
    "TemporalDecoder",
    "ValueDecoder",
]

_EPOCH_DATETIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
_EPOCH_DATE = date(1970, 1, 1)
_NANOS_REGEX = re.compile(r"^(\d+)(.*)$")


def _truncate_fraction_to_micros(s: str) -> str:
    """Truncates sub-microsecond fractional digits (>6) in an ISO date/time string."""
    if "." not in s:
        return s
    prefix, remainder = s.split(".", 1)
    match = _NANOS_REGEX.match(remainder)
    if not match:
        return s
    digits, tail = match.group(1), match.group(2)
    return f"{prefix}.{digits[:6]}{tail}"


class NumericDecoder:
    """Decodes integer, floating-point, and exact decimal values from CDC wire formats."""

    @staticmethod
    def scale_int_to_decimal(int_val: int, scale: int) -> Decimal:
        """Converts an integer to a scaled Decimal without division precision limits."""
        if scale == 0:
            return Decimal(int_val)
        sign = 0 if int_val >= 0 else 1
        digits = tuple(int(d) for d in str(abs(int_val)))
        return Decimal((sign, digits, -scale))

    def decode_decimal(self, raw: Any, scale: int = 0) -> Decimal:
        """Decodes big-endian byte buffers, base64 strings, or numeric inputs into a scaled Decimal."""
        if scale < 0:
            raise ValueError(f"Decimal scale must be non-negative, got {scale}")
        if raw is None:
            raise ValueError("Cannot decode None as Decimal")

        if isinstance(raw, Decimal):
            return raw

        if hasattr(raw, "toPlainString") and callable(raw.toPlainString):
            return Decimal(str(raw.toPlainString()))

        if hasattr(raw, "scale") and hasattr(raw, "unscaledValue"):
            return Decimal(str(raw.toString()))

        if isinstance(raw, (int, float)):
            return Decimal(str(raw))

        if isinstance(raw, (bytes, bytearray, memoryview)):
            int_val = int.from_bytes(bytes(raw), byteorder="big", signed=True)
            return self.scale_int_to_decimal(int_val, scale)

        if hasattr(raw, "array") and callable(raw.array):
            int_val = int.from_bytes(bytes(raw.array()), byteorder="big", signed=True)
            return self.scale_int_to_decimal(int_val, scale)

        raw_str = str(raw).strip()
        if not raw_str:
            raise ValueError("Cannot decode empty string as Decimal")

        if "." in raw_str or raw_str.startswith(("-", "+")):
            try:
                return Decimal(raw_str)
            except decimal.InvalidOperation:
                pass

        if raw_str.isdigit() and scale == 0:
            return Decimal(raw_str)

        try:
            raw_bytes = base64.b64decode(raw_str, validate=True)
            int_val = int.from_bytes(raw_bytes, byteorder="big", signed=True)
            return self.scale_int_to_decimal(int_val, scale)
        except (binascii.Error, ValueError):
            try:
                return Decimal(raw_str)
            except decimal.InvalidOperation as err:
                raise ValueError(f"Malformed decimal string: {raw_str!r}") from err

    def decode_variable_scale_decimal(self, val: Any) -> Decimal:
        """Decodes an io.debezium.data.VariableScaleDecimal struct {"scale": int, "value": b64/bytes}."""
        scale_val = None
        raw_val = None
        if isinstance(val, Mapping):
            scale_val = val.get("scale")
            raw_val = val.get("value")
        elif hasattr(val, "get"):
            try:
                scale_val = val.get("scale")
                raw_val = val.get("value")
            except (AttributeError, TypeError):
                pass

        if raw_val is not None:
            scale = int(scale_val) if scale_val is not None else 0
            return self.decode_decimal(raw_val, scale=scale)

        if isinstance(val, Mapping) or hasattr(val, "schema"):
            raise ValueError("VariableScaleDecimal missing 'value' field")

        try:
            return Decimal(str(val))
        except decimal.InvalidOperation as e:
            raise ValueError(
                f"Cannot decode value of type {type(val)} as VariableScaleDecimal: {val!r}"
            ) from e

    def decode_integer(self, value: Any) -> int:
        """Decodes integer primitives from numeric, timedelta, or binary representations."""
        if isinstance(value, (bytes, bytearray, memoryview)):
            return int.from_bytes(bytes(value), byteorder="big", signed=True)
        if isinstance(value, timedelta):
            return int(value.total_seconds() * 1_000_000)
        try:
            return int(value)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Cannot decode {value!r} as integer: {e}") from e

    def decode_float(self, value: Any, double_precision: bool = False) -> float:
        """Decodes single or double precision floating point values from buffers or numbers."""
        if isinstance(value, (bytes, bytearray, memoryview)):
            b = bytes(value)
            if len(b) == 4:
                return float(struct.unpack(">f", b)[0])
            if len(b) == 8:
                return float(struct.unpack(">d", b)[0])
        try:
            return float(value)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Cannot decode {value!r} as float: {e}") from e


class TemporalDecoder:
    """Decodes ISO strings, epoch offsets, and Java date/time objects into Python temporal types."""

    def __init__(self, tz_aware: bool = True) -> None:
        self.tz_aware = tz_aware

    def decode_date(self, val: Any) -> date:
        """Decodes epoch days, timestamps, Java date types, or ISO date strings into a datetime.date."""
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val
        if hasattr(val, "toEpochDay"):
            return _EPOCH_DATE + timedelta(days=int(val.toEpochDay()))
        if hasattr(val, "getTime"):
            return (_EPOCH_DATETIME + timedelta(milliseconds=int(val.getTime()))).date()
        if isinstance(val, (int, float)):
            int_val = int(val)
            if abs(int_val) > 10_000_000:
                return self.decode_timestamp(int_val).date()
            return _EPOCH_DATE + timedelta(days=int_val)
        if isinstance(val, str):
            s = val.strip()
            if len(s) == 11 and s.endswith(("Z", "z")):
                s = s[:-1]
            try:
                return date.fromisoformat(s)
            except ValueError:
                try:
                    s_clean = _truncate_fraction_to_micros(s)
                    return datetime.fromisoformat(
                        s_clean.replace("Z", "+00:00").replace("z", "+00:00")
                    ).date()
                except ValueError as e:
                    raise ValueError(f"Invalid date string format: {val!r}") from e
        raise ValueError(f"Cannot decode value of type {type(val)} into date: {val!r}")

    def decode_time(self, val: Any, precision: int | None = None) -> time:
        """Decodes millisecond/microsecond/nanosecond offsets, timedeltas, or ISO strings into a datetime.time."""
        if isinstance(val, datetime):
            return val.time()
        if isinstance(val, time):
            return val
        if isinstance(val, timedelta):
            total_seconds = int(val.total_seconds())
            us = val.microseconds
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return time(hours % 24, minutes, seconds, us)
        if hasattr(val, "toNanoOfDay"):
            us = int(val.toNanoOfDay()) // 1_000
            total_seconds, microsecond = divmod(us, 1_000_000)
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return time(hours % 24, minutes, seconds, microsecond)
        if hasattr(val, "getTime"):
            ms = int(val.getTime())
            us = ms * 1_000
            total_seconds, microsecond = divmod(us, 1_000_000)
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return time(hours % 24, minutes, seconds, microsecond)
        if isinstance(val, (int, float)):
            int_val = int(val)
            effective_precision = precision if precision is not None else 3
            if effective_precision == 3:
                us = int_val * 1_000
            elif effective_precision == 6:
                us = int_val
            elif effective_precision == 9:
                us = int_val // 1_000
            else:
                raise ValueError(
                    f"Unsupported time precision {effective_precision}. Must be 3 (ms), 6 (us), or 9 (ns)."
                )

            total_seconds, microsecond = divmod(us, 1_000_000)
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return time(hours % 24, minutes, seconds, microsecond)

        if isinstance(val, str):
            s = val.strip()
            if s.endswith(("Z", "z")):
                s = s[:-1] + "+00:00"
            s = _truncate_fraction_to_micros(s)
            try:
                return time.fromisoformat(s)
            except ValueError as e:
                raise ValueError(f"Invalid time string format: {val!r}") from e

        raise ValueError(f"Cannot decode value of type {type(val)} into time: {val!r}")

    def decode_timestamp(self, val: Any, precision: int | None = None) -> datetime:
        """Decodes epoch offsets, Java timestamps, dates, or ISO strings into a UTC datetime.datetime."""
        if isinstance(val, datetime):
            if self.tz_aware and val.tzinfo is None:
                return val.replace(tzinfo=timezone.utc)
            if not self.tz_aware and val.tzinfo is not None:
                return val.astimezone(timezone.utc).replace(tzinfo=None)
            return val

        if isinstance(val, date):
            dt = datetime.combine(val, time.min)
            return dt.replace(tzinfo=timezone.utc) if self.tz_aware else dt

        if hasattr(val, "getEpochSecond") and hasattr(val, "getNano"):
            dt = _EPOCH_DATETIME + timedelta(
                seconds=int(val.getEpochSecond()),
                microseconds=int(val.getNano()) // 1000,
            )
            return dt if self.tz_aware else dt.replace(tzinfo=None)

        if hasattr(val, "getTime"):
            nanos = int(val.getNanos()) if hasattr(val, "getNanos") else 0
            dt = _EPOCH_DATETIME + timedelta(
                milliseconds=int(val.getTime()),
                microseconds=(nanos % 1_000_000) // 1000,
            )
            return dt if self.tz_aware else dt.replace(tzinfo=None)

        if isinstance(val, (int, float)):
            int_val = int(val)
            effective_precision = precision if precision is not None else 3
            if effective_precision == 3:
                us = int_val * 1_000
            elif effective_precision == 6:
                us = int_val
            elif effective_precision == 9:
                us = int_val // 1_000
            else:
                raise ValueError(
                    f"Unsupported timestamp precision {effective_precision}. Must be 3 (ms), 6 (us), or 9 (ns)."
                )

            dt = _EPOCH_DATETIME + timedelta(microseconds=us)
            return dt if self.tz_aware else dt.replace(tzinfo=None)

        if isinstance(val, str):
            s = val.strip().replace("Z", "+00:00").replace("z", "+00:00")
            s = _truncate_fraction_to_micros(s)
            try:
                dt = datetime.fromisoformat(s)
                if self.tz_aware and dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                if not self.tz_aware and dt.tzinfo is not None:
                    return dt.astimezone(timezone.utc).replace(tzinfo=None)
                return dt
            except ValueError as e:
                raise ValueError(f"Invalid timestamp ISO string format: {val!r}") from e

        raise ValueError(
            f"Cannot decode value of type {type(val)} into timestamp: {val!r}"
        )


class ComplexDecoder:
    """Decodes composite types: STRUCT, LIST, and MAP, with support for Java JPype collections."""

    def decode_struct(
        self,
        value: Any,
        field_type: CanonicalType,
        field_decoder: ConnectValueDecoder,
    ) -> Any:
        if not field_type.fields:
            return value
        struct_map = {f.name: f for f in field_type.fields}
        if isinstance(value, Mapping):
            return {
                k: field_decoder.decode_field(v, struct_map[k].field_type)
                if k in struct_map
                else v
                for k, v in value.items()
            }
        if hasattr(value, "schema") and hasattr(value, "get"):
            result: dict[str, Any] = {}
            for fname, fdef in struct_map.items():
                fval = value.get(fname)
                result[fname] = field_decoder.decode_field(fval, fdef.field_type)
            return result
        return value

    def decode_list(
        self,
        value: Any,
        field_type: CanonicalType,
        field_decoder: ConnectValueDecoder,
    ) -> Any:
        if not field_type.element_type:
            return value
        elem_type = field_type.element_type
        if isinstance(value, str):
            s = value.strip()
            if s.startswith("[") and s.endswith("]"):
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, list):
                        return [field_decoder.decode_field(elem, elem_type) for elem in parsed]
                except json.JSONDecodeError as e:
                    raise ValueError(f"Malformed JSON array string for LIST field: {value!r}") from e
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview, Mapping)):
            return [field_decoder.decode_field(elem, elem_type) for elem in value]
        if hasattr(value, "toArray") and callable(value.toArray):
            return [field_decoder.decode_field(elem, elem_type) for elem in value.toArray()]
        if hasattr(value, "iterator"):
            return [field_decoder.decode_field(elem, elem_type) for elem in value]
        return value

    def decode_map(
        self,
        value: Any,
        field_type: CanonicalType,
        field_decoder: ConnectValueDecoder,
    ) -> Any:
        k_type = field_type.key_type
        v_type = field_type.value_type
        if isinstance(value, str):
            s = value.strip()
            if s.startswith("{") and s.endswith("}"):
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, dict):
                        return {
                            (field_decoder.decode_field(k, k_type) if k_type else k): (
                                field_decoder.decode_field(v, v_type) if v_type else v
                            )
                            for k, v in parsed.items()
                        }
                except json.JSONDecodeError as e:
                    raise ValueError(f"Malformed JSON object string for MAP field: {value!r}") from e
        if hasattr(value, "entrySet") and callable(value.entrySet):
            res_map = {}
            for entry in value.entrySet():
                k = entry.getKey() if hasattr(entry, "getKey") else None
                v = entry.getValue() if hasattr(entry, "getValue") else None
                res_map[field_decoder.decode_field(k, k_type) if k_type else k] = (
                    field_decoder.decode_field(v, v_type) if v_type else v
                )
            return res_map
        if isinstance(value, Mapping) or hasattr(value, "items"):
            items = value.items() if hasattr(value, "items") else []
            return {
                (field_decoder.decode_field(k, k_type) if k_type else k): (
                    field_decoder.decode_field(v, v_type) if v_type else v
                )
                for k, v in items
            }
        return value


class ConnectValueDecoder:
    """
    Decodes Kafka Connect and Debezium wire-format types into Python standard library primitives.

    Operates strictly and deterministically based on CanonicalType and CanonicalSchema definitions.
    Composes NumericDecoder, TemporalDecoder, and ComplexDecoder.
    """

    def __init__(self, tz_aware: bool = True) -> None:
        self.tz_aware = tz_aware
        self._numeric = NumericDecoder()
        self._temporal = TemporalDecoder(tz_aware=tz_aware)
        self._complex = ComplexDecoder()

    def decode_field(self, value: Any, field_type: CanonicalType) -> Any:
        """Decodes a single wire value into its target Python standard library primitive."""
        if value is None:
            return None

        match field_type.primitive:
            case CanonicalPrimitiveType.BOOLEAN:
                if isinstance(value, str):
                    s = value.strip().lower()
                    if s in ("true", "t", "1", "yes", "y"):
                        return True
                    if s in ("false", "f", "0", "no", "n"):
                        return False
                return bool(value)

            case (
                CanonicalPrimitiveType.INT8
                | CanonicalPrimitiveType.INT16
                | CanonicalPrimitiveType.INT32
                | CanonicalPrimitiveType.INT64
            ):
                return self._numeric.decode_integer(value)

            case CanonicalPrimitiveType.FLOAT:
                return self._numeric.decode_float(value, double_precision=False)

            case CanonicalPrimitiveType.DOUBLE:
                return self._numeric.decode_float(value, double_precision=True)

            case CanonicalPrimitiveType.DECIMAL:
                if isinstance(value, Mapping):
                    if "scale" in value and "value" in value:
                        return self._numeric.decode_variable_scale_decimal(value)
                elif hasattr(value, "get"):
                    try:
                        if value.get("scale") is not None and value.get("value") is not None:
                            return self._numeric.decode_variable_scale_decimal(value)
                    except (AttributeError, TypeError):
                        pass

                scale = field_type.scale if field_type.scale is not None else 0
                return self._numeric.decode_decimal(value, scale=scale)

            case CanonicalPrimitiveType.STRING:
                if isinstance(value, (bytes, bytearray, memoryview)):
                    return bytes(value).decode("utf-8", errors="replace")
                if hasattr(value, "array") and callable(value.array):
                    return bytes(value.array()).decode("utf-8", errors="replace")
                if isinstance(value, (dict, list)):
                    return json.dumps(value)
                return str(value)

            case CanonicalPrimitiveType.BINARY:
                return self.decode_binary(value)

            case CanonicalPrimitiveType.UUID:
                return self.decode_uuid(value)

            case CanonicalPrimitiveType.DATE:
                return self._temporal.decode_date(value)

            case CanonicalPrimitiveType.TIME:
                return self._temporal.decode_time(value, precision=field_type.precision)

            case CanonicalPrimitiveType.TIMESTAMP | CanonicalPrimitiveType.TIMESTAMPTZ:
                return self._temporal.decode_timestamp(value, precision=field_type.precision)

            case CanonicalPrimitiveType.STRUCT:
                return self._complex.decode_struct(value, field_type, self)

            case CanonicalPrimitiveType.LIST:
                return self._complex.decode_list(value, field_type, self)

            case CanonicalPrimitiveType.MAP:
                return self._complex.decode_map(value, field_type, self)

            case _:
                return value

    def decode_row(
        self, row: Any, schema: CanonicalSchema | None
    ) -> dict[str, Any] | None:
        """Decodes all fields in a row dictionary or Java Struct matching the CanonicalSchema."""
        if row is None:
            return None

        if schema is None or not schema.fields:
            if isinstance(row, Mapping):
                return dict(row)
            return row

        fields_by_name = {f.name: f for f in schema.fields}
        result: dict[str, Any] = {}

        if isinstance(row, Mapping):
            for k, v in row.items():
                if k in fields_by_name:
                    field_def = fields_by_name[k]
                    result[k] = self.decode_field(v, field_def.field_type)
                else:
                    result[k] = v
            return result

        if hasattr(row, "schema") and hasattr(row, "get"):
            for fname, field_def in fields_by_name.items():
                fval = row.get(fname)
                result[fname] = self.decode_field(fval, field_def.field_type)
            return result

        raise TypeError(
            f"Cannot decode row of type {type(row).__name__} against schema '{schema.identifier}'. Expected Mapping or Struct."
        )

    # --- Delegated Helpers (Public API backward-compatibility) ---

    def decode_decimal(self, raw: Any, scale: int = 0) -> Decimal:
        return self._numeric.decode_decimal(raw, scale=scale)

    def decode_variable_scale_decimal(self, val: Any) -> Decimal:
        return self._numeric.decode_variable_scale_decimal(val)

    def decode_date(self, val: Any) -> date:
        return self._temporal.decode_date(val)

    def decode_time(self, val: Any, precision: int | None = None) -> time:
        return self._temporal.decode_time(val, precision=precision)

    def decode_timestamp(self, val: Any, precision: int | None = None) -> datetime:
        return self._temporal.decode_timestamp(val, precision=precision)

    def decode_uuid(self, val: Any) -> uuid.UUID:
        if isinstance(val, uuid.UUID):
            return val
        if isinstance(val, (bytes, bytearray, memoryview)):
            b = bytes(val)
            if len(b) == 16:
                return uuid.UUID(bytes=b)
            try:
                return uuid.UUID(b.decode("utf-8").strip())
            except (ValueError, UnicodeDecodeError) as e:
                raise ValueError(f"Invalid UUID bytes representation: {val!r}") from e
        try:
            return uuid.UUID(str(val).strip())
        except (ValueError, TypeError, AttributeError) as e:
            raise ValueError(f"Invalid UUID string: {val!r}") from e

    def decode_binary(self, val: Any) -> bytes:
        if isinstance(val, (bytes, bytearray, memoryview)):
            return bytes(val)
        if hasattr(val, "array") and callable(val.array):
            return bytes(val.array())
        if isinstance(val, (list, tuple)):
            return bytes(val)
        if isinstance(val, str):
            s = val.strip()
            if s.startswith(("0x", "0X")):
                try:
                    return bytes.fromhex(s[2:])
                except ValueError:
                    pass
            try:
                return base64.b64decode(s, validate=True)
            except (binascii.Error, ValueError) as err:
                raise ValueError(f"Invalid base64 string for binary field: {val!r}") from err
        raise ValueError(f"Cannot decode value of type {type(val)} into bytes: {val!r}")


# Type alias and singleton for backward compatibility
ValueDecoder = ConnectValueDecoder
DEFAULT_VALUE_DECODER = ConnectValueDecoder()
