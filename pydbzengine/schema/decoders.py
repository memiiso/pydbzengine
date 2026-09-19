from __future__ import annotations

import base64
import binascii
import decimal
import json
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

from pydbzengine.logger import LoggingMixin
from pydbzengine.schema.models import (
    CanonicalPrimitiveType,
    CanonicalSchema,
    CanonicalType,
)

_EPOCH_DATETIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
_EPOCH_DATE = date(1970, 1, 1)


class ConnectValueDecoder(LoggingMixin):
    """
    Decodes Kafka Connect and Debezium wire-format types into Python standard library primitives.

    Operates strictly and deterministically based on CanonicalType and CanonicalSchema definitions.
    """

    def __init__(self, tz_aware: bool = True) -> None:
        self.tz_aware = tz_aware

    def decode_field(self, value: Any, field_type: CanonicalType) -> Any:
        """Decodes a single wire value into its target Python standard library primitive."""
        if value is None:
            return None

        match field_type.primitive:
            case CanonicalPrimitiveType.BOOLEAN:
                if isinstance(value, str):
                    s = value.strip().lower()
                    if s in ("true", "t", "1"):
                        return True
                    if s in ("false", "f", "0"):
                        return False
                return bool(value)

            case (
                CanonicalPrimitiveType.INT8
                | CanonicalPrimitiveType.INT16
                | CanonicalPrimitiveType.INT32
                | CanonicalPrimitiveType.INT64
            ):
                try:
                    return int(value)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Cannot decode {value!r} as integer: {e}") from e

            case CanonicalPrimitiveType.FLOAT | CanonicalPrimitiveType.DOUBLE:
                try:
                    return float(value)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Cannot decode {value!r} as float: {e}") from e

            case CanonicalPrimitiveType.DECIMAL:
                if (
                    (isinstance(value, Mapping) or hasattr(value, "get"))
                    and "scale" in value
                    and "value" in value
                ):
                    return self.decode_variable_scale_decimal(value)
                scale = field_type.scale if field_type.scale is not None else 0
                return self.decode_decimal(value, scale=scale)

            case CanonicalPrimitiveType.STRING:
                if isinstance(value, (dict, list)):
                    return json.dumps(value)
                return str(value)

            case CanonicalPrimitiveType.BINARY:
                return self.decode_binary(value)

            case CanonicalPrimitiveType.UUID:
                return self.decode_uuid(value)

            case CanonicalPrimitiveType.DATE:
                return self.decode_date(value)

            case CanonicalPrimitiveType.TIME:
                return self.decode_time(value, precision=field_type.precision)

            case CanonicalPrimitiveType.TIMESTAMP | CanonicalPrimitiveType.TIMESTAMPTZ:
                return self.decode_timestamp(value, precision=field_type.precision)

            case CanonicalPrimitiveType.STRUCT:
                if not isinstance(value, Mapping) or not field_type.fields:
                    return value
                struct_map = {f.name: f for f in field_type.fields}
                return {
                    k: self.decode_field(v, struct_map[k].field_type)
                    if k in struct_map
                    else v
                    for k, v in value.items()
                }

            case CanonicalPrimitiveType.LIST:
                if not isinstance(value, (list, tuple)) or not field_type.element_type:
                    return value
                elem_type = field_type.element_type
                return [self.decode_field(elem, elem_type) for elem in value]

            case CanonicalPrimitiveType.MAP:
                if not isinstance(value, Mapping):
                    return value
                k_type = field_type.key_type
                v_type = field_type.value_type
                return {
                    (self.decode_field(k, k_type) if k_type else k): (
                        self.decode_field(v, v_type) if v_type else v
                    )
                    for k, v in value.items()
                }

            case _:
                return value

    def decode_row(
        self, row: dict[str, Any] | None, schema: CanonicalSchema | None
    ) -> dict[str, Any] | None:
        """Decodes all fields in a row dictionary matching the CanonicalSchema."""
        if row is None:
            return None

        if not isinstance(row, Mapping):
            return row

        if schema is None or not schema.fields:
            return dict(row)

        result: dict[str, Any] = {}
        fields_by_name = {f.name: f for f in schema.fields}

        for k, v in row.items():
            if k in fields_by_name:
                field_def = fields_by_name[k]
                result[k] = self.decode_field(v, field_def.field_type)
            else:
                result[k] = v

        return result

    def decode_decimal(
        self,
        raw: str | bytes | bytearray | memoryview | int | float | Decimal,
        scale: int = 0,
    ) -> Decimal:
        """Decodes big-endian byte buffers, base64 strings, or numeric inputs into a scaled Decimal."""
        if raw is None:
            raise ValueError("Cannot decode None as Decimal")

        if isinstance(raw, Decimal):
            return raw

        if isinstance(raw, (int, float)):
            return Decimal(str(raw))

        if isinstance(raw, (bytes, bytearray, memoryview)):
            int_val = int.from_bytes(bytes(raw), byteorder="big", signed=True)
            if scale == 0:
                return Decimal(int_val)
            return Decimal(int_val) / Decimal(10**scale)

        raw_str = str(raw).strip()
        if not raw_str:
            raise ValueError("Cannot decode empty string as Decimal")

        # Formatted numbers with decimal point or signed
        if "." in raw_str or raw_str.startswith(("-", "+")):
            try:
                return Decimal(raw_str)
            except decimal.InvalidOperation:
                pass

        if raw_str.isdigit() and scale == 0:
            return Decimal(raw_str)

        # Base64-encoded big-endian integer buffer
        try:
            raw_bytes = base64.b64decode(raw_str, validate=True)
            int_val = int.from_bytes(raw_bytes, byteorder="big", signed=True)
            if scale == 0:
                return Decimal(int_val)
            return Decimal(int_val) / Decimal(10**scale)
        except (binascii.Error, ValueError):
            try:
                return Decimal(raw_str)
            except decimal.InvalidOperation as err:
                raise ValueError(f"Malformed decimal string: {raw_str!r}") from err

    def decode_variable_scale_decimal(self, val: dict[str, Any] | Any) -> Decimal:
        """Decodes an io.debezium.data.VariableScaleDecimal struct {"scale": int, "value": b64/bytes}."""
        if isinstance(val, Mapping) or hasattr(val, "get"):
            scale_val = val.get("scale") if hasattr(val, "get") else None
            raw_val = val.get("value") if hasattr(val, "get") else None
            if raw_val is not None:
                scale = int(scale_val) if scale_val is not None else 0
                return self.decode_decimal(raw_val, scale=scale)
            raise ValueError("VariableScaleDecimal missing 'value' field")

        try:
            return Decimal(str(val))
        except decimal.InvalidOperation as e:
            raise ValueError(
                f"Cannot decode value of type {type(val)} as VariableScaleDecimal: {val!r}"
            ) from e

    def decode_date(self, val: int | float | str | date | datetime) -> date:
        """Decodes epoch days or ISO date strings into a datetime.date."""
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val
        if isinstance(val, (int, float)):
            return _EPOCH_DATE + timedelta(days=int(val))
        if isinstance(val, str):
            s = val.strip()
            if len(s) == 11 and s.endswith(("Z", "z")):
                s = s[:-1]
            try:
                return date.fromisoformat(s)
            except ValueError:
                try:
                    return datetime.fromisoformat(
                        s.replace("Z", "+00:00").replace("z", "+00:00")
                    ).date()
                except ValueError as e:
                    raise ValueError(f"Invalid date string format: {val!r}") from e
        raise ValueError(f"Cannot decode value of type {type(val)} into date: {val!r}")

    def decode_time(
        self, val: int | float | str | time, precision: int | None = None
    ) -> time:
        """Decodes millisecond/microsecond/nanosecond offsets or ISO strings into a datetime.time."""
        if isinstance(val, time):
            return val
        if isinstance(val, (int, float)):
            int_val = int(val)
            if precision == 3:
                us = int_val * 1_000
            elif precision == 6:
                us = int_val
            elif precision == 9:
                us = int_val // 1_000
            else:
                abs_val = abs(int_val)
                if abs_val > 86_400_000_000:
                    us = int_val // 1_000
                elif abs_val > 86_400_000:
                    us = int_val
                else:
                    us = int_val * 1_000

            total_seconds, microsecond = divmod(us, 1_000_000)
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return time(hours % 24, minutes, seconds, microsecond)

        if isinstance(val, str):
            s = val.strip()
            if s.endswith(("Z", "z")):
                s = s[:-1] + "+00:00"
            try:
                return time.fromisoformat(s)
            except ValueError as e:
                raise ValueError(f"Invalid time string format: {val!r}") from e

        raise ValueError(f"Cannot decode value of type {type(val)} into time: {val!r}")

    def decode_timestamp(
        self, val: int | float | str | datetime, precision: int | None = None
    ) -> datetime:
        """Decodes epoch offsets or ISO strings into a UTC datetime.datetime."""
        if isinstance(val, datetime):
            if self.tz_aware and val.tzinfo is None:
                return val.replace(tzinfo=timezone.utc)
            if not self.tz_aware and val.tzinfo is not None:
                return val.astimezone(timezone.utc).replace(tzinfo=None)
            return val

        if isinstance(val, (int, float)):
            int_val = int(val)
            if precision == 3:
                us = int_val * 1_000
            elif precision == 6:
                us = int_val
            elif precision == 9:
                us = int_val // 1_000
            else:
                abs_val = abs(int_val)
                if abs_val > 10**17:
                    us = int_val // 1_000
                elif abs_val > 10**14:
                    us = int_val
                else:
                    us = int_val * 1_000

            dt = _EPOCH_DATETIME + timedelta(microseconds=us)
            return dt if self.tz_aware else dt.replace(tzinfo=None)

        if isinstance(val, str):
            s = val.strip().replace("Z", "+00:00").replace("z", "+00:00")
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

    def decode_uuid(
        self, val: str | bytes | bytearray | memoryview | uuid.UUID
    ) -> uuid.UUID:
        """Decodes string or 16-byte buffer representations into a uuid.UUID."""
        if isinstance(val, uuid.UUID):
            return val
        if isinstance(val, (bytes, bytearray, memoryview)) and len(val) == 16:
            return uuid.UUID(bytes=bytes(val))
        try:
            return uuid.UUID(str(val).strip())
        except (ValueError, TypeError, AttributeError) as e:
            raise ValueError(f"Invalid UUID string: {val!r}") from e

    def decode_binary(
        self,
        val: str
        | bytes
        | bytearray
        | memoryview
        | list[int]
        | tuple[int, ...]
        | Sequence[int],
    ) -> bytes:
        """Decodes base64 strings, raw bytes, or byte sequences into bytes."""
        if isinstance(val, (bytes, bytearray, memoryview)):
            return bytes(val)
        if isinstance(val, (list, tuple)):
            return bytes(val)
        if isinstance(val, str):
            try:
                return base64.b64decode(val.strip(), validate=True)
            except (binascii.Error, ValueError) as err:
                raise ValueError(
                    f"Invalid base64 string for binary field: {val!r}"
                ) from err
        raise ValueError(f"Cannot decode value of type {type(val)} into bytes: {val!r}")


# Type alias and singleton for backward compatibility
ValueDecoder = ConnectValueDecoder
DEFAULT_VALUE_DECODER = ConnectValueDecoder()
