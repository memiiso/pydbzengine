from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any, ClassVar, Literal

import pyarrow as pa

from pydbzengine.schema.handlers import (
    DataTypeHandler,
    IntTypeHandler,
    is_non_integer_connect_value,
)

__all__ = [
    "DateTypeHandler",
    "TimeTypeHandler",
    "TimeUnit",
    "TimestampTypeHandler",
]

TimeUnit = Literal["ms", "us", "ns"]


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
        if is_non_integer_connect_value(value):
            raise TypeError(
                f"Cannot convert {type(value).__name__} {value!r} into date"
            )
        if isinstance(value, int):
            return self._validate_days(int(value))
        if hasattr(value, "toEpochDay"):
            return self._validate_days(int(value.toEpochDay()))
        if hasattr(value, "toLocalDate"):
            return self._validate_days(int(value.toLocalDate().toEpochDay()))
        if hasattr(value, "getTime"):
            if hasattr(value, "toLocalTime") or hasattr(value, "getNanos"):
                raise TypeError(
                    f"Cannot convert Connect value {type(value).__name__} into date: {value!r}"
                )
            return self._validate_days(int(value.getTime()) // 86_400_000)
        if (
            hasattr(value, "bitLength")
            or hasattr(value, "longValue")
            or hasattr(value, "intValue")
        ):
            return self._validate_days(IntTypeHandler.decode_connect_int(value))
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
        if is_non_integer_connect_value(value):
            raise TypeError(
                f"Cannot convert {type(value).__name__} {value!r} into time"
            )
        if isinstance(value, int):
            return self._validate_offset(int(value))
        if hasattr(value, "toNanoOfDay"):
            return self._validate_offset(
                _scale_ns_to_unit(int(value.toNanoOfDay()), self.unit)
            )
        if hasattr(value, "toLocalTime"):
            nanos = int(value.toLocalTime().toNanoOfDay())
            if hasattr(value, "getOffset"):
                offset_ns = int(value.getOffset().getTotalSeconds()) * 1_000_000_000
                nanos = (nanos - offset_ns) % 86_400_000_000_000
            elif hasattr(value, "getTime"):
                # java.sql.Time.toLocalTime() drops sub-second milliseconds.
                nanos += (int(value.getTime()) % 1_000) * 1_000_000
            return self._validate_offset(_scale_ns_to_unit(nanos, self.unit))
        if hasattr(value, "getTime"):
            if hasattr(value, "toLocalDate") or hasattr(value, "getNanos"):
                raise TypeError(
                    f"Cannot convert Connect value {type(value).__name__} into time: {value!r}"
                )
            return self._validate_offset(
                _scale_ms_to_unit(int(value.getTime()), self.unit)
            )
        if (
            hasattr(value, "bitLength")
            or hasattr(value, "longValue")
            or hasattr(value, "intValue")
        ):
            return self._validate_offset(IntTypeHandler.decode_connect_int(value))
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

    _MIN_EPOCH: ClassVar[int] = -(1 << 63)
    _MAX_EPOCH: ClassVar[int] = (1 << 63) - 1

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

    def _validate_epoch(self, val: int) -> int:
        if val < self._MIN_EPOCH or val > self._MAX_EPOCH:
            raise ValueError(
                f"Timestamp value {val} out of int64 bounds "
                f"[{self._MIN_EPOCH}, {self._MAX_EPOCH}] for unit '{self.unit}'"
            )
        return val

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
            return self._validate_epoch(value)
        if isinstance(value, str):
            if self.unit == "ns":
                return self._validate_epoch(self.parse_iso_timestamp_nanos(value))
            return self.parse_iso_timestamp(value, tz_aware=self.tz is not None)
        raise TypeError(
            f"Cannot decode {type(value).__name__} into timestamp: {value!r}"
        )

    def convert_connect(self, value: Any) -> datetime | int:
        if is_non_integer_connect_value(value):
            raise TypeError(
                f"Cannot convert {type(value).__name__} {value!r} into timestamp"
            )
        if isinstance(value, int):
            return self._validate_epoch(int(value))
        if hasattr(value, "getTime") and (
            hasattr(value, "toLocalDate") or hasattr(value, "toLocalTime")
        ):
            raise TypeError(
                f"Cannot convert Connect value {type(value).__name__} into timestamp: {value!r}"
            )
        if hasattr(value, "toInstant"):
            value = value.toInstant()
        if hasattr(value, "getEpochSecond") and hasattr(value, "getNano"):
            total_ns = int(value.getEpochSecond()) * 1_000_000_000 + int(
                value.getNano()
            )
            return self._validate_epoch(_scale_ns_to_unit(total_ns, self.unit))
        if hasattr(value, "getTime"):
            ms = int(value.getTime())
            if hasattr(value, "getNanos"):
                total_ns = (ms // 1_000) * 1_000_000_000 + int(value.getNanos())
                return self._validate_epoch(_scale_ns_to_unit(total_ns, self.unit))
            return self._validate_epoch(_scale_ms_to_unit(ms, self.unit))
        if (
            hasattr(value, "bitLength")
            or hasattr(value, "longValue")
            or hasattr(value, "intValue")
        ):
            return self._validate_epoch(IntTypeHandler.decode_connect_int(value))
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
                return self._validate_epoch(self.parse_iso_timestamp_nanos(s))
            return self.parse_iso_timestamp(s, tz_aware=self.tz is not None)
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} into timestamp: {value!r}"
        )
