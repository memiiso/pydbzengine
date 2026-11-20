from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, ClassVar, cast

import pyarrow as pa

from pydbzengine.engine.base import ChangeEvent, EventFormat
from pydbzengine.schema.converter import (
    CompiledField,
    EventConverter,
    RowDecoder,
    ValueDecoder,
    _SchemaNode,
    decode_map_value,
    decode_sequence_items,
    decode_struct_fields,
)
from pydbzengine.schema.types import (
    TimeUnit,
    bytes_to_decimal,
    check_date_days,
    check_int_bounds,
    check_time_offset,
    check_timestamp_epoch,
    int_bounds,
    parse_iso_date,
    parse_iso_time,
    parse_iso_timestamp,
    quantize_decimal,
    scale_ms_to_unit,
)

__all__ = ["ConnectEventConverter"]


def _is_java_non_integer(value: Any) -> bool:
    return (
        isinstance(value, (bool, float, Decimal))
        or hasattr(value, "booleanValue")
        or hasattr(value, "toPlainString")
        or hasattr(value, "isNaN")
    )


def _decode_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if hasattr(value, "booleanValue"):
        return bool(value.booleanValue())
    raise TypeError(
        f"Cannot convert Connect value {type(value).__name__} to bool: {value!r}"
    )


def _decode_int(value: Any) -> int:
    if type(value) is int:
        return value
    if _is_java_non_integer(value):
        raise TypeError(f"Cannot convert {type(value).__name__} {value!r} to int")
    if isinstance(value, int):
        return int(value)
    if hasattr(value, "longValue"):
        return int(value.longValue())
    if hasattr(value, "intValue"):
        return int(value.intValue())
    raise TypeError(
        f"Cannot convert Connect value {type(value).__name__} to int: {value!r}"
    )


def _decode_float(value: Any) -> float:
    if type(value) is float or type(value) is int:
        return float(value)
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


def _decode_str(value: Any) -> str:
    if type(value) is str:
        return value
    if isinstance(value, str) or type(value).__name__ == "java.lang.String":
        return str(value)
    raise TypeError(
        f"Cannot convert Connect value {type(value).__name__} to str: {value!r}"
    )


def _array_to_bytes(arr: Any) -> bytes:
    if (
        isinstance(arr, (bytes, bytearray, memoryview))
        or type(arr).__name__ == "byte[]"
    ):
        return bytes(arr)
    return bytes(b & 0xFF for b in arr)


def _unwrap_bytes(value: Any) -> bytes | None:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)) or type(value).__name__ == "byte[]":
        return bytes(value)
    if hasattr(value, "hasArray"):
        if value.hasArray():
            raw = _array_to_bytes(value.array())
            offset = int(value.position()) + int(value.arrayOffset())
            return raw[offset : offset + int(value.remaining())]
        dup = value.duplicate()
        length = int(dup.remaining())
        if length == 0:
            return b""
        if hasattr(value, "allocate"):
            buf = value.allocate(length).array()
            dup.get(buf)
            return _array_to_bytes(buf)
        return bytes(int(dup.get()) & 0xFF for _ in range(length))
    if hasattr(value, "array"):
        raw = _array_to_bytes(value.array())
        if hasattr(value, "position") and hasattr(value, "remaining"):
            offset = int(value.position())
            if hasattr(value, "arrayOffset"):
                offset += int(value.arrayOffset())
            return raw[offset : offset + int(value.remaining())]
        return raw
    return None


def _decode_bytes(value: Any) -> bytes:
    raw = _unwrap_bytes(value)
    if raw is not None:
        return raw
    raise TypeError(
        f"Cannot convert Connect value {type(value).__name__} into bytes: {value!r}"
    )


def _decode_decimal(raw: Any, scale: int) -> Decimal:
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
    raw_bytes = _unwrap_bytes(raw)
    if raw_bytes is not None:
        return bytes_to_decimal(raw_bytes, scale)
    raise TypeError(f"Cannot decode Connect {type(raw).__name__} as Decimal: {raw!r}")


def _struct_reader(value: Any, context: str) -> Callable[[str], Any]:
    if not isinstance(value, Mapping) and not hasattr(value, "keySet"):
        if hasattr(value, "getWithoutDefault"):
            return cast(Callable[[str], Any], value.getWithoutDefault)
        if hasattr(value, "get"):
            return cast(Callable[[str], Any], value.get)
    raise TypeError(
        f"Expected Connect Struct for {context}, got {type(value).__name__}: {value!r}"
    )


def _decode_date(value: Any) -> date | int:
    if type(value) is int:
        return check_date_days(value)
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if type(value) is str or type(value).__name__ == "java.lang.String":
        return parse_iso_date(str(value))
    if hasattr(value, "getTime"):
        return check_date_days(int(value.getTime()) // 86_400_000)
    if _is_java_non_integer(value):
        raise TypeError(f"Cannot convert {type(value).__name__} {value!r} into date")
    if (
        isinstance(value, int)
        or hasattr(value, "longValue")
        or hasattr(value, "intValue")
    ):
        return check_date_days(_decode_int(value))
    raise TypeError(
        f"Cannot convert Connect value {type(value).__name__} into date: {value!r}"
    )


def _make_int_decoder(arrow_type: pa.DataType) -> ValueDecoder:
    min_v, max_v = int_bounds(arrow_type)
    return lambda v: check_int_bounds(_decode_int(v), min_v, max_v, arrow_type)


def _make_decimal_decoder(precision: int, scale: int) -> ValueDecoder:
    return lambda v: quantize_decimal(_decode_decimal(v, scale), scale, precision)


def _make_variable_decimal_decoder(precision: int, scale: int) -> ValueDecoder:
    def decode(value: Any) -> Decimal | None:
        reader = _struct_reader(value, "VariableScaleDecimal")
        raw_val = reader("value")
        if raw_val is None:
            return None
        raw_scale = reader("scale")
        if raw_scale is None:
            raise ValueError(
                "VariableScaleDecimal Connect Struct missing required 'scale' value"
            )
        return quantize_decimal(
            _decode_decimal(raw_val, _decode_int(raw_scale)), scale, precision
        )

    return decode


def _make_time_decoder(unit: TimeUnit) -> ValueDecoder:
    def decode(value: Any) -> time | int:
        if type(value) is int:
            return check_time_offset(value, unit)
        if isinstance(value, datetime):
            return value.time()
        if isinstance(value, time):
            return value
        if type(value) is str or type(value).__name__ == "java.lang.String":
            return parse_iso_time(str(value), unit)
        if hasattr(value, "getTime"):
            return check_time_offset(scale_ms_to_unit(int(value.getTime()), unit), unit)
        if _is_java_non_integer(value):
            raise TypeError(
                f"Cannot convert {type(value).__name__} {value!r} into time"
            )
        if (
            isinstance(value, int)
            or hasattr(value, "longValue")
            or hasattr(value, "intValue")
        ):
            return check_time_offset(_decode_int(value), unit)
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} into time: {value!r}"
        )

    return decode


def _make_timestamp_decoder(unit: TimeUnit, tz: str | None) -> ValueDecoder:
    def decode(value: Any) -> datetime | int:
        if type(value) is int:
            return check_timestamp_epoch(value, unit)
        if isinstance(value, datetime):
            if tz is not None:
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
        if type(value) is str or type(value).__name__ == "java.lang.String":
            return parse_iso_timestamp(str(value), unit, tz)
        if hasattr(value, "getTime"):
            return check_timestamp_epoch(
                scale_ms_to_unit(int(value.getTime()), unit), unit
            )
        if _is_java_non_integer(value):
            raise TypeError(
                f"Cannot convert {type(value).__name__} {value!r} into timestamp"
            )
        if (
            isinstance(value, int)
            or hasattr(value, "longValue")
            or hasattr(value, "intValue")
        ):
            return check_timestamp_epoch(_decode_int(value), unit)
        raise TypeError(
            f"Cannot convert Connect value {type(value).__name__} into timestamp: {value!r}"
        )

    return decode


class ConnectEventConverter(EventConverter):
    """
    Dedicated converter for in-memory Java Kafka Connect SourceRecords (`EventFormat.CONNECT`).

    Reads Java Connect Schemas and decodes standard Kafka Connect runtime values
    with strong Java schema reference caching and zero JSON overhead.
    """

    format: ClassVar[EventFormat] = EventFormat.CONNECT

    def _inspect_schema(
        self, raw_schema: Any, field_name: str | None = None
    ) -> _SchemaNode:
        name = field_name if field_name is not None else "root"
        if raw_schema is None:
            raise ValueError(f"Java Connect schema cannot be None for field '{name}'")
        j_type = raw_schema.type()
        if j_type is None:
            raise ValueError(
                f"Java Connect schema type() cannot be None for field '{name}'"
            )
        raw_type_name = j_type.name()
        type_str = (
            str(raw_type_name).strip().lower() if raw_type_name is not None else ""
        )
        if not type_str:
            raise ValueError(
                f"Java Connect schema type name cannot be empty for field '{name}'"
            )

        raw_doc = raw_schema.doc()
        raw_logical = raw_schema.name()
        logical_name = str(raw_logical).strip() or None if raw_logical else None
        raw_params = raw_schema.parameters()
        params = (
            {str(k): str(v) for k, v in raw_params.items()}
            if raw_params is not None
            else {}
        )

        raw_fields: tuple[tuple[str, Any], ...] = ()
        raw_items: Any = None
        raw_keys: Any = None
        raw_values: Any = None
        if type_str == "struct":
            j_fields = raw_schema.fields()
            if j_fields is not None:
                parsed: list[tuple[str, Any]] = []
                for sub_f in j_fields:
                    sub_name = sub_f.name() if sub_f is not None else None
                    if sub_name is None or not str(sub_name).strip():
                        raise ValueError(
                            f"Java Connect struct field name cannot be empty in '{name}'"
                        )
                    parsed.append((str(sub_name).strip(), sub_f.schema()))
                raw_fields = tuple(parsed)
        elif type_str == "array":
            raw_items = raw_schema.valueSchema()
        elif type_str == "map":
            raw_keys = raw_schema.keySchema()
            raw_values = raw_schema.valueSchema()

        return _SchemaNode(
            name=name,
            type_name=type_str,
            nullable=bool(raw_schema.isOptional()),
            logical_name=logical_name,
            doc=str(raw_doc) if raw_doc else None,
            parameters=params,
            raw_fields=raw_fields,
            raw_items=raw_items,
            raw_keys=raw_keys,
            raw_values=raw_values,
        )

    def _scalar_decoder(
        self, arrow_type: pa.DataType, logical_name: str | None
    ) -> ValueDecoder:
        if pa.types.is_boolean(arrow_type):
            return _decode_bool
        if pa.types.is_integer(arrow_type):
            return _make_int_decoder(arrow_type)
        if pa.types.is_floating(arrow_type):
            return _decode_float
        if pa.types.is_string(arrow_type):
            return _decode_str
        if pa.types.is_binary(arrow_type):
            return _decode_bytes
        if pa.types.is_decimal(arrow_type):
            if logical_name == "io.debezium.data.VariableScaleDecimal":
                return _make_variable_decimal_decoder(
                    arrow_type.precision, arrow_type.scale
                )
            return _make_decimal_decoder(arrow_type.precision, arrow_type.scale)
        if pa.types.is_date32(arrow_type):
            return _decode_date
        if pa.types.is_time(arrow_type):
            return _make_time_decoder(cast(TimeUnit, arrow_type.unit))
        if pa.types.is_timestamp(arrow_type):
            return _make_timestamp_decoder(
                cast(TimeUnit, arrow_type.unit), arrow_type.tz
            )
        raise ValueError(f"Unsupported compiled Arrow type: {arrow_type}")

    def _struct_decoder(self, fields: tuple[CompiledField, ...]) -> RowDecoder:
        return lambda value: decode_struct_fields(
            _struct_reader(value, "struct field"), fields
        )

    def _array_decoder(self, item_field: CompiledField) -> ValueDecoder:
        item_name, item_nullable, item_dec = item_field

        def decode(value: Any) -> list[Any]:
            if isinstance(value, (str, bytes, bytearray, memoryview)) or not hasattr(
                value, "__iter__"
            ):
                raise TypeError(
                    f"Expected iterable sequence for Connect array field, got {type(value).__name__}: {value!r}"
                )
            return decode_sequence_items(value, item_name, item_nullable, item_dec)

        return decode

    def _map_decoder(
        self,
        key_type: pa.DataType,
        key_dec: ValueDecoder,
        val_field: CompiledField,
    ) -> ValueDecoder:
        val_name, val_nullable, val_dec = val_field

        def decode_key(raw_key: Any) -> Any:
            if raw_key is None:
                raise ValueError("Map key cannot be None")
            converted = key_dec(raw_key)
            if converted is None:
                raise ValueError("Map key cannot be None")
            return converted

        def decode(value: Any) -> dict[Any, Any]:
            if hasattr(value, "keySet"):
                return {
                    decode_key(k): decode_map_value(
                        value.get(k), val_name, val_nullable, val_dec
                    )
                    for k in value.keySet()
                }
            if isinstance(value, Mapping):
                return {
                    decode_key(k): decode_map_value(v, val_name, val_nullable, val_dec)
                    for k, v in value.items()
                }
            raise TypeError(
                f"Expected Mapping for Connect map field, got {type(value).__name__}: {value!r}"
            )

        return decode

    @staticmethod
    def _is_empty_connect_struct(raw_key_schema: Any, raw_key: Any) -> bool:
        if raw_key_schema is None:
            return False
        j_type = raw_key_schema.type()
        if j_type is None or str(j_type.name()).strip().lower() != "struct":
            return False
        if raw_key_schema.fields():
            return False
        if raw_key is None:
            return True
        if hasattr(raw_key, "schema"):
            struct_schema = raw_key.schema()
            if struct_schema is None:
                return False
            st_type = struct_schema.type()
            return (
                st_type is not None
                and str(st_type.name()).strip().lower() == "struct"
                and not struct_schema.fields()
            )
        return False

    def _unpack_event(
        self,
        event: ChangeEvent,
        destination: str,
    ) -> tuple[Any, Any, Any, Any]:
        source_rec = event.value()
        if source_rec is None or not hasattr(source_rec, "valueSchema"):
            raise TypeError(
                f"Expected Java Connect SourceRecord with valueSchema() for destination '{destination}', "
                f"got {type(source_rec).__name__}"
            )
        raw_val_schema = source_rec.valueSchema()
        raw_val = source_rec.value()
        raw_key_schema = source_rec.keySchema()
        raw_key = source_rec.key()
        if self._is_empty_connect_struct(raw_key_schema, raw_key):
            raw_key_schema, raw_key = None, None
        return raw_val_schema, raw_val, raw_key_schema, raw_key
