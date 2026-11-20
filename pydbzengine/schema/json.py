from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
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
)

__all__ = ["JsonEventConverter"]


def _decode_json_object(raw: Any, context: str) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray, memoryview)):
        payload_str = bytes(raw).decode("utf-8").strip()
    elif isinstance(raw, str) or type(raw).__name__ == "java.lang.String":
        payload_str = str(raw).strip()
    else:
        raise TypeError(
            f"Unsupported payload type {type(raw).__name__} for {context}: {raw!r}"
        )
    try:
        parsed = json.loads(payload_str, parse_float=Decimal)
    except json.JSONDecodeError as err:
        raise ValueError(f"Invalid JSON in {context}: {err}") from err
    if not isinstance(parsed, dict):
        raise ValueError(
            f"Expected JSON object in {context}, got {type(parsed).__name__}"
        )
    return parsed


def _decode_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raise TypeError(f"Expected bool, got {type(value).__name__}: {value!r}")


def _decode_int(value: Any) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise TypeError(f"Expected int, got {type(value).__name__}: {value!r}")


def _decode_float(value: Any) -> float:
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return float(value)
    raise TypeError(f"Expected float, got {type(value).__name__}: {value!r}")


def _decode_str(value: Any) -> str:
    if isinstance(value, str):
        return value
    raise TypeError(f"Expected str, got {type(value).__name__}: {value!r}")


def _decode_bytes(value: Any) -> bytes:
    if not isinstance(value, str):
        raise TypeError(
            f"Expected Base64 str for JSON binary field, got {type(value).__name__}: {value!r}"
        )
    try:
        return base64.b64decode(value.strip(), validate=True)
    except (binascii.Error, ValueError) as err:
        raise ValueError(f"Invalid base64 string for binary field: {value!r}") from err


def _decode_decimal(raw: Any, scale: int) -> Decimal:
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
        raise TypeError(f"Cannot decode JSON {type(raw).__name__} as Decimal: {raw!r}")
    raw_str = raw.strip()
    if not raw_str:
        raise ValueError("Cannot decode empty string as Decimal")
    try:
        decoded_bytes = base64.b64decode(raw_str, validate=True)
    except (binascii.Error, ValueError) as err:
        raise ValueError(f"Malformed Base64 decimal string: {raw_str!r}") from err
    return bytes_to_decimal(decoded_bytes, scale)


def _decode_date(value: Any) -> date | int:
    if isinstance(value, int) and not isinstance(value, bool):
        return check_date_days(value)
    if isinstance(value, str):
        return parse_iso_date(value)
    raise TypeError(f"Cannot decode {type(value).__name__} into date: {value!r}")


def _make_int_decoder(arrow_type: pa.DataType) -> ValueDecoder:
    min_v, max_v = int_bounds(arrow_type)
    return lambda v: check_int_bounds(_decode_int(v), min_v, max_v, arrow_type)


def _make_decimal_decoder(precision: int, scale: int) -> ValueDecoder:
    return lambda v: quantize_decimal(_decode_decimal(v, scale), scale, precision)


def _make_variable_decimal_decoder(precision: int, scale: int) -> ValueDecoder:
    def decode(value: Any) -> Decimal | None:
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
        return quantize_decimal(
            _decode_decimal(raw_val, _decode_int(value["scale"])), scale, precision
        )

    return decode


def _make_time_decoder(unit: TimeUnit) -> ValueDecoder:
    def decode(value: Any) -> time | int:
        if isinstance(value, int) and not isinstance(value, bool):
            return check_time_offset(value, unit)
        if isinstance(value, str):
            return parse_iso_time(value, unit)
        raise TypeError(f"Cannot decode {type(value).__name__} into time: {value!r}")

    return decode


def _make_timestamp_decoder(unit: TimeUnit, tz: str | None) -> ValueDecoder:
    def decode(value: Any) -> datetime | int:
        if isinstance(value, int) and not isinstance(value, bool):
            return check_timestamp_epoch(value, unit)
        if isinstance(value, str):
            return parse_iso_timestamp(value, unit, tz)
        raise TypeError(
            f"Cannot decode {type(value).__name__} into timestamp: {value!r}"
        )

    return decode


class JsonEventConverter(EventConverter):
    """
    Dedicated converter for schema-enabled JSON records (`EventFormat.JSON`, `schemas_enabled=True`).

    Strictly requires `{"schema": ..., "payload": ...}` envelopes and decodes
    JSON wire payloads with zero Java/Connect reflection overhead.
    """

    format: ClassVar[EventFormat] = EventFormat.JSON

    def _inspect_schema(
        self, raw_schema: Any, field_name: str | None = None
    ) -> _SchemaNode:
        if not isinstance(raw_schema, Mapping):
            raise ValueError(
                f"Expected dict in schema definition, got {type(raw_schema).__name__}"
            )
        resolved_name = (
            field_name if field_name is not None else raw_schema.get("field")
        )
        if (
            not resolved_name
            or not isinstance(resolved_name, str)
            or not resolved_name.strip()
        ):
            raise ValueError(
                f"Encountered field without valid name in schema: {raw_schema}"
            )
        resolved_name = resolved_name.strip()

        raw_type = raw_schema.get("type")
        if not isinstance(raw_type, str) or not raw_type.strip():
            raise ValueError(
                f"Missing or invalid 'type' in schema for field '{resolved_name}': {raw_schema}"
            )
        type_str = raw_type.strip().lower()

        optional = raw_schema.get("optional", True)
        if not isinstance(optional, bool):
            raise ValueError(
                f"Expected bool for schema 'optional' in field '{resolved_name}', got {type(optional).__name__}"
            )

        raw_logical = raw_schema.get("name")
        if raw_logical is not None and not isinstance(raw_logical, str):
            raise ValueError(
                f"Expected str for schema 'name' in field '{resolved_name}', got {type(raw_logical).__name__}"
            )
        logical_name = raw_logical.strip() or None if raw_logical is not None else None

        raw_doc = raw_schema.get("doc")
        if raw_doc is not None and not isinstance(raw_doc, str):
            raise ValueError(
                f"Expected str for schema 'doc' in field '{resolved_name}', got {type(raw_doc).__name__}"
            )

        raw_params = raw_schema.get("parameters")
        if raw_params is not None and not isinstance(raw_params, Mapping):
            raise ValueError(
                f"Expected mapping for schema 'parameters' in field '{resolved_name}', got {type(raw_params).__name__}"
            )
        params = (
            {str(k): str(v) for k, v in raw_params.items()}
            if raw_params is not None
            else {}
        )

        raw_fields: tuple[tuple[str, Any], ...] = ()
        if type_str == "struct":
            fields_list = raw_schema.get("fields")
            if fields_list is not None and not isinstance(fields_list, list):
                raise ValueError(
                    f"Expected list for schema 'fields' in field '{resolved_name}', got {type(fields_list).__name__}"
                )
            if fields_list:
                raw_fields = tuple(("", sub) for sub in fields_list)

        return _SchemaNode(
            name=resolved_name,
            type_name=type_str,
            nullable=optional,
            logical_name=logical_name,
            doc=raw_doc,
            parameters=params,
            raw_fields=raw_fields,
            raw_items=raw_schema.get("items") if type_str == "array" else None,
            raw_keys=raw_schema.get("keys") if type_str == "map" else None,
            raw_values=raw_schema.get("values") if type_str == "map" else None,
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
        def decode(value: Any) -> dict[str, Any]:
            if not isinstance(value, Mapping):
                raise TypeError(
                    f"Expected dict or Mapping for struct field, got {type(value).__name__}: {value!r}"
                )
            return decode_struct_fields(value.get, fields)

        return decode

    def _array_decoder(self, item_field: CompiledField) -> ValueDecoder:
        item_name, item_nullable, item_dec = item_field

        def decode(value: Any) -> list[Any]:
            if not isinstance(value, Sequence) or isinstance(
                value, (str, bytes, bytearray, memoryview)
            ):
                raise TypeError(
                    f"Expected sequence for array field, got {type(value).__name__}: {value!r}"
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
        is_int_key = pa.types.is_integer(key_type)
        is_float_key = pa.types.is_floating(key_type)
        is_bool_key = pa.types.is_boolean(key_type)

        def decode_key(raw_key: Any) -> Any:
            if raw_key is None:
                raise ValueError("Map key cannot be None")
            if isinstance(raw_key, str):
                if is_int_key:
                    raw_key = int(raw_key)
                elif is_float_key:
                    raw_key = float(raw_key)
                elif is_bool_key:
                    norm = raw_key.strip().lower()
                    if norm == "true":
                        raw_key = True
                    elif norm == "false":
                        raw_key = False
                    else:
                        raise ValueError(f"Invalid boolean map key: {raw_key!r}")
            converted = key_dec(raw_key)
            if converted is None:
                raise ValueError("Map key cannot be None")
            return converted

        def decode(value: Any) -> dict[Any, Any]:
            if isinstance(value, Mapping):
                return {
                    decode_key(k): decode_map_value(v, val_name, val_nullable, val_dec)
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
                    result[decode_key(item[0])] = decode_map_value(
                        item[1], val_name, val_nullable, val_dec
                    )
                return result
            raise TypeError(
                f"Expected Mapping or sequence of [key, value] pairs for map field, "
                f"got {type(value).__name__}: {value!r}"
            )

        return decode

    def _unpack_event(
        self,
        event: ChangeEvent,
        destination: str,
    ) -> tuple[Any, Any, Any, Any]:
        val_schema, val_payload = self._unpack_envelope(
            event.value(), f"event for destination '{destination}'"
        )
        key_schema, key_payload = self._unpack_envelope(
            event.key(), f"CDC record key for destination '{destination}'"
        )
        if (
            isinstance(key_schema, Mapping)
            and isinstance(key_schema.get("type"), str)
            and key_schema["type"].strip().lower() == "struct"
            and (key_schema.get("fields") == [] or key_schema.get("fields") is None)
            and (
                key_payload is None
                or (isinstance(key_payload, Mapping) and len(key_payload) == 0)
            )
        ):
            key_schema, key_payload = None, None
        return val_schema, val_payload, key_schema, key_payload

    @staticmethod
    def _unpack_envelope(raw: Any, context: str) -> tuple[Any, Any]:
        if raw is None:
            return None, None
        envelope = _decode_json_object(raw, context)
        if "schema" not in envelope or "payload" not in envelope:
            raise ValueError(
                f"Missing required 'schema' and 'payload' blocks in {context}. "
                "Enable schemas ('converter.schemas.enable=true') for pydbzengine.schema, "
                "or subclass BasePythonChangeHandler for schemaless JSON payloads."
            )
        return envelope["schema"], envelope["payload"]
