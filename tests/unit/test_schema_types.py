from __future__ import annotations

import base64
import datetime
import json
import unittest
from decimal import Decimal
from typing import Any

import pyarrow as pa

from pydbzengine.schema import ConnectEventConverter, JsonEventConverter
from pydbzengine.schema.types import (
    TEMPORAL_LOGICAL_TYPES,
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
    resolve_scalar_arrow_type,
    scale_ms_to_unit,
    validate_decimal_bounds,
    validate_variable_scale,
)
from tests.support import MockChangeEvent


class TestSchemaTypesAndDecoders(unittest.TestCase):
    def test_resolve_scalar_arrow_types_and_wire_validation(self) -> None:
        for logical_name, (wire_type, expected_arrow) in TEMPORAL_LOGICAL_TYPES.items():
            resolved = resolve_scalar_arrow_type("col", wire_type, logical_name, {})
            self.assertEqual(resolved, expected_arrow)
            wrong_wire = "boolean"
            with self.assertRaisesRegex(ValueError, "Expected wire type"):
                resolve_scalar_arrow_type("col", wrong_wire, logical_name, {})

        self.assertEqual(
            resolve_scalar_arrow_type(
                "price",
                "bytes",
                "org.apache.kafka.connect.data.Decimal",
                {"connect.decimal.precision": "12", "scale": "4"},
            ),
            pa.decimal128(12, 4),
        )
        self.assertEqual(
            resolve_scalar_arrow_type(
                "var_price",
                "struct",
                "io.debezium.data.VariableScaleDecimal",
                {"precision": "10"},
                default_variable_scale=6,
            ),
            pa.decimal128(10, 6),
        )
        with self.assertRaisesRegex(ValueError, "default_variable_scale"):
            resolve_scalar_arrow_type(
                "var_price",
                "struct",
                "io.debezium.data.VariableScaleDecimal",
                {"precision": "10"},
                default_variable_scale=99,
            )
        with self.assertRaisesRegex(ValueError, "Missing required 'scale'"):
            resolve_scalar_arrow_type(
                "price", "bytes", "org.apache.kafka.connect.data.Decimal", {}
            )
        with self.assertRaisesRegex(ValueError, "Unsupported Debezium Connect type"):
            resolve_scalar_arrow_type("x", "unknown_type", None, {})

    def test_decimal_quantization_and_bounds(self) -> None:
        validate_decimal_bounds(10, 2)
        for bad_p, bad_s in (
            (0, 0),
            (39, 2),
            (10, -1),
            (10, 11),
            (True, 0),
            (10.5, 2),
            (10, 2.5),
        ):
            with self.assertRaises(ValueError):
                validate_decimal_bounds(bad_p, bad_s)  # type: ignore[arg-type]
        for bad_vs in (-1, 39, True):
            with self.assertRaises(ValueError):
                validate_variable_scale(bad_vs)

        raw = (12345).to_bytes(4, byteorder="big", signed=True)
        self.assertEqual(bytes_to_decimal(raw, 2), Decimal("123.45"))
        zero_dec = bytes_to_decimal(b"\x00", 2)
        self.assertEqual(str(zero_dec), "0.00")
        with self.assertRaises(ValueError):
            bytes_to_decimal(b"", 2)

        self.assertEqual(quantize_decimal(Decimal("12.345"), 2, 10), Decimal("12.35"))
        self.assertEqual(str(quantize_decimal(Decimal("12.3"), 2, 10)), "12.30")
        self.assertEqual(str(quantize_decimal(Decimal("-0.001"), 2, 10)), "0.00")
        self.assertEqual(str(quantize_decimal(Decimal(0), 2, 10)), "0.00")
        with self.assertRaises(ValueError):
            quantize_decimal(Decimal("NaN"), 2, 10)
        with self.assertRaises(ValueError):
            quantize_decimal(Decimal("123456.78"), 2, 5)

    def test_temporal_parsing_and_bounds(self) -> None:
        self.assertEqual(parse_iso_date("2024-05-01Z"), datetime.date(2024, 5, 1))
        with self.assertRaises(ValueError):
            parse_iso_date("not-a-date")

        self.assertEqual(
            parse_iso_time("12:30:45.123456+02:00", "us"),
            datetime.time(10, 30, 45, 123456),
        )
        self.assertEqual(
            parse_iso_time("01:02:03.123456789Z", "ns"),
            ((1 * 3600 + 2 * 60 + 3) * 1_000_000 + 123456) * 1_000 + 789,
        )
        self.assertEqual(
            parse_iso_timestamp("2024-01-01T00:00:01.000000789Z", "ns", "UTC"),
            1704067201_000_000_789,
        )
        self.assertEqual(
            parse_iso_timestamp("2024-01-01T12:00:00Z", "us", None),
            datetime.datetime(2024, 1, 1, 12, 0, 0),
        )
        self.assertEqual(
            parse_iso_timestamp("2024-01-01T12:00:00", "us", "UTC"),
            datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )
        utc_ts = parse_iso_timestamp("2024-01-01T12:00:00+02:00", "us", "UTC")
        self.assertEqual(
            utc_ts,
            datetime.datetime(2024, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc),
        )
        assert isinstance(utc_ts, datetime.datetime)
        self.assertEqual(utc_ts.tzinfo, datetime.timezone.utc)

        self.assertEqual(int_bounds(pa.int8()), (-128, 127))
        with self.assertRaises(ValueError):
            int_bounds(pa.float64())
        self.assertEqual(check_int_bounds(127, -128, 127, pa.int8()), 127)
        with self.assertRaises(ValueError):
            check_int_bounds(128, -128, 127, pa.int8())
        self.assertEqual(check_date_days(100), 100)
        with self.assertRaises(ValueError):
            check_date_days(1 << 32)
        self.assertEqual(check_time_offset(0, "ms"), 0)
        with self.assertRaises(ValueError):
            check_time_offset(86_400_000, "ms")
        self.assertEqual(scale_ms_to_unit(5, "us"), 5_000)
        with self.assertRaises(KeyError):
            scale_ms_to_unit(5, "s")  # type: ignore[arg-type]
        self.assertEqual(check_timestamp_epoch(0, "us"), 0)
        with self.assertRaises(ValueError):
            check_timestamp_epoch(1 << 63, "us")

    def test_json_format_decoders_and_error_paths(self) -> None:
        conv = JsonEventConverter()
        schema = {
            "type": "struct",
            "fields": [
                {"field": "id", "type": "int8", "optional": False},
                {"field": "raw", "type": "bytes", "optional": True},
                {
                    "field": "dec",
                    "type": "bytes",
                    "name": "org.apache.kafka.connect.data.Decimal",
                    "parameters": {"connect.decimal.precision": "8", "scale": "2"},
                    "optional": True,
                },
                {
                    "field": "scores",
                    "type": "map",
                    "keys": {"type": "int32", "optional": False},
                    "values": {"type": "float64", "optional": True},
                    "optional": True,
                },
                {
                    "field": "flags",
                    "type": "map",
                    "keys": {"type": "boolean", "optional": False},
                    "values": {"type": "string", "optional": True},
                    "optional": True,
                },
            ],
        }
        dec_b64 = base64.b64encode((1234).to_bytes(2, "big", signed=True)).decode(
            "ascii"
        )
        ev = MockChangeEvent(
            key=None,
            value=json.dumps(
                {
                    "schema": schema,
                    "payload": {
                        "id": 7,
                        "raw": base64.b64encode(b"hi").decode("ascii"),
                        "dec": dec_b64,
                        "scores": {"1": 2.5, "2": 3.0},
                        "flags": [["true", "yes"], ["false", "no"]],
                    },
                }
            ),
            destination="db.items",
        )
        rec = conv.convert_event(ev)
        self.assertEqual(
            rec.value,
            {
                "id": 7,
                "raw": b"hi",
                "dec": Decimal("12.34"),
                "scores": {1: 2.5, 2: 3.0},
                "flags": {True: "yes", False: "no"},
            },
        )

        # Nested field-path diagnostics & non-nullable enforcement
        bad_null = MockChangeEvent(
            key=None,
            value=json.dumps({"schema": schema, "payload": {"id": None}}),
            destination="db.items",
        )
        with self.assertRaisesRegex(ValueError, "Non-nullable field 'id'"):
            conv.convert_event(bad_null)

        bad_overflow = MockChangeEvent(
            key=None,
            value=json.dumps({"schema": schema, "payload": {"id": 999}}),
            destination="db.items",
        )
        with self.assertRaisesRegex(ValueError, "Field 'id':"):
            conv.convert_event(bad_overflow)

        bad_bool_key = MockChangeEvent(
            key=None,
            value=json.dumps(
                {"schema": schema, "payload": {"id": 1, "flags": {"maybe": "x"}}}
            ),
            destination="db.items",
        )
        with self.assertRaisesRegex(ValueError, "Invalid boolean map key"):
            conv.convert_event(bad_bool_key)

    def test_connect_format_java_runtime_types(self) -> None:
        from pydbzengine.engine.jvm import ensure_jvm_started

        ensure_jvm_started()
        import jpype

        Schema = jpype.JClass("org.apache.kafka.connect.data.Schema")
        SchemaBuilder = jpype.JClass("org.apache.kafka.connect.data.SchemaBuilder")
        Struct = jpype.JClass("org.apache.kafka.connect.data.Struct")
        SourceRecord = jpype.JClass("org.apache.kafka.connect.source.SourceRecord")
        ByteBuffer = jpype.JClass("java.nio.ByteBuffer")
        BigDecimal = jpype.JClass("java.math.BigDecimal")
        JDate = jpype.JClass("java.util.Date")
        HashMap = jpype.JClass("java.util.HashMap")
        ArrayList = jpype.JClass("java.util.ArrayList")

        dec_schema = (
            SchemaBuilder.bytes()
            .name("org.apache.kafka.connect.data.Decimal")
            .parameter("connect.decimal.precision", "10")
            .parameter("scale", "2")
            .optional()
            .build()
        )
        ts_schema = (
            SchemaBuilder.int64()
            .name("org.apache.kafka.connect.data.Timestamp")
            .optional()
            .build()
        )
        map_schema = (
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
            .optional()
            .build()
        )
        arr_schema = SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build()
        val_schema = (
            SchemaBuilder.struct()
            .name("db.connect_types.Value")
            .field("id", Schema.INT32_SCHEMA)
            .field("flag", Schema.BOOLEAN_SCHEMA)
            .field("heap_buf", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("direct_buf", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("amount", dec_schema)
            .field("created_at", ts_schema)
            .field("tags", arr_schema)
            .field("attrs", map_schema)
            .build()
        )

        heap_buf = ByteBuffer.wrap(jpype.JArray(jpype.JByte)(b"\x01\x02\x03"))
        direct_buf = ByteBuffer.allocateDirect(3)
        direct_buf.put(jpype.JArray(jpype.JByte)(b"\x04\x05\x06"))
        direct_buf.flip()

        j_tags = ArrayList()
        j_tags.add("a")
        j_tags.add("b")
        j_attrs = HashMap()
        j_attrs.put("k", jpype.JInt(9))

        st = Struct(val_schema)
        st.put("id", jpype.JInt(11))
        st.put("flag", jpype.JBoolean(True))
        st.put("heap_buf", heap_buf)
        st.put("direct_buf", direct_buf)
        st.put("amount", BigDecimal("42.50"))
        st.put("created_at", JDate(jpype.JLong(1_700_000_000_000)))
        st.put("tags", j_tags)
        st.put("attrs", j_attrs)

        src = SourceRecord(None, None, "db.connect_types", None, None, val_schema, st)
        rec = ConnectEventConverter().convert_event(
            MockChangeEvent(key=src, value=src, destination="db.connect_types")
        )
        self.assertEqual(
            rec.value,
            {
                "id": 11,
                "flag": True,
                "heap_buf": b"\x01\x02\x03",
                "direct_buf": b"\x04\x05\x06",
                "amount": Decimal("42.50"),
                "created_at": 1_700_000_000_000,
                "tags": ["a", "b"],
                "attrs": {"k": 9},
            },
        )
        # Direct ByteBuffer must not have its position mutated by conversion
        self.assertEqual(int(direct_buf.position()), 0)
        self.assertEqual(int(direct_buf.remaining()), 3)

        # Reject BigDecimal/float/bool in Connect int field
        bad_struct = Struct(
            SchemaBuilder.struct().field("n", Schema.INT64_SCHEMA).build()
        )

        class _FakeStruct:
            def __init__(self, val: Any) -> None:
                self._val = val

            def getWithoutDefault(self, _name: str) -> Any:
                return self._val

        class _FakeRec:
            def __init__(self, schema: Any, val: Any) -> None:
                self._schema = schema
                self._val = val

            def valueSchema(self) -> Any:
                return self._schema

            def value(self) -> Any:
                return _FakeStruct(self._val)

            def keySchema(self) -> Any:
                return None

            def key(self) -> Any:
                return None

        conv = ConnectEventConverter()
        int_schema = bad_struct.schema()
        for bad_val in (
            BigDecimal("3.7"),
            jpype.JClass("java.lang.Double")(3.5),
            jpype.JClass("java.lang.Boolean")(True),
        ):
            with self.assertRaises(TypeError):
                conv.convert_event(
                    MockChangeEvent(
                        key=None,
                        value=_FakeRec(int_schema, bad_val),
                        destination="db.bad",
                    )
                )


if __name__ == "__main__":
    unittest.main()
