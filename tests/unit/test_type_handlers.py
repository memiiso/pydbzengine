from __future__ import annotations

import base64
import copy
import datetime
import gc
import pickle
import unittest
from decimal import Decimal
from typing import Any

import pyarrow as pa

from pydbzengine.schema.handlers import (
    ArrayTypeHandler,
    BinaryTypeHandler,
    BooleanTypeHandler,
    DecimalTypeHandler,
    FloatTypeHandler,
    IntTypeHandler,
    MapTypeHandler,
    SchemaField,
    StringTypeHandler,
    StructTypeHandler,
    VariableScaleDecimalHandler,
)
from pydbzengine.schema.temporal import (
    DateTypeHandler,
    TimestampTypeHandler,
    TimeTypeHandler,
)


class TestTypeHandlers(unittest.TestCase):
    def test_scalar_handlers_json_and_connect(self) -> None:
        int_h = IntTypeHandler(pa.int64())
        self.assertEqual(int_h.convert_json(42), 42)
        with self.assertRaises(TypeError):
            int_h.convert_json(True)
        with self.assertRaises(TypeError):
            int_h.convert_json(Decimal("3.5"))
        with self.assertRaises(ValueError):
            IntTypeHandler(pa.binary())

        class MockJavaBoolean:
            def booleanValue(self) -> bool:
                return True

        self.assertTrue(BooleanTypeHandler().convert_connect(MockJavaBoolean()))

        float_h = FloatTypeHandler(pa.float64())
        self.assertEqual(float_h.convert_json(3.5), 3.5)
        self.assertEqual(float_h.convert_json(Decimal("3.5")), 3.5)
        self.assertEqual(float_h.convert_connect(Decimal("3.5")), 3.5)
        with self.assertRaises(TypeError):
            float_h.convert_json(True)

        str_h = StringTypeHandler()
        self.assertEqual(str_h.convert_json("hello"), "hello")
        self.assertEqual(str_h.convert_connect("hello"), "hello")

    def test_int_bounds_validation_json_and_connect(self) -> None:
        for pa_type, min_v, max_v in (
            (pa.int8(), -128, 127),
            (pa.int16(), -32_768, 32_767),
            (pa.int32(), -2_147_483_648, 2_147_483_647),
            (pa.int64(), -(1 << 63), (1 << 63) - 1),
        ):
            ih = IntTypeHandler(pa_type)
            self.assertEqual(ih.convert_json(min_v), min_v)
            self.assertEqual(ih.convert_json(max_v), max_v)
            self.assertEqual(ih.convert_connect(min_v), min_v)
            self.assertEqual(ih.convert_connect(max_v), max_v)
            for out_of_bounds in (min_v - 1, max_v + 1):
                with self.assertRaises(ValueError):
                    ih.convert_json(out_of_bounds)
                with self.assertRaises(ValueError):
                    ih.convert_connect(out_of_bounds)

    def test_connect_rejects_java_bigdecimal_and_floats_in_int_and_temporal_handlers(
        self,
    ) -> None:
        from pydbzengine.engine.jvm import ensure_jvm_started

        ensure_jvm_started()
        import jpype

        j_bigdec = jpype.JClass("java.math.BigDecimal")("3.7")
        j_double = jpype.JClass("java.lang.Double")(3.99)
        j_float = jpype.JClass("java.lang.Float")(2.5)
        j_bool = jpype.JClass("java.lang.Boolean")(True)

        for handler in (
            IntTypeHandler(pa.int64()),
            DateTypeHandler(),
            TimeTypeHandler(),
            TimestampTypeHandler(),
        ):
            for bad_val in (j_bigdec, j_double, j_float, j_bool, Decimal("3.7")):
                with self.assertRaises(
                    TypeError,
                    msg=f"{type(handler).__name__} must reject {type(bad_val).__name__}",
                ):
                    handler.convert_connect(bad_val)

        with self.assertRaises(TypeError):
            DecimalTypeHandler().convert_connect(j_bool)

        # BigInteger (> int64) and AtomicLong (> int32) fail fast instead of silently wrapping
        BigInteger = jpype.JClass("java.math.BigInteger")
        AtomicLong = jpype.JClass("java.util.concurrent.atomic.AtomicLong")
        huge_bi = BigInteger("18446744073709551617")  # 2^64 + 1
        over_int32 = AtomicLong(3_000_000_000)

        with self.assertRaises(ValueError):
            IntTypeHandler(pa.int64()).convert_connect(huge_bi)
        with self.assertRaises(ValueError):
            DateTypeHandler().convert_connect(over_int32)
        with self.assertRaises(ValueError):
            DateTypeHandler().convert_connect(huge_bi)
        with self.assertRaises(ValueError):
            TimeTypeHandler("ms").convert_connect(huge_bi)
        with self.assertRaises(ValueError):
            TimestampTypeHandler("us").convert_connect(huge_bi)

    def test_binary_handler_direct_readonly_and_sliced_java_byte_buffers(self) -> None:
        from pydbzengine.engine.jvm import ensure_jvm_started

        ensure_jvm_started()
        import jpype

        ByteBuffer = jpype.JClass("java.nio.ByteBuffer")
        raw = bytes([0xAA, 0xDE, 0xAD, 0xBE, 0xEF, 0xBB])
        bin_h = BinaryTypeHandler()

        # 1. Heap ByteBuffer sliced
        b_heap = ByteBuffer.wrap(raw)
        b_heap.position(1)
        b_heap.limit(5)
        self.assertEqual(bin_h.convert_connect(b_heap), b"\xde\xad\xbe\xef")
        self.assertEqual(int(b_heap.position()), 1)

        # 2. Direct ByteBuffer (hasArray() == False)
        b_direct = ByteBuffer.allocateDirect(len(raw))
        b_direct.put(raw)
        b_direct.flip()
        b_direct.position(1)
        b_direct.limit(5)
        self.assertEqual(bin_h.convert_connect(b_direct), b"\xde\xad\xbe\xef")
        self.assertEqual(int(b_direct.position()), 1)

        # 3. Read-only ByteBuffer (hasArray() == False)
        b_ro = ByteBuffer.wrap(raw).asReadOnlyBuffer()
        b_ro.position(1)
        b_ro.limit(5)
        self.assertEqual(bin_h.convert_connect(b_ro), b"\xde\xad\xbe\xef")
        self.assertEqual(int(b_ro.position()), 1)

        # 3b. Read-only direct ByteBuffer and empty 0-byte direct ByteBuffer
        b_ro_direct = b_direct.asReadOnlyBuffer()
        self.assertEqual(bin_h.convert_connect(b_ro_direct), b"\xde\xad\xbe\xef")
        self.assertEqual(int(b_ro_direct.position()), 1)
        self.assertEqual(bin_h.convert_connect(ByteBuffer.allocateDirect(0)), b"")

        # 4. DecimalTypeHandler.decode_connect also works with direct & read-only ByteBuffers
        #    (positive and negative two's-complement) and rejects empty 0-byte buffers
        dec_bytes = (19995).to_bytes(4, byteorder="big", signed=True)
        d_direct = ByteBuffer.allocateDirect(len(dec_bytes))
        d_direct.put(dec_bytes)
        d_direct.flip()
        d_ro = ByteBuffer.wrap(dec_bytes).asReadOnlyBuffer()
        dec_h = DecimalTypeHandler(precision=10, scale=2)
        self.assertEqual(dec_h.convert_connect(d_direct), Decimal("199.95"))
        self.assertEqual(dec_h.convert_connect(d_ro), Decimal("199.95"))

        neg_dec_bytes = (-19995).to_bytes(4, byteorder="big", signed=True)
        d_neg_direct = ByteBuffer.allocateDirect(len(neg_dec_bytes))
        d_neg_direct.put(neg_dec_bytes)
        d_neg_direct.flip()
        self.assertEqual(dec_h.convert_connect(d_neg_direct), Decimal("-199.95"))

        with self.assertRaisesRegex(ValueError, "empty bytes"):
            dec_h.convert_connect(b"")
        with self.assertRaisesRegex(ValueError, "empty bytes"):
            dec_h.convert_connect(ByteBuffer.allocateDirect(0))

        # 5. Reject list inputs
        with self.assertRaises(TypeError):
            bin_h.convert_json([1, 2, 3])
        with self.assertRaises(TypeError):
            bin_h.convert_connect([1, 2, 3])

    def test_java_sql_date_time_and_timestamp_under_non_utc_jvm_timezone(self) -> None:
        from pydbzengine.engine.jvm import ensure_jvm_started

        ensure_jvm_started()
        import jpype

        TimeZone = jpype.JClass("java.util.TimeZone")
        original_tz = TimeZone.getDefault()
        try:
            TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"))

            SqlDate = jpype.JClass("java.sql.Date")
            SqlTime = jpype.JClass("java.sql.Time")
            SqlTimestamp = jpype.JClass("java.sql.Timestamp")
            ConnectDate = jpype.JClass("org.apache.kafka.connect.data.Date")
            ConnectTime = jpype.JClass("org.apache.kafka.connect.data.Time")

            # java.sql.Date uses toLocalDate().toEpochDay() (no -1 day shift under CET +01:00)
            sql_d = SqlDate.valueOf("2024-01-15")
            date_h = DateTypeHandler()
            expected_days = (
                datetime.date(2024, 1, 15) - datetime.date(1970, 1, 1)
            ).days
            self.assertEqual(date_h.convert_connect(sql_d), expected_days)

            # Kafka Connect java.util.Date (UTC midnight) also produces exact epoch day
            connect_d = ConnectDate.toLogical(ConnectDate.SCHEMA, expected_days)
            self.assertEqual(date_h.convert_connect(connect_d), expected_days)

            # java.sql.Time preserves local time AND sub-second milliseconds (no CET shift or ms truncation)
            sql_t = SqlTime.valueOf("00:30:00")
            sql_t_ms = SqlTime(sql_t.getTime() + 456)
            self.assertEqual(
                TimeTypeHandler(unit="ms").convert_connect(sql_t_ms), 1_800_456
            )
            self.assertEqual(
                TimeTypeHandler(unit="us").convert_connect(sql_t_ms), 1_800_456_000
            )
            self.assertEqual(
                TimeTypeHandler(unit="ns").convert_connect(sql_t_ms), 1_800_456_000_000
            )

            # java.time.OffsetTime normalizes timezone offsets to UTC (including day wrap)
            OffsetTime = jpype.JClass("java.time.OffsetTime")
            ZoneOffset = jpype.JClass("java.time.ZoneOffset")
            ot_mid = OffsetTime.of(12, 30, 0, 0, ZoneOffset.ofHours(2))
            self.assertEqual(
                TimeTypeHandler(unit="ms").convert_connect(ot_mid),
                (10 * 3_600 + 30 * 60) * 1_000,
            )
            ot_wrap = OffsetTime.of(1, 0, 0, 0, ZoneOffset.ofHours(2))
            self.assertEqual(
                TimeTypeHandler(unit="ms").convert_connect(ot_wrap),
                23 * 3_600 * 1_000,
            )

            # Kafka Connect java.util.Date time (UTC 1970-01-01) works cleanly
            connect_t = ConnectTime.toLogical(ConnectTime.SCHEMA, 1_800_000)
            self.assertEqual(
                TimeTypeHandler(unit="ms").convert_connect(connect_t), 1_800_000
            )

            # java.sql.Date and java.sql.Time raise TypeError in TimestampTypeHandler instead of UnsupportedOperationException
            ts_h = TimestampTypeHandler(unit="us")
            with self.assertRaises(TypeError):
                ts_h.convert_connect(sql_d)
            with self.assertRaises(TypeError):
                ts_h.convert_connect(sql_t)

            # java.sql.Timestamp preserves sub-second nanoseconds accurately
            sql_ts = SqlTimestamp.from_(
                jpype.JClass("java.time.Instant").ofEpochSecond(1710513000, 123456789)
            )
            self.assertEqual(
                TimestampTypeHandler(unit="ns").convert_connect(sql_ts),
                1710513000 * 1_000_000_000 + 123456789,
            )

            # Cross-type rejections: java.sql.Time & java.sql.Timestamp in DateTypeHandler;
            # java.sql.Date & java.sql.Timestamp in TimeTypeHandler
            with self.assertRaises(TypeError):
                date_h.convert_connect(sql_t)
            with self.assertRaises(TypeError):
                date_h.convert_connect(sql_ts)
            with self.assertRaises(TypeError):
                TimeTypeHandler(unit="ms").convert_connect(sql_d)
            with self.assertRaises(TypeError):
                TimeTypeHandler(unit="ms").convert_connect(SqlTimestamp(1000))
        finally:
            TimeZone.setDefault(original_tz)

    def test_decimal_and_variable_scale_decimal_precision_and_base64_safety(
        self,
    ) -> None:
        # 38-digit precision
        val_38 = 12345678901234567890123456789012345678
        raw_bytes = val_38.to_bytes(
            (val_38.bit_length() + 8) // 8, byteorder="big", signed=True
        )
        b64_val = base64.b64encode(raw_bytes).decode("ascii")
        dec_h = DecimalTypeHandler(precision=38, scale=10)
        self.assertEqual(
            dec_h.convert_json(b64_val),
            Decimal("1234567890123456789012345678.9012345678"),
        )

        # >38-digit unscaled integer rounds once at target scale (no intermediate double rounding)
        unscaled_39 = 100000000004999999999999999999999999999
        b64_39 = base64.b64encode(
            unscaled_39.to_bytes(
                (unscaled_39.bit_length() + 8) // 8, "big", signed=True
            )
        ).decode("ascii")
        vardec_h = VariableScaleDecimalHandler(precision=38, scale=10)
        self.assertEqual(
            vardec_h.convert_json({"scale": 38, "value": b64_39}),
            Decimal("1.0000000000"),
        )
        self.assertIsNone(vardec_h.convert_json({"scale": 2, "value": None}))

        # Negative scale (1E+2 = 100)
        b64_one = base64.b64encode((1).to_bytes(1, "big", signed=True)).decode("ascii")
        self.assertEqual(
            vardec_h.convert_json({"scale": -2, "value": b64_one}),
            Decimal("100"),
        )

        # Overflow rejection
        dec_5_2 = DecimalTypeHandler(precision=5, scale=2)
        self.assertEqual(dec_5_2.convert_json(999.99), Decimal("999.99"))
        for overflow_val in (
            1000,
            1234.5,
            999.999,
            Decimal("1e3"),
            -1000,
            base64.b64encode((100_000).to_bytes(3, "big", signed=True)).decode("ascii"),
        ):
            with self.assertRaises(ValueError):
                dec_5_2.convert_json(overflow_val)

        # Non-finite rejection
        for bad_float in (float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                DecimalTypeHandler(10, 2).convert_json(bad_float)
        for bad_dec in (Decimal("NaN"), Decimal("Infinity")):
            with self.assertRaises(ValueError):
                DecimalTypeHandler(10, 2).convert_connect(bad_dec)

        # Base64 collision safety ("1234", "+123", "1e23" are decoded as Base64 bytes, not numeric strings)
        dec_10_2 = DecimalTypeHandler(precision=10, scale=2)
        for unscaled, expected_b64, expected_dec in (
            (-2_658_824, "1234", Decimal("-26588.24")),
            (-303_689, "+123", Decimal("-3036.89")),
            (-2_757_193, "1e23", Decimal("-27571.93")),
        ):
            wire_b64 = base64.b64encode(
                unscaled.to_bytes(3, byteorder="big", signed=True)
            ).decode("ascii")
            self.assertEqual(wire_b64, expected_b64)
            self.assertEqual(dec_10_2.convert_json(wire_b64), expected_dec)

        # VariableScaleDecimal strict validation
        with self.assertRaises(ValueError):
            vardec_h.convert_json({"scale": 2})
        with self.assertRaises(ValueError):
            vardec_h.convert_json({"value": "D4A="})
        with self.assertRaises(ValueError):
            vardec_h.convert_json({"scale": None, "value": "D4A="})
        with self.assertRaises(TypeError):
            vardec_h.convert_json({"scale": "2", "value": "D4A="})
        with self.assertRaises(TypeError):
            vardec_h.convert_json({"scale": True, "value": "D4A="})
        with self.assertRaises(TypeError):
            vardec_h.convert_json("not_a_mapping")

    def test_temporal_iso_strings_nanoseconds_and_bounds(self) -> None:
        date_h = DateTypeHandler()
        self.assertEqual(date_h.convert_json("2024-05-05Z"), datetime.date(2024, 5, 5))
        with self.assertRaises(ValueError):
            date_h.convert_json("2024-05-05T14:30:00Z")

        ts_ns_h = TimestampTypeHandler(unit="ns")
        tm_ns_h = TimeTypeHandler(unit="ns")
        expected_ts_ns = 1714919400 * 1_000_000_000 + 123_456_789
        self.assertEqual(
            ts_ns_h.convert_json("2024-05-05T14:30:00.123456789Z"),
            expected_ts_ns,
        )
        self.assertEqual(
            ts_ns_h.convert_connect("2024-05-05T16:30:00.123456789+02:00"),
            expected_ts_ns,
        )
        self.assertEqual(
            ts_ns_h.convert_json("1969-12-31T23:59:59.123456789Z"),
            -1_000_000_000 + 123_456_789,
        )
        self.assertEqual(
            ts_ns_h.convert_json("2024-05-05T14:30:00.1234567Z"),
            1714919400 * 1_000_000_000 + 123_456_700,
        )

        expected_tm_ns = (4 * 3_600 + 5 * 60 + 11) * 1_000_000_000 + 123_456_789
        self.assertEqual(tm_ns_h.convert_json("04:05:11.123456789"), expected_tm_ns)
        self.assertEqual(tm_ns_h.convert_connect("04:05:11.123456789Z"), expected_tm_ns)

        # Non-3/6 digit fractional seconds normalize to microseconds
        ts_us_h = TimestampTypeHandler(unit="us", tz="UTC")
        self.assertEqual(
            ts_us_h.convert_json("2024-05-01T12:00:00.1Z"),
            datetime.datetime(
                2024, 5, 1, 12, 0, 0, 100000, tzinfo=datetime.timezone.utc
            ),
        )
        tm_us_h = TimeTypeHandler(unit="us")
        self.assertEqual(
            tm_us_h.convert_json("12:30:00.1234+02:00"),
            datetime.time(10, 30, 0, 123400),
        )

        # Time bounds
        tm_ms = TimeTypeHandler(unit="ms")
        with self.assertRaises(ValueError):
            tm_ms.convert_json(-1)
        with self.assertRaises(ValueError):
            tm_ms.convert_json(86_400_000)

    def test_map_array_and_struct_handlers_nullability_and_key_errors(self) -> None:
        int_map_h = MapTypeHandler(
            SchemaField("key", IntTypeHandler(pa.int16()), nullable=False),
            SchemaField("value", FloatTypeHandler(pa.float64()), nullable=True),
        )
        self.assertEqual(
            int_map_h.convert_json([[1, 0.5], [5, 1.25]]), {1: 0.5, 5: 1.25}
        )
        self.assertEqual(
            int_map_h.convert_json({"1": 0.5, "5": 1.25}), {1: 0.5, 5: 1.25}
        )

        bool_map_h = MapTypeHandler(
            SchemaField("key", BooleanTypeHandler(), nullable=False),
            SchemaField("value", IntTypeHandler(pa.int32()), nullable=True),
        )
        self.assertEqual(
            bool_map_h.convert_json({"true": 1, "false": 0}), {True: 1, False: 0}
        )
        self.assertEqual(
            bool_map_h.convert_json([[True, 1], [False, 0]]), {True: 1, False: 0}
        )
        # Standardized ValueError for invalid bool, int, and float string keys
        with self.assertRaisesRegex(ValueError, "Invalid boolean map key"):
            bool_map_h.convert_json({"not_a_bool": 1})
        with self.assertRaises(ValueError):
            int_map_h.convert_json({"not_an_int": 0.5})
        with self.assertRaises(ValueError):
            int_map_h.convert_json([[None, 0.5]])
        with self.assertRaises(ValueError):
            int_map_h.convert_connect({None: 0.5})
        with self.assertRaises(TypeError):
            int_map_h.convert_json("not_a_map")

        # Non-nullable element & metadata preservation
        arr_h = ArrayTypeHandler(
            SchemaField(
                "item",
                StringTypeHandler(),
                nullable=False,
                doc="elem doc",
                logical_name="io.debezium.data.Uuid",
            )
        )
        self.assertFalse(arr_h.arrow_type.value_field.nullable)
        self.assertEqual(
            arr_h.arrow_type.value_field.metadata[b"debezium.logical_type"],
            b"io.debezium.data.Uuid",
        )
        with self.assertRaises(ValueError):
            arr_h.convert_json(["ok", None])

        # Nested field-path error context
        nested_handler = StructTypeHandler(
            (
                SchemaField(
                    "customer",
                    StructTypeHandler((SchemaField("age", IntTypeHandler(pa.int8())),)),
                ),
            )
        )
        with self.assertRaisesRegex(
            TypeError, r"Field 'customer': Field 'age': Expected int"
        ):
            nested_handler.convert_json({"customer": {"age": "not_an_int"}})
        with self.assertRaisesRegex(
            ValueError,
            r"Field 'customer': Field 'age': Integer value 200 out of bounds",
        ):
            nested_handler.convert_json({"customer": {"age": 200}})

    def test_slotted_handlers_have_no_dict_and_support_copy_and_pickle(self) -> None:
        leaf_field = SchemaField("id", IntTypeHandler(pa.int32()), nullable=False)
        all_instances: tuple[Any, ...] = (
            leaf_field,
            BooleanTypeHandler(),
            IntTypeHandler(pa.int16()),
            FloatTypeHandler(pa.float32()),
            StringTypeHandler(),
            BinaryTypeHandler(),
            DecimalTypeHandler(12, 4),
            VariableScaleDecimalHandler(12, 4),
            DateTypeHandler(),
            TimeTypeHandler("ms"),
            TimestampTypeHandler("us", tz="UTC"),
            StructTypeHandler((leaf_field,)),
            ArrayTypeHandler(leaf_field),
            MapTypeHandler(
                SchemaField("key", StringTypeHandler(), nullable=False),
                leaf_field,
            ),
        )
        for obj in all_instances:
            self.assertFalse(
                hasattr(obj, "__dict__"),
                msg=f"{type(obj).__name__} unexpectedly allocated __dict__",
            )
            for cloner in (
                lambda x: pickle.loads(pickle.dumps(x)),
                copy.copy,
                copy.deepcopy,
            ):
                cloned = cloner(obj)
                self.assertEqual(cloned, obj)
                self.assertEqual(cloned.arrow_type, obj.arrow_type)

        gc.collect()
        gc.disable()
        try:
            for _ in range(50):
                IntTypeHandler(pa.int32())
            self.assertEqual(gc.collect(), 0)
        finally:
            gc.enable()


if __name__ == "__main__":
    unittest.main()
