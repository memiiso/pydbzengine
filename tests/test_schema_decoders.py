from __future__ import annotations

import base64
import datetime
import unittest
import uuid
from decimal import Decimal

from pydbzengine.schema.decoders import ValueDecoder
from pydbzengine.schema.models import (
    CanonicalField,
    CanonicalPrimitiveType,
    CanonicalSchema,
    CanonicalType,
)


class TestSchemaDecoders(unittest.TestCase):
    def setUp(self) -> None:
        self.decoder = ValueDecoder(tz_aware=True)

    def test_decode_decimal_base64_positive_and_negative(self) -> None:
        # 12345 with scale 2 = 123.45
        raw_pos = (12345).to_bytes(4, byteorder="big", signed=True)
        b64_pos = base64.b64encode(raw_pos).decode("ascii")
        self.assertEqual(
            self.decoder.decode_decimal(b64_pos, scale=2), Decimal("123.45")
        )

        # -54321 with scale 2 = -543.21
        raw_neg = (-54321).to_bytes(4, byteorder="big", signed=True)
        b64_neg = base64.b64encode(raw_neg).decode("ascii")
        self.assertEqual(
            self.decoder.decode_decimal(b64_neg, scale=2), Decimal("-543.21")
        )

    def test_decode_decimal_scale_zero(self) -> None:
        raw = (999).to_bytes(4, byteorder="big", signed=True)
        b64 = base64.b64encode(raw).decode("ascii")
        self.assertEqual(self.decoder.decode_decimal(b64, scale=0), Decimal("999"))

    def test_decode_decimal_malformed_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            self.decoder.decode_decimal("not_valid_b64_or_number!!@@##", scale=2)

    def test_decode_variable_scale_decimal(self) -> None:
        raw = (123456).to_bytes(4, byteorder="big", signed=True)
        b64 = base64.b64encode(raw).decode("ascii")
        struct_val = {"scale": 3, "value": b64}
        self.assertEqual(
            self.decoder.decode_variable_scale_decimal(struct_val),
            Decimal("123.456"),
        )

    def test_decode_date_from_epoch_days_and_iso(self) -> None:
        # Day 0 = 1970-01-01
        self.assertEqual(self.decoder.decode_date(0), datetime.date(1970, 1, 1))
        # Day 18262 = 2020-01-01
        self.assertEqual(self.decoder.decode_date(18262), datetime.date(2020, 1, 1))
        # ISO string
        self.assertEqual(
            self.decoder.decode_date("2026-09-07"),
            datetime.date(2026, 9, 7),
        )

    def test_decode_time(self) -> None:
        # 3600000 ms = 1 hour = 01:00:00
        self.assertEqual(self.decoder.decode_time(3600000), datetime.time(1, 0, 0))
        # ISO string
        self.assertEqual(self.decoder.decode_time("14:30:00"), datetime.time(14, 30, 0))

    def test_decode_timestamp(self) -> None:
        # 1000 ms = 1970-01-01 00:00:01 UTC
        dt = self.decoder.decode_timestamp(1000)
        self.assertEqual(
            dt, datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc)
        )

        # ISO string
        iso_dt = self.decoder.decode_timestamp("2026-09-07T12:00:00+00:00")
        self.assertEqual(
            iso_dt,
            datetime.datetime(2026, 9, 7, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

    def test_decode_uuid(self) -> None:
        raw_uuid = "12345678-1234-5678-1234-567812345678"
        self.assertEqual(self.decoder.decode_uuid(raw_uuid), uuid.UUID(raw_uuid))

    def test_decode_row_full(self) -> None:
        schema = CanonicalSchema(
            identifier="test.table",
            fields=(
                CanonicalField(
                    name="id",
                    field_type=CanonicalType(primitive=CanonicalPrimitiveType.INT64),
                ),
                CanonicalField(
                    name="price",
                    field_type=CanonicalType(
                        primitive=CanonicalPrimitiveType.DECIMAL, precision=10, scale=2
                    ),
                ),
                CanonicalField(
                    name="created_date",
                    field_type=CanonicalType(primitive=CanonicalPrimitiveType.DATE),
                ),
            ),
        )
        raw_bytes = (1999).to_bytes(4, byteorder="big", signed=True)
        raw_row = {
            "id": 1,
            "price": base64.b64encode(raw_bytes).decode("ascii"),
            "created_date": 18262,
            "extra_meta": "untouched",
        }
        decoded = self.decoder.decode_row(raw_row, schema)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["id"], 1)
        self.assertEqual(decoded["price"], Decimal("19.99"))
        self.assertEqual(decoded["created_date"], datetime.date(2020, 1, 1))
        self.assertEqual(decoded["extra_meta"], "untouched")

    def test_decode_decimal_direct_instance(self) -> None:
        dec = Decimal("42.75")
        self.assertEqual(self.decoder.decode_decimal(dec, scale=2), dec)

    def test_decode_decimal_numeric_strings_not_corrupted_by_base64(self) -> None:
        # "1000" has length 4, which is valid base64. It must NOT be treated as base64 if scale=0!
        self.assertEqual(self.decoder.decode_decimal("1000", scale=0), Decimal("1000"))
        # Numbers with decimal points must be parsed directly
        self.assertEqual(
            self.decoder.decode_decimal("123.456", scale=3), Decimal("123.456")
        )
        self.assertEqual(
            self.decoder.decode_decimal("-42.5", scale=1), Decimal("-42.5")
        )

    def test_decode_decimal_bytearray_and_memoryview(self) -> None:
        raw = (5000).to_bytes(4, byteorder="big", signed=True)
        self.assertEqual(
            self.decoder.decode_decimal(bytearray(raw), scale=2), Decimal("50.00")
        )
        self.assertEqual(
            self.decoder.decode_decimal(memoryview(raw), scale=2), Decimal("50.00")
        )

    def test_decode_date_with_datetime_and_trailing_z(self) -> None:
        dt = datetime.datetime(2026, 9, 7, 15, 30, 0)
        self.assertEqual(self.decoder.decode_date(dt), datetime.date(2026, 9, 7))
        # Debezium IsoDate format "YYYY-MM-DDZ"
        self.assertEqual(
            self.decoder.decode_date("2024-05-05Z"), datetime.date(2024, 5, 5)
        )
        # Full ISO timestamp string
        self.assertEqual(
            self.decoder.decode_date("2024-05-05T12:00:00Z"), datetime.date(2024, 5, 5)
        )

    def test_decode_time_with_precisions_and_trailing_z(self) -> None:
        # Milliseconds (precision=3): 3600000 ms = 01:00:00
        self.assertEqual(
            self.decoder.decode_time(3600000, precision=3), datetime.time(1, 0, 0)
        )
        # Microseconds (precision=6): 3600000000 us = 01:00:00
        self.assertEqual(
            self.decoder.decode_time(3600000000, precision=6), datetime.time(1, 0, 0)
        )
        # Nanoseconds (precision=9): 3600000000000 ns = 01:00:00
        self.assertEqual(
            self.decoder.decode_time(3600000000000, precision=9), datetime.time(1, 0, 0)
        )
        # Trailing 'Z' in Python 3.10
        self.assertEqual(
            self.decoder.decode_time("04:05:11Z"),
            datetime.time(4, 5, 11, tzinfo=datetime.timezone.utc),
        )

    def test_decode_timestamp_with_precisions_and_negative_epoch(self) -> None:
        # Pre-1970 negative epoch offset (-1000 ms = 1969-12-31 23:59:59 UTC)
        dt_neg = self.decoder.decode_timestamp(-1000, precision=3)
        self.assertEqual(
            dt_neg,
            datetime.datetime(1969, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc),
        )

        # Nanosecond precision (precision=9)
        dt_ns = self.decoder.decode_timestamp(1_000_000_000, precision=9)
        self.assertEqual(
            dt_ns, datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc)
        )

        # ISO string with 'Z'
        dt_iso = self.decoder.decode_timestamp("2026-09-07T12:00:00Z")
        self.assertEqual(
            dt_iso,
            datetime.datetime(2026, 9, 7, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

    def test_decode_uuid_binary_16_bytes(self) -> None:
        expected = uuid.UUID("12345678-1234-5678-1234-567812345678")
        self.assertEqual(self.decoder.decode_uuid(expected.bytes), expected)
        self.assertEqual(self.decoder.decode_uuid(bytearray(expected.bytes)), expected)

    def test_decode_binary_buffers_and_integer_lists(self) -> None:
        expected = b"hello world"
        self.assertEqual(self.decoder.decode_binary(bytearray(expected)), expected)
        self.assertEqual(self.decoder.decode_binary(memoryview(expected)), expected)
        self.assertEqual(
            self.decoder.decode_binary([104, 101, 108, 108, 111]), b"hello"
        )

    def test_decode_field_string_with_json_dict(self) -> None:
        field_type = CanonicalType(primitive=CanonicalPrimitiveType.STRING)
        data = {"key": "value", "num": 123}
        self.assertEqual(
            self.decoder.decode_field(data, field_type), '{"key": "value", "num": 123}'
        )

    def test_decode_invalid_date_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            self.decoder.decode_date("not-a-date")
        with self.assertRaises(ValueError):
            self.decoder.decode_date([1, 2, 3])  # type: ignore[arg-type]

    def test_decode_invalid_time_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            self.decoder.decode_time("invalid-time-format")
        with self.assertRaises(ValueError):
            self.decoder.decode_time(object())  # type: ignore[arg-type]

    def test_decode_invalid_timestamp_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            self.decoder.decode_timestamp("not-an-iso-ts")
        with self.assertRaises(ValueError):
            self.decoder.decode_timestamp(object())  # type: ignore[arg-type]

    def test_decode_boolean_strings(self) -> None:
        field_type = CanonicalType(primitive=CanonicalPrimitiveType.BOOLEAN)
        self.assertTrue(self.decoder.decode_field("true", field_type))
        self.assertTrue(self.decoder.decode_field("1", field_type))
        self.assertFalse(self.decoder.decode_field("false", field_type))
        self.assertFalse(self.decoder.decode_field("0", field_type))

    def test_decode_invalid_integers_and_floats_fail_fast(self) -> None:
        int_type = CanonicalType(primitive=CanonicalPrimitiveType.INT32)
        with self.assertRaises(ValueError):
            self.decoder.decode_field("not-an-int", int_type)

        float_type = CanonicalType(primitive=CanonicalPrimitiveType.FLOAT)
        with self.assertRaises(ValueError):
            self.decoder.decode_field("not-a-float", float_type)

    def test_decode_empty_decimal_string_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            self.decoder.decode_decimal("   ", scale=2)

    def test_decode_variable_scale_missing_value_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            self.decoder.decode_variable_scale_decimal({"scale": 2})

    def test_decode_invalid_uuid_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            self.decoder.decode_uuid("not-a-valid-uuid")

    def test_decode_invalid_binary_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            self.decoder.decode_binary(12345)  # type: ignore[arg-type]

    def test_decode_nested_struct_list_map(self) -> None:
        struct_type = CanonicalType(
            primitive=CanonicalPrimitiveType.STRUCT,
            fields=(
                CanonicalField(
                    name="id",
                    field_type=CanonicalType(primitive=CanonicalPrimitiveType.INT64),
                ),
                CanonicalField(
                    name="tags",
                    field_type=CanonicalType(
                        primitive=CanonicalPrimitiveType.LIST,
                        element_type=CanonicalType(
                            primitive=CanonicalPrimitiveType.STRING
                        ),
                    ),
                ),
            ),
        )
        data = {"id": "100", "tags": [1, 2, 3]}
        decoded = self.decoder.decode_field(data, struct_type)
        self.assertEqual(decoded["id"], 100)
        self.assertEqual(decoded["tags"], ["1", "2", "3"])


if __name__ == "__main__":
    unittest.main()
