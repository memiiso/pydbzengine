from __future__ import annotations

import base64
import copy
import datetime
import gc
import json
import pickle
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from pydbzengine import DebeziumConfig, EventFormat
from pydbzengine.schema import (
    ArrayTypeHandler,
    BinaryTypeHandler,
    BooleanTypeHandler,
    ChangeBatch,
    ChangeOp,
    ConnectEventConverter,
    DateTypeHandler,
    DecimalTypeHandler,
    DenseVectorHandler,
    EventKind,
    FieldSpec,
    FloatTypeHandler,
    GeometryTypeHandler,
    IntTypeHandler,
    JsonEventConverter,
    MapTypeHandler,
    PointTypeHandler,
    SchemaAwareChangeHandler,
    SchemaDiff,
    SchemaField,
    SchemalessJsonEventConverter,
    SparseVectorHandler,
    StringTypeHandler,
    TableBatch,
    TableSchema,
    TemporalIntervalHandler,
    TimestampTypeHandler,
    TimeTypeHandler,
    TypeRegistry,
    VariableScaleDecimalHandler,
)
from tests.support import MockChangeEvent


def _wrap_json_key(key: dict[str, Any] | str | None) -> str:
    if isinstance(key, str):
        return key
    key_dict = key if key is not None else {"id": 1}
    if "schema" in key_dict and "payload" in key_dict:
        return json.dumps(key_dict)
    return json.dumps(
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "type": "int64" if isinstance(v, int) else "string",
                        "field": k,
                        "optional": False,
                    }
                    for k, v in key_dict.items()
                ],
            },
            "payload": key_dict,
        }
    )


def make_flat_event(
    dest: str,
    fields: list[dict[str, Any]],
    payload: dict[str, Any] | None,
    key: dict[str, Any] | str | None = None,
) -> MockChangeEvent:
    val_json = {
        "schema": {
            "type": "struct",
            "name": f"{dest}.Value",
            "fields": fields,
        },
        "payload": payload,
    }
    return MockChangeEvent(
        key=_wrap_json_key(key),
        value=json.dumps(val_json),
        destination=dest,
    )


def make_envelope_event(
    dest: str,
    fields: list[dict[str, Any]],
    after: dict[str, Any] | None = None,
    before: dict[str, Any] | None = None,
    op: str = "c",
    key: dict[str, Any] | str | None = None,
) -> MockChangeEvent:
    payload_fields = [
        {"type": "struct", "fields": fields, "field": "before", "optional": True},
        {"type": "struct", "fields": fields, "field": "after", "optional": True},
        {"type": "string", "field": "op", "optional": False},
        {"type": "int64", "field": "ts_ms", "optional": True},
    ]
    val_json = {
        "schema": {
            "type": "struct",
            "name": f"{dest}.Envelope",
            "fields": payload_fields,
        },
        "payload": {
            "before": before,
            "after": after,
            "op": op,
            "ts_ms": 1620000000000,
        },
    }
    return MockChangeEvent(
        key=_wrap_json_key(key),
        value=json.dumps(val_json),
        destination=dest,
    )


class TestEventConverterAndTypeHandlers(unittest.TestCase):
    def setUp(self) -> None:
        self.converter = JsonEventConverter()

    def test_flattened_event_extraction_and_to_arrow(self) -> None:
        salary_b64 = base64.b64encode(
            (19995).to_bytes(4, byteorder="big", signed=True)
        ).decode("ascii")
        fields = [
            {
                "type": "int32",
                "optional": False,
                "field": "id",
                "doc": "Customer PK",
            },
            {"type": "string", "optional": True, "field": "name"},
            {
                "type": "int32",
                "optional": True,
                "name": "io.debezium.time.Date",
                "field": "birth_date",
            },
            {
                "type": "int64",
                "optional": True,
                "name": "io.debezium.time.MicroTimestamp",
                "field": "created_at",
            },
            {
                "type": "string",
                "optional": True,
                "name": "io.debezium.time.ZonedTimestamp",
                "field": "updated_at",
            },
            {
                "type": "bytes",
                "optional": True,
                "name": "org.apache.kafka.connect.data.Decimal",
                "parameters": {
                    "scale": "2",
                    "connect.decimal.precision": "10",
                },
                "field": "salary",
            },
            {
                "type": "string",
                "optional": True,
                "name": "io.debezium.data.Uuid",
                "field": "account_id",
            },
        ]
        rec = make_flat_event(
            dest="inventory.customers",
            fields=fields,
            payload={
                "id": 1001,
                "name": "Sally",
                "birth_date": 18262,
                "created_at": 1620000000000000,
                "updated_at": "2024-05-05T14:30:00Z",
                "salary": salary_b64,
                "account_id": "c9a646d3-9c61-4073-8946-f94d93d3b762",
            },
            key={
                "schema": {
                    "type": "struct",
                    "fields": [{"type": "int32", "field": "id", "optional": False}],
                },
                "payload": {"id": 1001},
            },
        )

        batch = self.converter.convert([rec])
        self.assertEqual(len(batch.records), 1)
        record = batch.records[0]
        table_schema, row = record.schema, record.row
        assert table_schema is not None
        assert row is not None
        self.assertEqual(table_schema.destination, "inventory.customers")
        self.assertEqual(table_schema.primary_keys, ("id",))
        self.assertIsNotNone(table_schema.key_schema)
        assert table_schema.key_schema is not None
        self.assertEqual(table_schema.key_schema.field("id").type, pa.int32())
        self.assertEqual(record.key, {"id": 1001})
        self.assertEqual(row["salary"], Decimal("199.95"))
        self.assertEqual(record.data, row)

        pa_schema = table_schema.arrow_schema
        self.assertEqual(pa_schema.field("id").type, pa.int32())
        self.assertFalse(pa_schema.field("id").nullable)
        self.assertEqual(pa_schema.field("id").metadata[b"doc"], b"Customer PK")
        self.assertEqual(pa_schema.field("birth_date").type, pa.date32())
        self.assertEqual(pa_schema.field("created_at").type, pa.timestamp("us"))
        self.assertEqual(
            pa_schema.field("updated_at").type, pa.timestamp("us", tz="UTC")
        )
        self.assertEqual(pa_schema.field("salary").type, pa.decimal128(10, 2))
        self.assertEqual(pa_schema.field("account_id").type, pa.string())

        pa_table = batch.table_batches()[0].to_arrow()
        rows = pa_table.to_pylist()
        self.assertEqual(len(rows), 1)
        out_row = rows[0]
        self.assertEqual(out_row["id"], 1001)
        self.assertEqual(out_row["name"], "Sally")
        self.assertEqual(out_row["birth_date"], datetime.date(2020, 1, 1))
        self.assertEqual(out_row["created_at"], datetime.datetime(2021, 5, 3, 0, 0))
        self.assertEqual(
            out_row["updated_at"],
            datetime.datetime(2024, 5, 5, 14, 30, 0, tzinfo=datetime.timezone.utc),
        )
        self.assertEqual(out_row["salary"], Decimal("199.95"))
        self.assertEqual(out_row["account_id"], "c9a646d3-9c61-4073-8946-f94d93d3b762")

    def test_pure_envelope_conversion_preserves_before_after_op(self) -> None:
        fields = [
            {"type": "int32", "optional": False, "field": "id"},
            {"type": "string", "optional": True, "field": "name"},
        ]
        create_rec = make_envelope_event(
            dest="inventory.customers",
            fields=fields,
            after={"id": 1, "name": "Alice"},
            op="c",
        )
        delete_rec = make_envelope_event(
            dest="inventory.customers",
            fields=fields,
            after=None,
            before={"id": 1, "name": "Alice"},
            op="d",
        )

        batch = self.converter.convert([create_rec, delete_rec])
        tb = batch.table_batches()[0]
        tbl = tb.to_arrow()
        self.assertEqual(tbl.num_rows, 2)
        self.assertEqual(tbl.schema.names, ["before", "after", "op", "ts_ms"])
        self.assertEqual(
            tbl.to_pylist(),
            [
                {
                    "before": None,
                    "after": {"id": 1, "name": "Alice"},
                    "op": "c",
                    "ts_ms": 1620000000000,
                },
                {
                    "before": {"id": 1, "name": "Alice"},
                    "after": None,
                    "op": "d",
                    "ts_ms": 1620000000000,
                },
            ],
        )

        # Also verify extract_rows=True on envelope batch
        row_tbl = tb.to_arrow(extract_rows=True)
        self.assertEqual(row_tbl.schema.names, ["id", "name"])
        self.assertEqual(
            row_tbl.to_pylist(),
            [{"id": 1, "name": "Alice"}, {"id": 1, "name": "Alice"}],
        )

    def test_to_arrow_batches_and_to_arrow_mid_batch_evolution(self) -> None:
        v1_fields = [
            {"type": "int32", "optional": False, "field": "id"},
            {"type": "int32", "optional": True, "field": "amount"},
        ]
        v2_fields = [
            {"type": "int32", "optional": False, "field": "id"},
            {"type": "int64", "optional": True, "field": "amount"},
            {"type": "string", "optional": True, "field": "discount_code"},
        ]
        records = [
            make_flat_event("orders", v1_fields, {"id": 1, "amount": 100}),
            make_flat_event("orders", v1_fields, {"id": 2, "amount": 200}),
            make_flat_event(
                "orders",
                v2_fields,
                {"id": 3, "amount": 300, "discount_code": "SAVE10"},
            ),
        ]

        batch = self.converter.convert(records)
        batches = batch.table_batches()
        self.assertEqual(len(batches), 2)
        b1, b2 = batches[0], batches[1]
        t1, t2 = b1.to_arrow(), b2.to_arrow()
        self.assertEqual(t1.num_rows, 2)
        self.assertEqual(b1.schema.arrow_schema.field("amount").type, pa.int32())
        self.assertEqual(t2.num_rows, 1)
        self.assertEqual(b2.schema.arrow_schema.field("amount").type, pa.int64())
        self.assertIn("discount_code", b2.schema.names)

        # Additive unification via TableBatch.to_arrow(target_schema=...)
        unified_schema = pa.unify_schemas(
            [b1.schema.arrow_schema, b2.schema.arrow_schema],
            promote_options="permissive",
        )
        unified_table = pa.concat_tables(
            [
                b1.to_arrow(target_schema=unified_schema),
                b2.to_arrow(target_schema=unified_schema),
            ]
        )
        self.assertEqual(unified_table.num_rows, 3)
        self.assertEqual(unified_table.schema.field("amount").type, pa.int64())
        self.assertEqual(
            unified_table.to_pylist(),
            [
                {"id": 1, "amount": 100, "discount_code": None},
                {"id": 2, "amount": 200, "discount_code": None},
                {"id": 3, "amount": 300, "discount_code": "SAVE10"},
            ],
        )

    def test_all_debezium_logical_types_and_complex_types(self) -> None:
        vardec_b64 = base64.b64encode(
            (12345).to_bytes(4, byteorder="big", signed=True)
        ).decode("ascii")
        fields: list[dict[str, Any]] = [
            {
                "type": "string",
                "name": "io.debezium.time.IsoDate",
                "field": "f_isodate",
            },
            {
                "type": "string",
                "name": "io.debezium.time.IsoTime",
                "field": "f_isotime",
            },
            {
                "type": "int64",
                "name": "io.debezium.time.MicroTime",
                "field": "f_microtime",
            },
            {
                "type": "int64",
                "name": "io.debezium.time.NanoTime",
                "field": "f_nanotime",
            },
            {
                "type": "int32",
                "name": "io.debezium.time.Time",
                "field": "f_time",
            },
            {
                "type": "int64",
                "name": "io.debezium.time.Timestamp",
                "field": "f_ts_ms",
            },
            {
                "type": "int64",
                "name": "io.debezium.time.NanoTimestamp",
                "field": "f_ts_ns",
            },
            {
                "type": "string",
                "name": "io.debezium.time.IsoTimestamp",
                "field": "f_isots",
            },
            {"type": "int32", "name": "io.debezium.time.Year", "field": "f_year"},
            {
                "type": "int64",
                "name": "io.debezium.time.MicroDuration",
                "field": "f_duration",
            },
            {
                "type": "bytes",
                "name": "io.debezium.data.Bits",
                "field": "f_bits",
            },
            {
                "type": "struct",
                "name": "io.debezium.data.VariableScaleDecimal",
                "fields": [
                    {"type": "int32", "field": "scale"},
                    {"type": "bytes", "field": "value"},
                ],
                "field": "f_vardec",
            },
            {
                "type": "map",
                "name": "io.debezium.data.Hstore",
                "field": "f_hstore",
            },
            {
                "type": "struct",
                "name": "io.debezium.data.geometry.Point",
                "field": "f_point",
            },
            {
                "type": "array",
                "items": {"type": "string"},
                "field": "f_tags",
            },
        ]
        rec = make_flat_event(
            dest="test.logical_types",
            fields=fields,
            payload={
                "f_isodate": "2024-05-05Z",
                "f_isotime": "04:05:11Z",
                "f_microtime": 14711000000,
                "f_nanotime": 14711000000000,
                "f_time": 14711000,
                "f_ts_ms": 1620000000000,
                "f_ts_ns": 1620000000000000000,
                "f_isots": "2024-05-05T14:30:00.123456789",
                "f_year": 2024,
                "f_duration": 86400000000,
                "f_bits": "AQ==",
                "f_vardec": {"scale": 2, "value": vardec_b64},
                "f_hstore": {"k1": "v1"},
                "f_point": {"x": 1.5, "y": 2.5, "wkb": "AQ==", "srid": 4326},
                "f_tags": ["a", "b"],
            },
        )
        batch = self.converter.convert([rec])
        pa_table = batch.table_batches()[0].to_arrow()

        self.assertEqual(pa_table.schema.field("f_time").type, pa.time32("ms"))
        self.assertEqual(pa_table.schema.field("f_microtime").type, pa.time64("us"))
        self.assertEqual(pa_table.schema.field("f_nanotime").type, pa.time64("ns"))
        self.assertEqual(pa_table.schema.field("f_ts_ms").type, pa.timestamp("ms"))
        self.assertEqual(pa_table.schema.field("f_ts_ns").type, pa.timestamp("ns"))

        row = pa_table.to_pylist()[0]
        self.assertEqual(row["f_isodate"], datetime.date(2024, 5, 5))
        self.assertEqual(row["f_isotime"], datetime.time(4, 5, 11))
        self.assertEqual(row["f_microtime"], datetime.time(4, 5, 11))
        self.assertEqual(row["f_nanotime"], datetime.time(4, 5, 11))
        self.assertEqual(row["f_time"], datetime.time(4, 5, 11))
        self.assertEqual(row["f_ts_ms"], datetime.datetime(2021, 5, 3, 0, 0))
        self.assertEqual(row["f_ts_ns"], datetime.datetime(2021, 5, 3, 0, 0))
        self.assertEqual(
            row["f_isots"], datetime.datetime(2024, 5, 5, 14, 30, 0, 123456)
        )
        self.assertEqual(row["f_year"], 2024)
        self.assertEqual(row["f_duration"], 86400000000)
        self.assertEqual(row["f_bits"], b"\x01")
        self.assertEqual(row["f_vardec"], Decimal("123.45"))
        self.assertEqual(row["f_hstore"], [("k1", "v1")])
        self.assertEqual(
            row["f_point"], {"x": 1.5, "y": 2.5, "wkb": b"\x01", "srid": 4326}
        )
        self.assertEqual(row["f_tags"], ["a", "b"])

    def test_java_connect_schema_and_struct_support(self) -> None:
        class MockConnectType:
            def __init__(self, name: str) -> None:
                self._name = name

            def name(self) -> str:
                return self._name

        class MockFieldSchema:
            def __init__(
                self,
                type_name: str,
                name: str | None = None,
                parameters: dict[str, str] | None = None,
            ) -> None:
                self._type = MockConnectType(type_name)
                self._name = name
                self._params = parameters or {}

            def type(self) -> MockConnectType:
                return self._type

            def name(self) -> str | None:
                return self._name

            def isOptional(self) -> bool:
                return True

            def doc(self) -> str | None:
                return None

            def parameters(self) -> dict[str, str]:
                return self._params

        class MockField:
            def __init__(self, name: str, schema: MockFieldSchema) -> None:
                self._name = name
                self._schema = schema

            def name(self) -> str:
                return self._name

            def schema(self) -> MockFieldSchema:
                return self._schema

        class MockNestedSchema(MockFieldSchema):
            def __init__(self) -> None:
                super().__init__("STRUCT")

            def fields(self) -> list[MockField]:
                return [
                    MockField("nested_str", MockFieldSchema("STRING")),
                    MockField("nested_long", MockFieldSchema("INT64")),
                ]

        class MockNestedStruct:
            def schema(self) -> MockNestedSchema:
                return MockNestedSchema()

            def get(self, name: str) -> Any:
                return {"nested_str": "hello", "nested_long": 999}[name]

        class MockValueSchema(MockFieldSchema):
            def __init__(self) -> None:
                super().__init__("STRUCT")

            def fields(self) -> list[MockField]:
                return [
                    MockField("id", MockFieldSchema("INT64")),
                    MockField(
                        "amount",
                        MockFieldSchema(
                            "BYTES",
                            "org.apache.kafka.connect.data.Decimal",
                            {"scale": "2", "connect.decimal.precision": "10"},
                        ),
                    ),
                    MockField("col_default", MockFieldSchema("STRING")),
                    MockField("col_struct", MockNestedSchema()),
                ]

        class MockKeySchema(MockFieldSchema):
            def __init__(self) -> None:
                super().__init__("STRUCT")

            def fields(self) -> list[MockField]:
                return [MockField("id", MockFieldSchema("INT64"))]

        class MockKeyStruct:
            def getWithoutDefault(self, name: str) -> Any:
                return {"id": 7}[name]

        class MockStruct:
            def get(self, name: str) -> Any:
                return {
                    "id": 7,
                    "amount": Decimal("12.50"),
                    "col_default": "default_substituted",
                    "col_struct": MockNestedStruct(),
                }[name]

            def getWithoutDefault(self, name: str) -> Any:
                return {
                    "id": 7,
                    "amount": Decimal("12.50"),
                    "col_default": None,
                    "col_struct": MockNestedStruct(),
                }[name]

        val_schema = MockValueSchema()
        key_schema = MockKeySchema()

        class MockSourceRecord:
            def valueSchema(self) -> MockValueSchema:
                return val_schema

            def keySchema(self) -> MockKeySchema:
                return key_schema

            def value(self) -> MockStruct:
                return MockStruct()

            def key(self) -> MockKeyStruct:
                return MockKeyStruct()

        source_rec = MockSourceRecord()
        rec = MockChangeEvent(
            key=source_rec, value=source_rec, destination="connect.orders"
        )
        connect_converter = ConnectEventConverter()
        self.assertEqual(connect_converter.format, EventFormat.CONNECT)
        batch = connect_converter.convert([rec, rec])
        self.assertEqual(len(batch.table_batches()), 1)
        tb = batch.table_batches()[0]
        schema, rows = tb.schema, tb.rows
        self.assertEqual(schema.primary_keys, ("id",))
        self.assertEqual(batch.records[0].key, {"id": 7})
        self.assertEqual(
            rows,
            [
                {
                    "id": 7,
                    "amount": Decimal("12.50"),
                    "col_default": None,
                    "col_struct": {"nested_str": "hello", "nested_long": 999},
                },
                {
                    "id": 7,
                    "amount": Decimal("12.50"),
                    "col_default": None,
                    "col_struct": {"nested_str": "hello", "nested_long": 999},
                },
            ],
        )

    def test_type_handlers_convert_json_and_convert_connect_all_families(self) -> None:
        # 1. Explicit Primitive Domain Handlers
        int_h = IntTypeHandler(pa.int64())
        self.assertEqual(int_h.convert_json(42), 42)
        with self.assertRaises(TypeError):
            int_h.convert_json(True)

        class MockJavaBoolean:
            def booleanValue(self) -> bool:
                return True

        bool_h = BooleanTypeHandler()
        self.assertTrue(bool_h.convert_connect(MockJavaBoolean()))

        float_h = FloatTypeHandler(pa.float64())
        self.assertEqual(float_h.convert_json(3.5), 3.5)

        str_h = StringTypeHandler()
        self.assertEqual(str_h.convert_json("hello"), "hello")

        interval_h = TemporalIntervalHandler(pa.int64())
        self.assertEqual(interval_h.convert_json(86400000000), 86400000000)

        # 2. BinaryTypeHandler
        bin_h = BinaryTypeHandler()
        self.assertEqual(bin_h.convert_json("0xDEADBEEF"), b"\xde\xad\xbe\xef")

        class MockByteBuffer:
            def array(self) -> list[int]:
                return [-34, -83, -66, -17]

        self.assertEqual(bin_h.convert_connect(MockByteBuffer()), b"\xde\xad\xbe\xef")

        # 3. DecimalTypeHandler & VariableScaleDecimalHandler
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

        class MockBigDecimal:
            def toPlainString(self) -> str:
                return "99.956"

        dec2_h = DecimalTypeHandler(precision=10, scale=2)
        self.assertEqual(dec2_h.convert_connect(MockBigDecimal()), Decimal("99.96"))

        vardec_h = VariableScaleDecimalHandler(precision=38, scale=10)
        self.assertEqual(
            vardec_h.convert_json({"scale": 10, "value": b64_val}),
            Decimal("1234567890123456789012345678.9012345678"),
        )
        self.assertIsNone(vardec_h.convert_json({"scale": 2, "value": None}))

        class MockVarDecStruct:
            def getWithoutDefault(self, name: str) -> Any:
                return {"scale": 2, "value": (12345).to_bytes(4, "big", signed=True)}[
                    name
                ]

        self.assertEqual(
            vardec_h.convert_connect(MockVarDecStruct()), Decimal("123.45")
        )

        # 4. DateTypeHandler, TimeTypeHandler, TimestampTypeHandler
        class MockJavaUtilDate:
            def getTime(self) -> int:
                return 19118 * 86_400_000

        date_h = DateTypeHandler()
        self.assertEqual(date_h.convert_connect(MockJavaUtilDate()), 19118)
        self.assertEqual(
            date_h.convert_json("2024-05-05T14:30:00Z"), datetime.date(2024, 5, 5)
        )

        time_h = TimeTypeHandler(unit="ms")
        self.assertEqual(time_h.convert_connect(MockJavaUtilDate()), 19118 * 86_400_000)

        ts_utc_h = TimestampTypeHandler(unit="us", tz="UTC")
        self.assertEqual(
            ts_utc_h.convert_json("2026-09-07T12:00:00Z"),
            datetime.datetime(2026, 9, 7, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

        # 5. ArrayTypeHandler & MapTypeHandler
        arr_h = ArrayTypeHandler(SchemaField("item", DecimalTypeHandler(10, 2)))
        self.assertEqual(arr_h.convert_json(["1.25", None]), [Decimal("1.25"), None])
        map_h = MapTypeHandler(
            SchemaField("key", StringTypeHandler(), nullable=False),
            SchemaField("value", IntTypeHandler(pa.int32()), nullable=True),
        )
        self.assertEqual(map_h.convert_json({"a": 1, "b": None}), {"a": 1, "b": None})

        with self.assertRaises(ValueError):
            IntTypeHandler(pa.binary())
        with self.assertRaises(ValueError):
            DecimalTypeHandler.decode_json("   ", scale=2)
        with self.assertRaises(TypeError):
            DecimalTypeHandler.decode_json(object(), scale=2)
        with self.assertRaises(ValueError):
            vardec_h.convert_json({"scale": 2})
        with self.assertRaises(TypeError):
            vardec_h.convert_json("not_a_mapping")

    def test_strict_converter_errors_on_unexpected_inputs(self) -> None:
        with self.assertRaises(ValueError):
            self.converter.convert(
                [MockChangeEvent(key=None, value="{}", destination="")]
            )

        with self.assertRaises(ValueError):
            self.converter.convert(
                [
                    MockChangeEvent(
                        key="{}", value='{"schema": {}, "payload": {}}', destination="t"
                    )
                ]
            )

        with self.assertRaises(ValueError):
            self.converter.convert(
                [MockChangeEvent(key=None, value='{"id": 1}', destination="t")]
            )

        with self.assertRaises(ValueError):
            self.converter.convert(
                [MockChangeEvent(key=None, value="[1, 2, 3]", destination="t")]
            )

        schemaless_conv = SchemalessJsonEventConverter()
        with self.assertRaises(ValueError):
            schemaless_conv.convert(
                [
                    MockChangeEvent(
                        key="plain_string_key", value='{"id": 1}', destination="t"
                    )
                ]
            )
        with self.assertRaises(ValueError):
            schemaless_conv.convert(
                [MockChangeEvent(key="123", value='{"id": 1}', destination="t")]
            )

        with self.assertRaises(ValueError):
            self.converter.compile_json_schema(
                "t",
                {"type": "struct", "fields": [{"type": "unknown_type", "field": "x"}]},
            )

        with self.assertRaises(ValueError):
            self.converter.compile_json_schema(
                "t",
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "bytes",
                            "name": "org.apache.kafka.connect.data.Decimal",
                            "parameters": {
                                "scale": "2",
                                "connect.decimal.precision": "50",
                            },
                            "field": "x",
                        }
                    ],
                },
            )

        # Map key cannot be nested type
        with self.assertRaises(ValueError):
            self.converter.compile_json_schema(
                "inventory.bad_map",
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "map",
                            "field": "m",
                            "keys": {"type": "array", "items": {"type": "string"}},
                            "values": {"type": "int32"},
                        }
                    ],
                },
            )

        # Array / Map field cannot be part of primary key
        with self.assertRaises(ValueError):
            self.converter.compile_json_schema(
                "inventory.bad_pk",
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "array",
                            "field": "tags",
                            "items": {"type": "string"},
                        }
                    ],
                },
                primary_keys=("tags",),
            )

    def test_schema_change_handler_lifecycle_and_parquet_writing(self) -> None:
        class LakehouseParquetHandler(SchemaAwareChangeHandler):
            def __init__(self, base_dir: Path) -> None:
                super().__init__()
                self.base_dir = base_dir
                self.target_schemas: dict[str, pa.Schema] = {}
                self.written_files: list[Path] = []
                self.created_tables: list[str] = []
                self.evolutions: list[tuple[str, SchemaDiff]] = []

            def on_table_created(self, schema: TableSchema) -> None:
                self.created_tables.append(schema.destination)
                self.target_schemas[schema.destination] = schema.arrow_schema

            def on_schema_evolved(
                self,
                new_schema: TableSchema,
                old_schema: TableSchema,
                diff: SchemaDiff,
            ) -> None:
                self.evolutions.append((new_schema.destination, diff))
                self.target_schemas[new_schema.destination] = pa.unify_schemas(
                    [
                        self.target_schemas[new_schema.destination],
                        new_schema.arrow_schema,
                    ],
                    promote_options="permissive",
                )

            def handle_table_batch(self, batch: TableBatch) -> None:
                dest = batch.destination
                aligned = batch.to_arrow(target_schema=self.target_schemas[dest])
                out_file = (
                    self.base_dir
                    / f"{batch.schema.table_name}_{len(self.written_files)}.parquet"
                )
                pq.write_table(aligned, out_file)
                self.written_files.append(out_file)

        with tempfile.TemporaryDirectory() as tmpdir:
            handler = LakehouseParquetHandler(Path(tmpdir))
            handler.configure(DebeziumConfig({}))

            price_b64 = base64.b64encode(
                (4999).to_bytes(4, byteorder="big", signed=True)
            ).decode("ascii")
            v1_fields = [
                {"type": "int32", "optional": False, "field": "id"},
                {"type": "string", "optional": True, "field": "legacy_col"},
                {
                    "type": "bytes",
                    "optional": True,
                    "name": "org.apache.kafka.connect.data.Decimal",
                    "parameters": {
                        "scale": "2",
                        "connect.decimal.precision": "10",
                    },
                    "field": "price",
                },
            ]
            v2_fields = [
                {"type": "int64", "optional": False, "field": "id"},
                {
                    "type": "bytes",
                    "optional": True,
                    "name": "org.apache.kafka.connect.data.Decimal",
                    "parameters": {
                        "scale": "2",
                        "connect.decimal.precision": "10",
                    },
                    "field": "price",
                },
                {"type": "string", "optional": True, "field": "email"},
            ]

            rec1 = make_flat_event(
                "inventory.orders",
                v1_fields,
                {"id": 1, "legacy_col": "old", "price": price_b64},
                key={"id": 1},
            )
            rec2 = make_flat_event(
                "inventory.orders",
                v2_fields,
                {"id": 2, "price": price_b64, "email": "alice@example.com"},
                key={"id": 2},
            )
            ts_rec = MockChangeEvent(
                key=_wrap_json_key({"id": 99}),
                value=None,
                destination="inventory.orders",
            )

            handler.handleJsonBatch([rec1, rec2, ts_rec])

            self.assertEqual(handler.created_tables, ["inventory.orders"])
            self.assertEqual(len(handler.evolutions), 1)
            dest, diff = handler.evolutions[0]
            self.assertEqual(dest, "inventory.orders")
            self.assertTrue(diff.has_changes)
            self.assertEqual([f.name for f in diff.added_fields], ["email"])
            self.assertEqual([f.name for f in diff.removed_fields], ["legacy_col"])
            self.assertEqual(len(diff.changed_fields), 1)
            self.assertIsNotNone(handler.get_schema("inventory.orders"))
            self.assertIn("inventory.orders", handler.schemas)

            self.assertEqual(len(handler.written_files), 2)
            b2_rows = pq.read_table(handler.written_files[1]).to_pylist()
            self.assertEqual(
                b2_rows,
                [
                    {
                        "id": 2,
                        "legacy_col": None,
                        "price": Decimal("49.99"),
                        "email": "alice@example.com",
                    }
                ],
            )

    def test_predicate_helpers_heartbeats_ddl_and_tombstones(self) -> None:
        heartbeat_rec = MockChangeEvent(
            key=_wrap_json_key({"id": 1}),
            value=json.dumps(
                {
                    "schema": {
                        "type": "struct",
                        "fields": [{"type": "int64", "field": "ts_ms"}],
                    },
                    "payload": {"ts_ms": 1620000000000},
                }
            ),
            destination="__debezium-heartbeat.postgres",
        )
        ddl_rec = MockChangeEvent(
            key=_wrap_json_key({"databaseName": "inventory"}),
            value=json.dumps(
                {
                    "schema": {
                        "type": "struct",
                        "fields": [
                            {"type": "string", "field": "ddl"},
                            {"type": "string", "field": "databaseName"},
                            {"type": "string", "field": "tableChanges"},
                        ],
                    },
                    "payload": {
                        "ddl": "ALTER TABLE customers ADD COLUMN age INT",
                        "databaseName": "inventory",
                        "tableChanges": "[]",
                    },
                }
            ),
            destination="inventory",
        )
        tombstone_rec = MockChangeEvent(
            key=_wrap_json_key({"id": 1}),
            value=None,
            destination="inventory.customers",
        )
        tombstone_json_rec = MockChangeEvent(
            key=_wrap_json_key({"id": 2}),
            value='{"schema": {"type": "struct", "fields": [{"type": "int64", "field": "id"}]}, "payload": null}',
            destination="inventory.customers",
        )

        batch = self.converter.convert(
            [heartbeat_rec, ddl_rec, tombstone_rec, tombstone_json_rec]
        )
        self.assertEqual(len(batch.records), 4)
        self.assertEqual(
            [r.kind for r in batch.records],
            [
                EventKind.HEARTBEAT,
                EventKind.SCHEMA_CHANGE,
                EventKind.TOMBSTONE,
                EventKind.TOMBSTONE,
            ],
        )
        tombstones = batch.of_kind(EventKind.TOMBSTONE)
        self.assertEqual(len(tombstones), 2)
        self.assertEqual(tombstones[0].key, {"id": 1})
        self.assertIsNone(tombstones[0].schema)
        self.assertEqual(tombstones[1].key, {"id": 2})
        self.assertIsNotNone(tombstones[1].schema)
        self.assertEqual(batch.table_batches(), ())

    def test_schemaless_json_mode_none_in_first_row_and_null_guard(self) -> None:
        conv = SchemalessJsonEventConverter()
        r1 = MockChangeEvent(
            key=json.dumps({"id": 1}),
            value=json.dumps({"id": 1, "age": None, "note": None}),
            destination="users",
        )
        r2 = MockChangeEvent(
            key=json.dumps({"id": 2}),
            value=json.dumps({"id": 2, "age": 25, "note": None}),
            destination="users",
        )
        batch = conv.convert([r1, r2])
        tb = batch.table_batches()[0]
        schema, rows = tb.schema, tb.rows
        self.assertEqual(schema.primary_keys, ("id",))
        self.assertEqual(schema.arrow_schema.field("age").type, pa.int64())
        self.assertEqual(schema.arrow_schema.field("note").type, pa.string())
        self.assertEqual(len(rows), 2)

        tbl = tb.to_arrow()
        self.assertEqual(tbl.num_rows, 2)
        self.assertEqual(tbl.column("age").to_pylist(), [None, 25])

        # Also test single-event convert_event in schemaless mode
        single_rec = conv.convert_event(r2)
        self.assertEqual(single_rec.value, {"id": 2, "age": 25, "note": None})
        self.assertEqual(single_rec.key, {"id": 2})

    def test_schemaless_envelope_symmetry_cross_batch_nulls_and_ordering(self) -> None:
        # 1. Envelope insert (before=None) then delete (after=None)
        conv_env = SchemalessJsonEventConverter()
        ev_c = MockChangeEvent(
            key=json.dumps({"id": 1}),
            value=json.dumps(
                {"before": None, "after": {"id": 1, "name": "Alice"}, "op": "c"}
            ),
            destination="users",
        )
        ev_d = MockChangeEvent(
            key=json.dumps({"id": 1}),
            value=json.dumps(
                {"before": {"id": 1, "name": "Alice"}, "after": None, "op": "d"}
            ),
            destination="users",
        )
        r_c = conv_env.convert_event(ev_c)
        r_d = conv_env.convert_event(ev_d)
        self.assertEqual(r_c.schema, r_d.schema)
        self.assertEqual(r_c.op, ChangeOp.INSERT)
        self.assertEqual(r_d.op, ChangeOp.DELETE)
        tbl_env = ChangeBatch(records=(r_c, r_d)).table_batches()[0].to_arrow()
        self.assertEqual(tbl_env.num_rows, 2)

        # 2. Cross-batch null preservation (int64 -> None) & refinement (None -> int64)
        conv_null_first = SchemalessJsonEventConverter()
        rec_null = conv_null_first.convert_event(
            MockChangeEvent(
                key=json.dumps({"id": 1}),
                value=json.dumps({"id": 1, "age": None}),
                destination="users",
            )
        )
        rec_int = conv_null_first.convert_event(
            MockChangeEvent(
                key=json.dumps({"id": 2}),
                value=json.dumps({"id": 2, "age": 25}),
                destination="users",
            )
        )
        assert rec_null.schema is not None and rec_int.schema is not None
        self.assertTrue(rec_int.schema.diff(rec_null.schema).is_compatible)
        batches_null_int = ChangeBatch(records=(rec_null, rec_int)).table_batches()
        tbl_unified = pa.concat_tables(
            [
                batches_null_int[0].to_arrow(target_schema=rec_int.schema.arrow_schema),
                batches_null_int[1].to_arrow(),
            ]
        )
        self.assertEqual(tbl_unified.schema.field("age").type, pa.int64())
        self.assertEqual(tbl_unified.column("age").to_pylist(), [None, 25])

        # Subsequent None event preserves concrete int64 type from cache
        rec_null_after = conv_null_first.convert_event(
            MockChangeEvent(
                key=json.dumps({"id": 3}),
                value=json.dumps({"id": 3, "age": None}),
                destination="users",
            )
        )
        self.assertEqual(rec_null_after.schema, rec_int.schema)

        # 3. Empty dict {} in schemaless mode and interleaved destination ordering
        conv_misc = SchemalessJsonEventConverter()
        b_misc = conv_misc.convert(
            [
                MockChangeEvent(
                    key=json.dumps({"id": 1}),
                    value=json.dumps({"id": 1, "meta": {}}),
                    destination="table_a",
                ),
                MockChangeEvent(
                    key=json.dumps({"id": 10}),
                    value=json.dumps({"id": 10}),
                    destination="table_b",
                ),
                MockChangeEvent(
                    key=json.dumps({"id": 2}),
                    value=json.dumps({"id": 2, "meta": {}}),
                    destination="table_a",
                ),
            ]
        )
        self.assertEqual(
            [(r.destination, r.value["id"]) for r in b_misc.records if r.value],
            [("table_a", 1), ("table_b", 10), ("table_a", 2)],
        )

    def test_connect_scalar_key_tombstones_temporal_scaling_and_negative_scale(
        self,
    ) -> None:
        class MockConnectType:
            def __init__(self, name: str) -> None:
                self._name = name

            def name(self) -> str:
                return self._name

        class MockScalarKeySchema:
            def type(self) -> MockConnectType:
                return MockConnectType("INT64")

            def name(self) -> None:
                return None

            def isOptional(self) -> bool:
                return False

            def doc(self) -> None:
                return None

            def parameters(self) -> dict[str, str]:
                return {}

        class MockScalarTombstoneSourceRecord:
            def keySchema(self) -> MockScalarKeySchema:
                return MockScalarKeySchema()

            def valueSchema(self) -> None:
                return None

            def key(self) -> int:
                return 42

            def value(self) -> None:
                return None

        conv_conn = ConnectEventConverter()
        ts_rec = conv_conn.convert_event(
            MockChangeEvent(
                key=42,
                value=MockScalarTombstoneSourceRecord(),
                destination="connect.orders",
            )
        )
        self.assertEqual(ts_rec.kind, EventKind.TOMBSTONE)
        self.assertEqual(ts_rec.key, {"key": 42})
        self.assertIsNone(ts_rec.schema)

        # java.sql.Timestamp (getTime in ms + getNanos) and java.time.Instant / LocalTime
        class MockJavaSqlTimestamp:
            def getTime(self) -> int:
                return 1710513000123

            def getNanos(self) -> int:
                return 123456000

        class MockJavaInstant:
            def getEpochSecond(self) -> int:
                return 1710513000

            def getNano(self) -> int:
                return 123456789

        class MockJavaLocalTime:
            def toNanoOfDay(self) -> int:
                return (14 * 3600 + 30 * 60) * 1_000_000_000 + 123456000

        tsh_us = TimestampTypeHandler(unit="us")
        val_sql_ts = tsh_us.convert_connect(MockJavaSqlTimestamp())
        tbl_sql_ts = pa.Table.from_pylist(
            [{"ts": val_sql_ts}], schema=pa.schema([("ts", tsh_us.arrow_type)])
        )
        self.assertEqual(
            tbl_sql_ts.to_pylist()[0]["ts"],
            datetime.datetime(2024, 3, 15, 14, 30, 0, 123456),
        )

        tsh_ns = TimestampTypeHandler(unit="ns")
        self.assertEqual(
            tsh_ns.convert_connect(MockJavaInstant()),
            1710513000 * 1_000_000_000 + 123456789,
        )

        th_us = TimeTypeHandler(unit="us")
        val_lt = th_us.convert_connect(MockJavaLocalTime())
        tbl_lt = pa.Table.from_pylist(
            [{"t": val_lt}], schema=pa.schema([("t", th_us.arrow_type)])
        )
        self.assertEqual(
            tbl_lt.to_pylist()[0]["t"],
            datetime.time(14, 30, 0, 123456),
        )

        # Sliced ByteBuffer with position/remaining/arrayOffset
        class MockSlicedByteBuffer:
            def array(self) -> bytes:
                return b"XX\xde\xad\xbe\xefYY"

            def arrayOffset(self) -> int:
                return 1

            def position(self) -> int:
                return 1

            def remaining(self) -> int:
                return 4

        self.assertEqual(
            BinaryTypeHandler().convert_connect(MockSlicedByteBuffer()),
            b"\xde\xad\xbe\xef",
        )

        # VariableScaleDecimal with negative scale (e.g. 1E+2 = 100)
        b64_one = base64.b64encode((1).to_bytes(1, "big", signed=True)).decode("ascii")
        self.assertEqual(
            VariableScaleDecimalHandler(38, 10).convert_json(
                {"scale": -2, "value": b64_one}
            ),
            Decimal("100"),
        )

    def test_event_format_resolve_and_table_schema_domain_methods(self) -> None:
        self.assertEqual(
            EventFormat.resolve_from(EventFormat.CONNECT),
            EventFormat.CONNECT,
        )
        self.assertEqual(
            EventFormat.resolve_from("io.debezium.embedded.Connect"),
            EventFormat.CONNECT,
        )
        self.assertEqual(
            EventFormat.resolve_from("io.debezium.engine.format.Json"),
            EventFormat.JSON,
        )
        self.assertEqual(
            EventFormat.resolve_from("connect"),
            EventFormat.CONNECT,
        )
        self.assertEqual(
            EventFormat.resolve_from(
                "org.apache.kafka.connect.storage.ConnectConverter"
            ),
            EventFormat.CONNECT,
        )
        self.assertEqual(
            EventFormat.resolve_from("org.apache.kafka.connect.json.JsonConverter"),
            EventFormat.JSON,
        )
        with self.assertRaises(ValueError):
            EventFormat.resolve_from("invalid_format")

        # TableSchema compiled via JsonEventConverter binds value_handler and key_handler
        schema = self.converter.compile_json_schema(
            destination="inventory.items",
            schema_block={
                "type": "struct",
                "fields": [
                    {"type": "int64", "field": "id"},
                    {
                        "type": "string",
                        "name": "io.debezium.time.IsoDate",
                        "field": "dt",
                    },
                ],
            },
            key_schema_block={
                "type": "struct",
                "fields": [{"type": "int64", "field": "id"}],
            },
        )
        self.assertIsNotNone(schema.value_handler)
        self.assertIsNotNone(schema.key_handler)
        self.assertEqual(
            schema.convert_json_value({"id": 5, "dt": "2024-01-15"}),
            {"id": 5, "dt": datetime.date(2024, 1, 15)},
        )
        self.assertEqual(schema.convert_json_key({"id": 5}), {"id": 5})

        # Temporal and Decimal Connect handlers reject Java Boolean (booleanValue)
        class MockJavaBoolAsInt(int):
            def booleanValue(self) -> bool:
                return True

        j_bool = MockJavaBoolAsInt(1)
        for handler in (
            DateTypeHandler(),
            TimeTypeHandler(),
            TimestampTypeHandler(),
            DecimalTypeHandler(),
        ):
            with self.assertRaises(TypeError):
                handler.convert_connect(j_bool)

        # Schemaless empty list [] followed by populated list [10, 20] unifies cleanly
        conv_sl = SchemalessJsonEventConverter()
        r_empty_list = conv_sl.convert_event(
            MockChangeEvent(
                key=json.dumps({"id": 1}),
                value=json.dumps({"id": 1, "scores": []}),
                destination="users",
            )
        )
        r_int_list = conv_sl.convert_event(
            MockChangeEvent(
                key=json.dumps({"id": 2}),
                value=json.dumps({"id": 2, "scores": [10, 20]}),
                destination="users",
            )
        )
        assert r_empty_list.schema is not None and r_int_list.schema is not None
        self.assertTrue(r_int_list.schema.diff(r_empty_list.schema).is_compatible)
        b_lists = ChangeBatch(records=(r_empty_list, r_int_list)).table_batches()
        tbl_lists = pa.concat_tables(
            [
                b_lists[0].to_arrow(target_schema=r_int_list.schema.arrow_schema),
                b_lists[1].to_arrow(),
            ]
        )
        self.assertEqual(
            tbl_lists.to_pylist(),
            [{"id": 1, "scores": []}, {"id": 2, "scores": [10, 20]}],
        )

        # Schemaless envelope UPDATE with None in before and int64 in after unifies before/after
        r_env_upd = conv_sl.convert_event(
            MockChangeEvent(
                key=json.dumps({"id": 1}),
                value=json.dumps(
                    {
                        "before": {"id": 1, "age": None},
                        "after": {"id": 1, "age": 25},
                        "op": "u",
                    }
                ),
                destination="users_env",
            )
        )
        assert r_env_upd.schema is not None
        self.assertEqual(
            r_env_upd.schema.arrow_schema.field("before").type,
            r_env_upd.schema.arrow_schema.field("after").type,
        )

        # TableSchema.convert_connect_key with MapTypeHandler vs uncompiled key
        keyless_schema = TableSchema(
            destination="inventory.items",
            kind=EventKind.DATA,
            arrow_schema=pa.schema([("id", pa.int64())]),
        )
        with self.assertRaises(TypeError):
            keyless_schema.convert_connect_key({"101": 42, "nested": "v"})

        map_key_schema = TableSchema(
            destination="inventory.items",
            kind=EventKind.DATA,
            arrow_schema=pa.schema([("id", pa.int64())]),
            key_handler=MapTypeHandler(
                SchemaField("key", StringTypeHandler(), nullable=False),
                SchemaField("value", IntTypeHandler(pa.int64()), nullable=True),
            ),
        )
        self.assertEqual(map_key_schema.convert_connect_key({"k1": 10}), {"k1": 10})
        with self.assertRaises(TypeError):
            keyless_schema.convert_connect_key(object())

        # VariableScaleDecimal requires explicit int scale when value is present
        vardec = VariableScaleDecimalHandler(precision=38, scale=4)
        with self.assertRaises(ValueError):
            vardec.convert_json({"value": "D4A="})
        with self.assertRaises(ValueError):
            vardec.convert_json({"scale": None, "value": "D4A="})
        with self.assertRaises(TypeError):
            vardec.convert_json({"scale": "2", "value": "D4A="})
        with self.assertRaises(TypeError):
            vardec.convert_json({"scale": True, "value": "D4A="})

        # BinaryTypeHandler rejects list/tuple inputs
        bin_h = BinaryTypeHandler()
        with self.assertRaises(TypeError):
            bin_h.convert_json([1, 2, 3])
        with self.assertRaises(TypeError):
            bin_h.convert_connect([1, 2, 3])

        # Empty/whitespace JSON strings fail fast with ValueError
        with self.assertRaises(ValueError):
            self.converter.convert_event(
                MockChangeEvent(
                    key=_wrap_json_key({"id": 1}), value="   ", destination="t"
                )
            )
        with self.assertRaises(ValueError):
            self.converter.convert_event(
                MockChangeEvent(key="   ", value=None, destination="t")
            )

        # FieldSpec.from_json rejects non-bool optional, non-str doc/name, and non-Mapping map keys/values
        with self.assertRaises(ValueError):
            FieldSpec.from_json({"type": "int32", "field": "x", "optional": "true"})
        with self.assertRaises(ValueError):
            FieldSpec.from_json({"type": "int32", "field": "x", "doc": 123})
        with self.assertRaises(ValueError):
            FieldSpec.from_json({"type": "int32", "field": "x", "name": 123})
        with self.assertRaises(ValueError):
            FieldSpec.from_json(
                {
                    "type": "map",
                    "field": "m",
                    "keys": "string",
                    "values": {"type": "int32"},
                }
            )

        # Schemaless row with a column named 'payload' alongside other columns or alone is preserved as a row
        r_col_payload = conv_sl.convert_event(
            MockChangeEvent(
                key=json.dumps({"id": 1}),
                value=json.dumps({"id": 1, "payload": "raw_body"}),
                destination="webhooks",
            )
        )
        self.assertEqual(r_col_payload.value, {"id": 1, "payload": "raw_body"})

        r_single_col_payload = conv_sl.convert_event(
            MockChangeEvent(
                key=json.dumps({"id": 1}),
                value=json.dumps({"payload": "raw_body"}),
                destination="webhooks_single",
            )
        )
        self.assertEqual(r_single_col_payload.value, {"payload": "raw_body"})

        # Schemaless key with a column named 'schema' or 'payload' is preserved as a key
        r_key_with_schema_col = conv_sl.convert_event(
            MockChangeEvent(
                key=json.dumps({"schema": "public", "table": "users"}),
                value=json.dumps({"schema": "public", "table": "users"}),
                destination="pg_tables",
            )
        )
        assert r_key_with_schema_col.schema is not None
        self.assertEqual(
            r_key_with_schema_col.key, {"schema": "public", "table": "users"}
        )
        self.assertEqual(r_key_with_schema_col.schema.primary_keys, ("schema", "table"))

        # JDouble / float with longValue/intValue is rejected by integer and temporal Connect handlers
        class MockJavaDouble(float):
            def longValue(self) -> int:
                return int(self)

            def intValue(self) -> int:
                return int(self)

            def doubleValue(self) -> float:
                return float(self)

        j_double = MockJavaDouble(3.99)
        for int_or_temporal_handler in (
            IntTypeHandler(pa.int64()),
            DateTypeHandler(),
            TimeTypeHandler(),
            TimestampTypeHandler(),
        ):
            with self.assertRaises(TypeError):
                int_or_temporal_handler.convert_connect(j_double)

        # Non-finite Decimal / float values fail fast
        for bad_float in (float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                DecimalTypeHandler.decode_json(bad_float)
            with self.assertRaises(ValueError):
                DecimalTypeHandler(10, 2).convert_json(bad_float)
        for bad_dec in (Decimal("NaN"), Decimal("Infinity")):
            with self.assertRaises(ValueError):
                DecimalTypeHandler.decode_connect(bad_dec)
            with self.assertRaises(ValueError):
                DecimalTypeHandler(10, 2).convert_connect(bad_dec)

        # Fixed-scale Connect Decimal requires 'scale' parameter in schema
        with self.assertRaises(ValueError):
            self.converter.compile_json_schema(
                "inventory.missing_scale",
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "bytes",
                            "name": "org.apache.kafka.connect.data.Decimal",
                            "parameters": {"connect.decimal.precision": "10"},
                            "field": "amount",
                        }
                    ],
                },
            )

        # Hstore rejects invalid underlying schema type
        with self.assertRaises(ValueError):
            self.converter.compile_json_schema(
                "inventory.bad_hstore",
                {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "int32",
                            "name": "io.debezium.data.Hstore",
                            "field": "tags",
                        }
                    ],
                },
            )

        # Non-string destination and non-int partition fail fast with TypeError
        with self.assertRaises(TypeError):
            self.converter.convert_event(
                MockChangeEvent(
                    key=_wrap_json_key({"id": 1}),
                    value=None,
                    destination=123,  # type: ignore[arg-type]
                )
            )
        with self.assertRaises(TypeError):
            self.converter.convert_event(
                MockChangeEvent(
                    key=_wrap_json_key({"id": 1}),
                    value=None,
                    destination="t",
                    partition="0",  # type: ignore[arg-type]
                )
            )

    def test_nanosecond_iso_strings_vectors_non_string_maps_and_map_sanitization(
        self,
    ) -> None:
        # 1. NanoTimestamp and NanoTime preserve all 9 fractional digits on ISO strings
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
        # Pre-1970 nanosecond ISO timestamp
        self.assertEqual(
            ts_ns_h.convert_json("1969-12-31T23:59:59.123456789Z"),
            -1_000_000_000 + 123_456_789,
        )
        # 7 fractional digits right-padded to 9 digits
        self.assertEqual(
            ts_ns_h.convert_json("2024-05-05T14:30:00.1234567Z"),
            1714919400 * 1_000_000_000 + 123_456_700,
        )

        expected_tm_ns = (4 * 3_600 + 5 * 60 + 11) * 1_000_000_000 + 123_456_789
        self.assertEqual(
            tm_ns_h.convert_json("04:05:11.123456789"),
            expected_tm_ns,
        )
        self.assertEqual(
            tm_ns_h.convert_connect("04:05:11.123456789Z"),
            expected_tm_ns,
        )

        # Verify pa.Table materialization preserves exact nanosecond integers
        ns_tbl = pa.Table.from_pylist(
            [
                {
                    "ts": ts_ns_h.convert_json("2024-05-05T14:30:00.123456789Z"),
                    "tm": tm_ns_h.convert_json("04:05:11.123456789"),
                }
            ],
            schema=pa.schema([("ts", ts_ns_h.arrow_type), ("tm", tm_ns_h.arrow_type)]),
        )
        self.assertEqual(ns_tbl.column("ts")[0].value, expected_ts_ns)
        self.assertEqual(ns_tbl.column("tm")[0].value, expected_tm_ns)

        # 2. Non-3/6 digit fractional seconds (1, 2, 4, 5 digits) normalize to 6 digits
        self.assertEqual(
            TimestampTypeHandler.truncate_fraction_to_micros("2024-05-01T12:00:00.1Z"),
            "2024-05-01T12:00:00.100000Z",
        )
        self.assertEqual(
            TimestampTypeHandler.truncate_fraction_to_micros("10:30:00.1234+00:00"),
            "10:30:00.123400+00:00",
        )
        ts_us_h = TimestampTypeHandler(unit="us", tz="UTC")
        self.assertEqual(
            ts_us_h.convert_json("2024-05-01T12:00:00.1Z"),
            datetime.datetime(
                2024, 5, 1, 12, 0, 0, 100000, tzinfo=datetime.timezone.utc
            ),
        )
        tm_us_h = TimeTypeHandler(unit="us")
        self.assertEqual(
            tm_us_h.convert_json("10:30:00.1234Z"),
            datetime.time(10, 30, 0, 123400, tzinfo=datetime.timezone.utc),
        )

        # 3. MapTypeHandler.convert_json with non-string keys (array-of-pairs & JSON object)
        int_map_h = MapTypeHandler(
            SchemaField("key", IntTypeHandler(pa.int16()), nullable=False),
            SchemaField("value", FloatTypeHandler(pa.float64()), nullable=True),
        )
        self.assertEqual(
            int_map_h.convert_json([[1, 0.5], [5, 1.25]]),
            {1: 0.5, 5: 1.25},
        )
        self.assertEqual(
            int_map_h.convert_json({"1": 0.5, "5": 1.25}),
            {1: 0.5, 5: 1.25},
        )
        float_map_h = MapTypeHandler(
            SchemaField("key", FloatTypeHandler(pa.float64()), nullable=False),
            SchemaField("value", IntTypeHandler(pa.int32()), nullable=True),
        )
        self.assertEqual(
            float_map_h.convert_json({"1.5": 10, "2.25": 20}),
            {1.5: 10, 2.25: 20},
        )
        bool_map_h = MapTypeHandler(
            SchemaField("key", BooleanTypeHandler(), nullable=False),
            SchemaField("value", IntTypeHandler(pa.int32()), nullable=True),
        )
        self.assertEqual(
            bool_map_h.convert_json({"true": 1, "false": 0}),
            {True: 1, False: 0},
        )
        self.assertEqual(
            bool_map_h.convert_json([[True, 1], [False, 0]]),
            {True: 1, False: 0},
        )
        with self.assertRaises(TypeError):
            bool_map_h.convert_json({"not_a_bool": 1})
        with self.assertRaises(ValueError):
            int_map_h.convert_json([[1, 2, 3]])
        with self.assertRaises(ValueError):
            int_map_h.convert_json(["ab"])
        with self.assertRaises(ValueError):
            int_map_h.convert_json([[None, 0.5]])
        with self.assertRaises(ValueError):
            int_map_h.convert_connect({None: 0.5})
        with self.assertRaises(ValueError):
            int_map_h.convert_json({"not_an_int": 0.5})
        with self.assertRaises(TypeError):
            int_map_h.convert_json("not_a_map")

        # 4. Tsvector, Point, and PGVector logical type handlers
        fields: list[dict[str, Any]] = [
            {
                "type": "string",
                "name": "io.debezium.data.Tsvector",
                "field": "f_tsvector",
            },
            {
                "type": "array",
                "name": "io.debezium.data.FloatVector",
                "field": "f_float_vec",
            },
            {
                "type": "array",
                "name": "io.debezium.data.DoubleVector",
                "field": "f_double_vec",
            },
            {
                "type": "struct",
                "name": "io.debezium.data.SparseDoubleVector",
                "field": "f_sparse_vec",
            },
            {
                "type": "struct",
                "name": "io.debezium.data.geometry.Point",
                "field": "f_point",
            },
        ]
        ev = make_flat_event(
            dest="test.vectors_and_geo",
            fields=fields,
            payload={
                "f_tsvector": "'cat':1 'fat':2,4",
                "f_float_vec": [1.0, 2.5],
                "f_double_vec": [3.5, 4.25],
                "f_sparse_vec": {"dimensions": 10, "vector": [[1, 0.5], [5, 1.25]]},
                "f_point": {"x": 10.0, "y": 20.0, "wkb": "0x0102", "srid": 4326},
            },
        )
        vec_batch = self.converter.convert([ev])
        vec_tbl = vec_batch.table_batches()[0].to_arrow()
        point_type = vec_tbl.schema.field("f_point").type
        self.assertFalse(point_type.field("x").nullable)
        self.assertFalse(point_type.field("y").nullable)
        self.assertTrue(point_type.field("wkb").nullable)
        self.assertTrue(point_type.field("srid").nullable)
        self.assertEqual(
            vec_tbl.to_pylist()[0],
            {
                "f_tsvector": "'cat':1 'fat':2,4",
                "f_float_vec": [1.0, 2.5],
                "f_double_vec": [3.5, 4.25],
                "f_sparse_vec": {"dimensions": 10, "vector": [(1, 0.5), (5, 1.25)]},
                "f_point": {
                    "x": 10.0,
                    "y": 20.0,
                    "wkb": b"\x01\x02",
                    "srid": 4326,
                },
            },
        )

        # Verify dedicated domain handler lookups in TypeRegistry
        reg = TypeRegistry()
        self.assertIs(
            reg.compile_handler(
                FieldSpec(
                    name="tags",
                    type_name="map",
                    logical_name="io.debezium.data.Hstore",
                )
            ),
            TypeRegistry.HSTORE_MAP_HANDLER,
        )
        for struct_logical, expected_cls in (
            ("io.debezium.data.geometry.Geometry", GeometryTypeHandler),
            ("io.debezium.data.geometry.Geography", GeometryTypeHandler),
            ("io.debezium.data.geometry.Point", PointTypeHandler),
            ("io.debezium.data.SparseDoubleVector", SparseVectorHandler),
        ):
            compiled = reg.compile_handler(
                FieldSpec(
                    name="col",
                    type_name="struct",
                    logical_name=struct_logical,
                )
            )
            self.assertIsInstance(compiled, expected_cls)
            self.assertIs(compiled, TypeRegistry.FIXED_LOGICAL_HANDLERS[struct_logical])
        for array_logical in (
            "io.debezium.data.FloatVector",
            "io.debezium.data.DoubleVector",
        ):
            compiled_arr = reg.compile_handler(
                FieldSpec(
                    name="col",
                    type_name="array",
                    logical_name=array_logical,
                )
            )
            self.assertIsInstance(compiled_arr, DenseVectorHandler)
            self.assertIs(
                compiled_arr, TypeRegistry.FIXED_LOGICAL_HANDLERS[array_logical]
            )

        p1 = StringTypeHandler()
        p2 = StringTypeHandler()
        self.assertEqual(p1, p2)
        self.assertEqual(hash(p1), hash(p2))
        self.assertEqual(p1.convert_json("abc"), "abc")
        self.assertEqual(p1.convert_connect("abc"), "abc")
        for cloned in (
            pickle.loads(pickle.dumps(p1)),
            copy.copy(p1),
            copy.deepcopy(p1),
        ):
            self.assertEqual(cloned, p1)
            self.assertEqual(cloned.convert_json("abc"), "abc")
            self.assertEqual(cloned.convert_connect("abc"), "abc")
        gc.collect()
        gc.disable()
        try:
            for _ in range(50):
                IntTypeHandler(pa.int32())
            self.assertEqual(gc.collect(), 0)
        finally:
            gc.enable()

        # 5. SchemalessJsonEventConverter sanitization for pa.map_(key, pa.null()) and nested structures
        s_map_null = SchemalessJsonEventConverter.sanitize_arrow_schema(
            pa.schema([("id", pa.int64()), ("m", pa.map_(pa.string(), pa.null()))])
        )
        m_field = s_map_null.field("m")
        self.assertEqual(m_field.type, pa.map_(pa.string(), pa.string()))
        self.assertTrue(TableSchema.is_inferred_null_field(m_field))

        # Subsequent all-null batch preserves the map container type and inferred-null marker
        s_map_then_null = SchemalessJsonEventConverter.sanitize_arrow_schema(
            pa.schema([("id", pa.int64()), ("m", pa.null())]),
            previous_schema=s_map_null,
        )
        self.assertEqual(
            s_map_then_null.field("m").type, pa.map_(pa.string(), pa.string())
        )
        self.assertTrue(TableSchema.is_inferred_null_field(s_map_then_null.field("m")))

        s_map_int = pa.schema(
            [("id", pa.int64()), ("m", pa.map_(pa.string(), pa.int64()))]
        )
        s_preserved = SchemalessJsonEventConverter.sanitize_arrow_schema(
            pa.schema([("id", pa.int64()), ("m", pa.map_(pa.string(), pa.null()))]),
            previous_schema=s_map_int,
        )
        self.assertEqual(s_preserved.field("m").type, pa.map_(pa.string(), pa.int64()))
        self.assertFalse(TableSchema.is_inferred_null_field(s_preserved.field("m")))

        # Nested map inside struct and list<map> sanitization and diff compatibility
        s_nested_null = SchemalessJsonEventConverter.sanitize_arrow_schema(
            pa.schema(
                [
                    ("id", pa.int64()),
                    ("s", pa.struct([("m", pa.map_(pa.string(), pa.null()))])),
                    ("lm", pa.list_(pa.map_(pa.string(), pa.null()))),
                ]
            )
        )
        self.assertTrue(TableSchema.is_inferred_null_field(s_nested_null.field("s")))
        self.assertTrue(TableSchema.is_inferred_null_field(s_nested_null.field("lm")))

        s_nested_int = pa.schema(
            [
                ("id", pa.int64()),
                ("s", pa.struct([("m", pa.map_(pa.string(), pa.int64()))])),
                ("lm", pa.list_(pa.map_(pa.string(), pa.int64()))),
            ]
        )
        ts_nested_1 = TableSchema(
            destination="t", kind=EventKind.DATA, arrow_schema=s_nested_null
        )
        ts_nested_2 = TableSchema(
            destination="t", kind=EventKind.DATA, arrow_schema=s_nested_int
        )
        self.assertTrue(ts_nested_2.diff(ts_nested_1).is_compatible)


if __name__ == "__main__":
    unittest.main()
