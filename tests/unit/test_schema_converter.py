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
    ChangeBatch,
    ChangeRecord,
    ConnectEventConverter,
    EventKind,
    JsonEventConverter,
    SchemaAwareChangeHandler,
    SchemaDiff,
    SchemalessJsonEventConverter,
    TableBatch,
    TableSchema,
)
from pydbzengine.schema.types import (
    ArrayTypeHandler,
    BinaryTypeHandler,
    BooleanTypeHandler,
    DateTypeHandler,
    DecimalTypeHandler,
    FieldSpec,
    FloatTypeHandler,
    IntTypeHandler,
    MapTypeHandler,
    SchemaField,
    StringTypeHandler,
    StructTypeHandler,
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
        table_schema, row = record.schema, record.value
        assert table_schema is not None
        assert row is not None
        self.assertEqual(table_schema.destination, "inventory.customers")
        self.assertEqual(table_schema.primary_keys, ("id",))
        self.assertIsNotNone(table_schema.key_schema)
        assert table_schema.key_schema is not None
        self.assertEqual(table_schema.key_schema.field("id").type, pa.int32())
        self.assertEqual(record.key, {"id": 1001})
        self.assertEqual(row["salary"], Decimal("199.95"))

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
        self.assertIn("discount_code", b2.schema.arrow_schema.names)

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

            def fields(self) -> list[Any] | None:
                return None

            def valueSchema(self) -> Any:
                return None

            def keySchema(self) -> Any:
                return None

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

        interval_h = TypeRegistry.FIXED_LOGICAL_HANDLERS[
            "io.debezium.time.MicroDuration"
        ]
        self.assertEqual(interval_h.convert_json(86400000000), 86400000000)

        # 2. BinaryTypeHandler
        bin_h = BinaryTypeHandler()
        self.assertEqual(
            bin_h.convert_json(base64.b64encode(b"\xde\xad\xbe\xef").decode("ascii")),
            b"\xde\xad\xbe\xef",
        )

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
        self.assertEqual(date_h.convert_json("2024-05-05Z"), datetime.date(2024, 5, 5))
        with self.assertRaises(ValueError):
            date_h.convert_json("2024-05-05T14:30:00Z")

        class MockJavaConnectTime:
            def getTime(self) -> int:
                return 14_711_000

        time_h = TimeTypeHandler(unit="ms")
        self.assertEqual(time_h.convert_connect(MockJavaConnectTime()), 14_711_000)
        with self.assertRaises(ValueError):
            time_h.convert_connect(MockJavaUtilDate())

        ts_utc_h = TimestampTypeHandler(unit="us", tz="UTC")
        self.assertEqual(
            ts_utc_h.convert_json("2026-09-07T12:00:00Z"),
            datetime.datetime(2026, 9, 7, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

        # 5. ArrayTypeHandler & MapTypeHandler
        b64_125 = base64.b64encode((125).to_bytes(2, "big", signed=True)).decode(
            "ascii"
        )
        arr_h = ArrayTypeHandler(SchemaField("item", DecimalTypeHandler(10, 2)))
        self.assertEqual(arr_h.convert_json([b64_125, None]), [Decimal("1.25"), None])
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
            self.converter.compile_schema(
                "t",
                {"type": "struct", "fields": [{"type": "unknown_type", "field": "x"}]},
            )

        with self.assertRaises(ValueError):
            self.converter.compile_schema(
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
            self.converter.compile_schema(
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

        # Malformed non-list 'fields', non-dict 'parameters'/'items'/'keys'/'values', or non-bool 'optional' in FieldSpec.from_json fail fast
        for malformed_schema in (
            {"type": "struct", "field": "st", "fields": "not_a_list"},
            {"type": "string", "field": "s", "parameters": "not_a_dict"},
            {"type": "string", "field": "s", "optional": "false"},
            {"type": "array", "field": "arr", "items": 123},
            {"type": "map", "field": "mp", "keys": 123, "values": {"type": "int32"}},
            {"type": "map", "field": "mp", "keys": {"type": "string"}, "values": 123},
        ):
            with self.assertRaises(ValueError):
                FieldSpec.from_json(malformed_schema)

        # DecimalTypeHandler fails fast when value exceeds configured precision
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

        # Valid key schema with null key payload preserves primary_keys on TableSchema
        null_key_payload_rec = self.converter.convert_event(
            MockChangeEvent(
                key=json.dumps(
                    {
                        "schema": {
                            "type": "struct",
                            "fields": [{"type": "int64", "field": "id"}],
                        },
                        "payload": None,
                    }
                ),
                value=json.dumps(
                    {
                        "schema": {
                            "type": "struct",
                            "fields": [{"type": "int64", "field": "id"}],
                        },
                        "payload": {"id": 1},
                    }
                ),
                destination="inventory.nullable_key_payload",
            )
        )
        assert null_key_payload_rec.schema is not None
        self.assertEqual(null_key_payload_rec.schema.primary_keys, ("id",))
        self.assertIsNone(null_key_payload_rec.key)

        # Empty struct key schema with non-empty key payload fails fast
        with self.assertRaises(ValueError):
            self.converter.convert_event(
                MockChangeEvent(
                    key=json.dumps(
                        {
                            "schema": {"type": "struct", "fields": []},
                            "payload": {"id": 1},
                        }
                    ),
                    value=json.dumps(
                        {
                            "schema": {
                                "type": "struct",
                                "fields": [{"type": "int64", "field": "id"}],
                            },
                            "payload": {"id": 1},
                        }
                    ),
                    destination="inventory.bad_empty_struct_key",
                )
            )

        # Non-null key with null key schema fails fast with ValueError without polluting _cache
        with self.assertRaises(ValueError):
            self.converter.convert_event(
                MockChangeEvent(
                    key=json.dumps({"schema": None, "payload": {"id": 1}}),
                    value=json.dumps(
                        {
                            "schema": {
                                "type": "struct",
                                "fields": [{"type": "int64", "field": "id"}],
                            },
                            "payload": {"id": 1},
                        }
                    ),
                    destination="inventory.missing_key_schema",
                )
            )
        self.assertNotIn("inventory.missing_key_schema", self.converter._cache)

        # Complex FieldSpec missing required child schemas fails fast
        reg = TypeRegistry()
        for bad_spec in (
            FieldSpec(name="arr", type_name="array"),
            FieldSpec(name="mp", type_name="map"),
            FieldSpec(name="st", type_name="struct"),
        ):
            with self.assertRaises(ValueError):
                reg.compile_handler(bad_spec)

    def test_schema_change_handler_lifecycle_and_parquet_writing(self) -> None:
        class LakehouseParquetHandler(SchemaAwareChangeHandler):
            def __init__(self, base_dir: Path) -> None:
                super().__init__()
                self.base_dir = base_dir
                self.target_schemas: dict[str, pa.Schema] = {}
                self.written_files: list[Path] = []
                self.created_tables: list[str] = []
                self.evolutions: list[tuple[str, SchemaDiff]] = []
                self.tombstones: list[ChangeRecord] = []

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

            def on_tombstone(self, record: ChangeRecord) -> None:
                self.tombstones.append(record)

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
            self.assertEqual(len(handler.tombstones), 1)
            self.assertEqual(handler.tombstones[0].key, {"id": 99})
            self.assertEqual(handler.tombstones[0].kind, EventKind.TOMBSTONE)

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
        self.assertEqual(schema.arrow_schema.field("note").type, pa.null())
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
        assert r_c.schema is not None and r_d.schema is not None
        assert r_c.value is not None and r_d.value is not None
        self.assertEqual(r_c.value["op"], "c")
        self.assertEqual(r_d.value["op"], "d")
        tbl_env = conv_env.convert([ev_c, ev_d]).table_batches()[0].to_arrow()
        self.assertEqual(tbl_env.num_rows, 2)

        # 2. Cross-batch null refinement (None -> int64) via native PyArrow promotion
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

        # TableSchema compiled via JsonEventConverter caches value_handler and key_handler in _CachedSchema
        schema = self.converter.compile_schema(
            destination="inventory.items",
            value_schema={
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
            key_schema={
                "type": "struct",
                "fields": [{"type": "int64", "field": "id"}],
            },
        )
        self.assertEqual(schema.primary_keys, ("id",))
        cached_entry = self.converter._cache["inventory.items"]
        assert cached_entry.key_handler is not None
        self.assertEqual(
            cached_entry.value_handler.convert_json({"id": 5, "dt": "2024-01-15"}),
            {"id": 5, "dt": datetime.date(2024, 1, 15)},
        )
        self.assertEqual(cached_entry.key_handler.convert_json({"id": 5}), {"id": 5})

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

        # Schemaless envelope UPDATE with None in before and int64 in after preserves raw PyArrow types
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
            r_env_upd.schema.arrow_schema.field("after").type.field("age").type,
            pa.int64(),
        )

        # _SchemaEventConverter._convert_key with MapTypeHandler
        self.assertEqual(
            JsonEventConverter()._convert_key(
                "t",
                {"k1": 10},
                {
                    "type": "map",
                    "keys": {"type": "string", "optional": False},
                    "values": {"type": "int64", "optional": True},
                },
            ),
            {"k1": 10},
        )

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

        # JSON scalar key payload wraps into {"key": converted_scalar} (SCH-BUG-4)
        scalar_key_rec = self.converter.convert_event(
            MockChangeEvent(
                key=json.dumps({"schema": {"type": "int64"}, "payload": 42}),
                value=json.dumps(
                    {
                        "schema": {
                            "type": "struct",
                            "fields": [{"type": "int64", "field": "id"}],
                        },
                        "payload": {"id": 42},
                    }
                ),
                destination="inventory.scalar_key",
            )
        )
        self.assertEqual(scalar_key_rec.key, {"key": 42})

        # JSON scalar VariableScaleDecimal key payload converts cleanly (with or without explicit 'fields' in key schema)
        vardec_key_b64 = base64.b64encode(
            (12345).to_bytes(4, byteorder="big", signed=True)
        ).decode("ascii")
        for key_schema_dict in (
            {
                "type": "struct",
                "name": "io.debezium.data.VariableScaleDecimal",
                "fields": [
                    {"type": "int32", "field": "scale"},
                    {"type": "bytes", "field": "value"},
                ],
            },
            {
                "type": "struct",
                "name": "io.debezium.data.VariableScaleDecimal",
            },
        ):
            vardec_key_rec = self.converter.convert_event(
                MockChangeEvent(
                    key=json.dumps(
                        {
                            "schema": key_schema_dict,
                            "payload": {"scale": 2, "value": vardec_key_b64},
                        }
                    ),
                    value=json.dumps(
                        {
                            "schema": {
                                "type": "struct",
                                "fields": [{"type": "int64", "field": "id"}],
                            },
                            "payload": {"id": 1},
                        }
                    ),
                    destination="inventory.vardec_scalar_key",
                )
            )
            self.assertEqual(vardec_key_rec.key, {"key": Decimal("123.45")})

        # Malformed dict payload for scalar int64 key fails fast with TypeError instead of becoming None
        with self.assertRaises(TypeError):
            self.converter.convert_event(
                MockChangeEvent(
                    key=json.dumps(
                        {"schema": {"type": "int64"}, "payload": {"wrong": 1}}
                    ),
                    value=json.dumps(
                        {
                            "schema": {
                                "type": "struct",
                                "fields": [{"type": "int64", "field": "id"}],
                            },
                            "payload": {"id": 1},
                        }
                    ),
                    destination="inventory.bad_scalar_key",
                )
            )

        # ChangeRecord tombstone invariant and ChangeBatch fail-fast on missing schema
        with self.assertRaises(ValueError):
            ChangeRecord(
                destination="inventory.orders",
                value={"id": 1},
                kind=EventKind.TOMBSTONE,
            )
        with self.assertRaises(ValueError):
            ChangeBatch(
                records=(
                    ChangeRecord(
                        destination="inventory.orders",
                        value={"id": 1},
                        schema=None,
                        kind=EventKind.DATA,
                    ),
                )
            ).table_batches()

        # FieldSpec.from_connect accepts logical complex types without child schemas (SCH-BUG-6)
        class _MockLogicalConnectSchema:
            def __init__(self, c_type: str, logical_name: str) -> None:
                self._c_type = c_type
                self._logical_name = logical_name

            def type(self) -> Any:
                return type("_T", (), {"name": lambda _s: self._c_type})()

            def name(self) -> str:
                return self._logical_name

            def isOptional(self) -> bool:
                return True

            def doc(self) -> None:
                return None

            def parameters(self) -> None:
                return None

            def fields(self) -> None:
                return None

            def valueSchema(self) -> None:
                return None

            def keySchema(self) -> None:
                return None

        for c_type, logical_name in (
            ("STRUCT", "io.debezium.data.geometry.Point"),
            ("ARRAY", "io.debezium.data.FloatVector"),
            ("MAP", "io.debezium.data.Hstore"),
        ):
            spec = FieldSpec.from_connect(
                _MockLogicalConnectSchema(c_type, logical_name),
                field_name="col",
            )
            self.assertEqual(spec.logical_name, logical_name)

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
            self.converter.compile_schema(
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
            self.converter.compile_schema(
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

        # 2. Non-3/6 digit fractional seconds (1, 2, 4, 5 digits) normalize to microsecond precision
        self.assertEqual(
            TimestampTypeHandler.parse_iso_timestamp(
                "2024-05-01T12:00:00.1Z", tz_aware=True
            ),
            datetime.datetime(
                2024, 5, 1, 12, 0, 0, 100000, tzinfo=datetime.timezone.utc
            ),
        )
        self.assertEqual(
            TimeTypeHandler.parse_iso_time("10:30:00.1234+00:00"),
            datetime.time(10, 30, 0, 123400),
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
            datetime.time(10, 30, 0, 123400),
        )
        self.assertEqual(
            tm_us_h.convert_json("12:30:00.1234+02:00"),
            datetime.time(10, 30, 0, 123400),
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
                "f_point": {
                    "x": 10.0,
                    "y": 20.0,
                    "wkb": base64.b64encode(b"\x01\x02").decode("ascii"),
                    "srid": 4326,
                },
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
        for struct_logical in (
            "io.debezium.data.geometry.Geometry",
            "io.debezium.data.geometry.Geography",
            "io.debezium.data.geometry.Point",
            "io.debezium.data.SparseDoubleVector",
        ):
            compiled = reg.compile_handler(
                FieldSpec(
                    name="col",
                    type_name="struct",
                    logical_name=struct_logical,
                )
            )
            self.assertIsInstance(compiled, StructTypeHandler)
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
            self.assertIsInstance(compiled_arr, ArrayTypeHandler)
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

        # 5. High-precision 38-digit Decimal quantization (SCH-BUG-1) and nested null promotion
        dec38_h = DecimalTypeHandler(precision=38, scale=18)
        high_prec_unscaled = 12345678901234567890123456789012345678
        high_prec_b64 = base64.b64encode(
            high_prec_unscaled.to_bytes(16, "big", signed=True)
        ).decode("ascii")
        expected_dec38 = Decimal("12345678901234567890.123456789012345678")
        self.assertEqual(dec38_h.convert_json(high_prec_b64), expected_dec38)
        self.assertEqual(dec38_h.convert_json(expected_dec38), expected_dec38)

        s_nested_null = pa.schema(
            [
                ("id", pa.int64()),
                ("s", pa.struct([("m", pa.map_(pa.string(), pa.null()))])),
                ("lm", pa.list_(pa.map_(pa.string(), pa.null()))),
            ]
        )
        s_nested_int = pa.schema(
            [
                ("id", pa.int64()),
                ("s", pa.struct([("m", pa.map_(pa.string(), pa.int64()))])),
                ("lm", pa.list_(pa.map_(pa.string(), pa.int64()))),
            ]
        )
        ts_nested_1 = TableSchema(destination="t", arrow_schema=s_nested_null)
        ts_nested_2 = TableSchema(destination="t", arrow_schema=s_nested_int)
        self.assertTrue(ts_nested_2.diff(ts_nested_1).is_compatible)

    def test_base64_decimal_and_binary_collision_safety(self) -> None:
        dec_h = DecimalTypeHandler(precision=10, scale=2)
        for unscaled, expected_b64, expected_dec in (
            (-2_658_824, "1234", Decimal("-26588.24")),
            (-303_689, "+123", Decimal("-3036.89")),
            (-2_757_193, "1e23", Decimal("-27571.93")),
        ):
            wire_b64 = base64.b64encode(
                unscaled.to_bytes(3, byteorder="big", signed=True)
            ).decode("ascii")
            self.assertEqual(wire_b64, expected_b64)
            self.assertEqual(dec_h.convert_json(wire_b64), expected_dec)

        bin_h = BinaryTypeHandler()
        for raw_bytes, expected_b64 in (
            (b"\xd3\x16\x9b", "0xab"),
            (b"\xd3\x10", "0xA="),
        ):
            wire_b64 = base64.b64encode(raw_bytes).decode("ascii")
            self.assertEqual(wire_b64, expected_b64)
            self.assertEqual(bin_h.convert_json(wire_b64), raw_bytes)
        with self.assertRaises(ValueError):
            bin_h.convert_json("0xDEADBEEF")

    def test_causal_tombstone_and_lifecycle_hook_ordering(self) -> None:
        class StatefulOrderHandler(SchemaAwareChangeHandler):
            def __init__(self) -> None:
                super().__init__()
                self.kv_state: dict[int, str] = {}
                self.events_log: list[str] = []

            def on_table_created(self, schema: TableSchema) -> None:
                self.events_log.append(f"created:{schema.destination}")

            def handle_table_batch(self, batch: TableBatch) -> None:
                self.events_log.append(f"batch:{len(batch)}")
                for row in batch.rows:
                    self.kv_state[row["id"]] = row["name"]

            def on_tombstone(self, record: ChangeRecord) -> None:
                assert record.key is not None
                self.events_log.append(f"tombstone:{record.key['id']}")
                self.kv_state.pop(record.key["id"], None)

            def on_schema_change(self, record: ChangeRecord) -> None:
                self.events_log.append("ddl")

            def on_heartbeat(self, record: ChangeRecord) -> None:
                self.events_log.append("heartbeat")

        handler = StatefulOrderHandler()
        handler.configure(DebeziumConfig({}))
        fields = [
            {"type": "int32", "optional": False, "field": "id"},
            {"type": "string", "optional": False, "field": "name"},
        ]
        ev_alice = make_flat_event(
            "inventory.users", fields, {"id": 1, "name": "Alice"}, key={"id": 1}
        )
        ev_ts = MockChangeEvent(
            key=_wrap_json_key({"id": 1}),
            value=None,
            destination="inventory.users",
        )
        ev_bob = make_flat_event(
            "inventory.users", fields, {"id": 1, "name": "Bob"}, key={"id": 1}
        )
        ev_ddl = MockChangeEvent(
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
                        "ddl": "ALTER TABLE users ADD COLUMN c INT",
                        "databaseName": "inventory",
                        "tableChanges": "[]",
                    },
                }
            ),
            destination="inventory",
        )
        ev_hb = MockChangeEvent(
            key=_wrap_json_key({"id": 1}),
            value=json.dumps(
                {
                    "schema": {
                        "type": "struct",
                        "fields": [{"type": "int64", "field": "ts_ms"}],
                    },
                    "payload": {"ts_ms": 1700000000000},
                }
            ),
            destination="__debezium-heartbeat.pg",
        )

        handler.handleJsonBatch([ev_alice, ev_ts, ev_bob, ev_ddl, ev_hb])
        self.assertEqual(handler.kv_state, {1: "Bob"})
        self.assertEqual(
            handler.events_log,
            [
                "created:inventory.users",
                "batch:1",
                "tombstone:1",
                "batch:1",
                "ddl",
                "heartbeat",
            ],
        )

    def test_schemaless_cross_batch_unification_prevents_null_regression(self) -> None:
        class TrackingHandler(SchemaAwareChangeHandler):
            def __init__(self) -> None:
                super().__init__()
                self.evolutions: list[SchemaDiff] = []
                self.batches: list[pa.Table] = []

            def on_schema_evolved(
                self,
                new_schema: TableSchema,
                old_schema: TableSchema,
                diff: SchemaDiff,
            ) -> None:
                self.evolutions.append(diff)

            def handle_table_batch(self, batch: TableBatch) -> None:
                self.batches.append(batch.to_arrow())

        handler = TrackingHandler()
        handler.configure(DebeziumConfig({"converter.schemas.enable": "false"}))

        for row in (
            {"id": 1, "email": "alice@example.com"},
            {"id": 2, "email": None},
            {"id": 3, "email": "bob@example.com"},
        ):
            handler.handleJsonBatch(
                [
                    MockChangeEvent(
                        key=json.dumps({"id": row["id"]}),
                        value=json.dumps(row),
                        destination="users",
                    )
                ]
            )
        self.assertEqual(handler.evolutions, [])
        for tbl in handler.batches:
            self.assertEqual(tbl.schema.field("email").type, pa.string())

        # Envelope INSERT followed by DELETE in separate batches never produces pa.null() struct fields
        env_handler = TrackingHandler()
        env_handler.configure(DebeziumConfig({"converter.schemas.enable": "false"}))
        env_handler.handleJsonBatch(
            [
                MockChangeEvent(
                    key=json.dumps({"id": 1}),
                    value=json.dumps(
                        {"before": None, "after": {"id": 1, "name": "A"}, "op": "c"}
                    ),
                    destination="orders",
                )
            ]
        )
        env_handler.handleJsonBatch(
            [
                MockChangeEvent(
                    key=json.dumps({"id": 1}),
                    value=json.dumps(
                        {"before": {"id": 1, "name": "A"}, "after": None, "op": "d"}
                    ),
                    destination="orders",
                )
            ]
        )
        self.assertEqual(len(env_handler.evolutions), 1)
        self.assertTrue(env_handler.evolutions[0].is_compatible)
        with tempfile.TemporaryDirectory() as tmpdir:
            pq_path = Path(tmpdir) / "orders_del.parquet"
            pq.write_table(env_handler.batches[1], pq_path)
            self.assertEqual(pq.read_table(pq_path).num_rows, 1)

        # Breaking type change (int64 -> string) updates schema with is_compatible=False
        handler.handleJsonBatch(
            [
                MockChangeEvent(
                    key=json.dumps({"id": 4}),
                    value=json.dumps({"id": "not_an_int", "email": "c@example.com"}),
                    destination="users",
                )
            ]
        )
        self.assertEqual(len(handler.evolutions), 1)
        self.assertFalse(handler.evolutions[0].is_compatible)

    def test_non_nullable_fields_nested_metadata_and_logical_wire_validation(
        self,
    ) -> None:
        # 1. Non-nullable SchemaField rejects None in JSON and Connect modes (including VariableScaleDecimal)
        req_int = SchemaField("id", IntTypeHandler(pa.int32()), nullable=False)
        with self.assertRaises(ValueError):
            req_int.convert_json(None)
        with self.assertRaises(ValueError):
            req_int.convert_connect(None)

        req_vardec = SchemaField(
            "amount", VariableScaleDecimalHandler(10, 2), nullable=False
        )
        with self.assertRaises(ValueError):
            req_vardec.convert_json({"scale": 2, "value": None})

        # 2. ArrayTypeHandler & MapTypeHandler preserve child nullability and field metadata
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

        map_h = MapTypeHandler(
            SchemaField("key", StringTypeHandler(), nullable=True, doc="kdoc"),
            SchemaField(
                "value", IntTypeHandler(pa.int32()), nullable=False, doc="vdoc"
            ),
        )
        self.assertFalse(map_h.arrow_type.key_field.nullable)
        self.assertEqual(map_h.arrow_type.key_field.metadata[b"doc"], b"kdoc")
        self.assertFalse(map_h.arrow_type.item_field.nullable)
        self.assertEqual(map_h.arrow_type.item_field.metadata[b"doc"], b"vdoc")
        with self.assertRaises(ValueError):
            map_h.convert_json({"a": None})

        # 3. Geometry, SparseDoubleVector, and Vector nullability match Debezium 3.6.3
        self.assertFalse(TypeRegistry.GEOMETRY_HANDLER.arrow_type.field("wkb").nullable)
        self.assertTrue(TypeRegistry.GEOMETRY_HANDLER.arrow_type.field("srid").nullable)
        self.assertFalse(
            TypeRegistry.SPARSE_VECTOR_HANDLER.arrow_type.field("dimensions").nullable
        )
        self.assertFalse(
            TypeRegistry.SPARSE_VECTOR_HANDLER.arrow_type.field("vector").nullable
        )
        for vec_name in (
            "io.debezium.data.FloatVector",
            "io.debezium.data.DoubleVector",
        ):
            vec_h = TypeRegistry.FIXED_LOGICAL_HANDLERS[vec_name]
            assert isinstance(vec_h, ArrayTypeHandler)
            self.assertFalse(vec_h.arrow_type.value_field.nullable)

        # 4. TypeRegistry rejects mismatched wire primitive types on logical types
        reg = TypeRegistry()
        for bad_spec in (
            FieldSpec("u", "array", logical_name="io.debezium.data.Uuid"),
            FieldSpec(
                "d",
                "struct",
                logical_name="org.apache.kafka.connect.data.Decimal",
                parameters={"scale": "2"},
            ),
            FieldSpec(
                "vd",
                "bytes",
                logical_name="io.debezium.data.VariableScaleDecimal",
            ),
            FieldSpec("p", "string", logical_name="io.debezium.data.geometry.Point"),
        ):
            with self.assertRaises(ValueError):
                reg.compile_handler(bad_spec)

        # 5. FieldSpec immutability & hashing, tombstone key cache, and Connect None SourceRecord TypeError
        fs1 = FieldSpec("x", "bytes", parameters={"scale": "2", "precision": "10"})
        fs2 = FieldSpec("x", "bytes", parameters={"precision": "10", "scale": "2"})
        self.assertEqual(fs1, fs2)
        self.assertEqual(hash(fs1), hash(fs2))
        with self.assertRaises(TypeError):
            fs1.parameters["scale"] = "4"  # type: ignore[index]

        ts_conv = JsonEventConverter()
        for pk in (1, 2):
            ts_conv.convert_event(
                MockChangeEvent(
                    key=_wrap_json_key({"id": pk}),
                    value=None,
                    destination="unseen.table",
                )
            )
        self.assertIn("unseen.table", ts_conv._key_cache)

        conn_conv = ConnectEventConverter()
        with self.assertRaises(TypeError):
            conn_conv.convert_event(
                MockChangeEvent(key="k", value=None, destination="connect.bad")
            )

        # 6. TimeTypeHandler bounds validation
        tm_ms = TimeTypeHandler(unit="ms")
        with self.assertRaises(ValueError):
            tm_ms.convert_json(-1)
        with self.assertRaises(ValueError):
            tm_ms.convert_json(86_400_000)

    def test_handler_hook_failure_preserves_atomic_schema_state_for_retry(self) -> None:
        class FlakyCatalogHandler(SchemaAwareChangeHandler):
            def __init__(self) -> None:
                super().__init__(converter=JsonEventConverter())
                self.fail_create_once = True
                self.fail_evolve_once = True
                self.created: list[str] = []
                self.evolved: list[SchemaDiff] = []
                self.batches: list[list[dict[str, Any]]] = []

            def on_table_created(self, schema: TableSchema) -> None:
                if self.fail_create_once:
                    self.fail_create_once = False
                    raise RuntimeError("Transient catalog unavailability on create")
                self.created.append(schema.destination)

            def on_schema_evolved(
                self,
                new_schema: TableSchema,
                old_schema: TableSchema,
                diff: SchemaDiff,
            ) -> None:
                if self.fail_evolve_once:
                    self.fail_evolve_once = False
                    raise RuntimeError("Transient catalog unavailability on evolve")
                self.evolved.append(diff)

            def handle_table_batch(self, batch: TableBatch) -> None:
                self.batches.append(batch.rows)

        handler = FlakyCatalogHandler()
        v1_fields = [{"type": "int32", "optional": False, "field": "id"}]
        v2_fields = [
            {"type": "int64", "optional": False, "field": "id"},
            {"type": "string", "optional": True, "field": "email"},
        ]
        ev_v1 = make_flat_event("inventory.orders", v1_fields, {"id": 1}, key={"id": 1})
        ev_v2 = make_flat_event(
            "inventory.orders",
            v2_fields,
            {"id": 2, "email": "a@example.com"},
            key={"id": 2},
        )

        # First create attempt fails -> tracked schema must remain None so retry calls on_table_created
        with self.assertRaisesRegex(RuntimeError, "on create"):
            handler.handle_batch([ev_v1])
        self.assertIsNone(handler.get_schema("inventory.orders"))
        self.assertEqual(handler.batches, [])

        # Retry create succeeds
        handler.handle_batch([ev_v1])
        v1_schema = handler.get_schema("inventory.orders")
        assert v1_schema is not None
        self.assertEqual(handler.created, ["inventory.orders"])
        self.assertEqual(handler.batches, [[{"id": 1}]])

        # First evolve attempt fails -> tracked schema must remain v1_schema so retry calls on_schema_evolved
        with self.assertRaisesRegex(RuntimeError, "on evolve"):
            handler.handle_batch([ev_v2])
        self.assertEqual(handler.get_schema("inventory.orders"), v1_schema)
        self.assertEqual(handler.evolved, [])

        # Retry evolve succeeds
        handler.handle_batch([ev_v2])
        self.assertEqual(len(handler.evolved), 1)
        self.assertTrue(handler.evolved[0].is_compatible)
        self.assertEqual(
            handler.batches, [[{"id": 1}], [{"id": 2, "email": "a@example.com"}]]
        )

    def test_schemaless_partial_breaking_change_sparse_columns_and_lifecycle_skip(
        self,
    ) -> None:
        conv = SchemalessJsonEventConverter()
        b1 = conv.convert(
            [
                MockChangeEvent(
                    key=json.dumps({"id": 1}),
                    value=json.dumps(
                        {
                            "id": 1,
                            "email": "alice@example.com",
                            "phone": "555-0100",
                            "meta": {"tier": "gold", "notes": "vip"},
                        }
                    ),
                    destination="users",
                )
            ]
        )
        s1 = b1.records[0].schema
        assert s1 is not None

        # Batch 2: 'id' undergoes breaking change (int64 -> string), 'email' is null,
        # 'phone' is omitted (sparse JSON), 'meta.notes' is null, plus heartbeat & DDL events!
        hb_ev = MockChangeEvent(
            key=json.dumps({"server": 1}),
            value=json.dumps({"ts_ms": 1700000000000}),
            destination="__debezium-heartbeat.pg",
        )
        ddl_ev = MockChangeEvent(
            key=json.dumps({"databaseName": "inventory"}),
            value=json.dumps(
                {
                    "ddl": "ALTER TABLE users ALTER COLUMN id TYPE TEXT",
                    "databaseName": "inventory",
                    "tableChanges": "[]",
                }
            ),
            destination="inventory",
        )
        b2 = conv.convert(
            [
                hb_ev,
                ddl_ev,
                MockChangeEvent(
                    key=json.dumps({"id": "usr_2"}),
                    value=json.dumps(
                        {
                            "id": "usr_2",
                            "email": None,
                            "meta": {"tier": "silver", "notes": None},
                        }
                    ),
                    destination="users",
                ),
            ]
        )
        self.assertEqual(b2.records[0].kind, EventKind.HEARTBEAT)
        self.assertIsNone(b2.records[0].schema)
        self.assertEqual(b2.records[1].kind, EventKind.SCHEMA_CHANGE)
        self.assertIsNone(b2.records[1].schema)
        self.assertNotIn("__debezium-heartbeat.pg", conv._schemas)
        self.assertNotIn("inventory", conv._schemas)

        s2 = b2.records[2].schema
        assert s2 is not None
        self.assertEqual(s2.arrow_schema.field("id").type, pa.string())
        self.assertEqual(s2.arrow_schema.field("email").type, pa.string())
        self.assertEqual(s2.arrow_schema.field("phone").type, pa.string())
        self.assertEqual(
            s2.arrow_schema.field("meta").type.field("notes").type, pa.string()
        )
        diff = s2.diff(s1)
        self.assertFalse(diff.is_compatible)

        tbl = b2.table_batches()[0].to_arrow()
        self.assertEqual(
            tbl.to_pylist(),
            [
                {
                    "id": "usr_2",
                    "email": None,
                    "phone": None,
                    "meta": {"tier": "silver", "notes": None},
                }
            ],
        )

    def test_tombstone_preserves_active_schema_cache_and_handles_non_struct_tombstone_schema(
        self,
    ) -> None:
        conv = JsonEventConverter()
        fields = [
            {"type": "int64", "optional": False, "field": "id"},
            {"type": "string", "optional": True, "field": "name"},
        ]
        ev_data = make_flat_event(
            "inventory.orders", fields, {"id": 1, "name": "Order-1"}, key={"id": 1}
        )
        r1 = conv.convert_event(ev_data)
        cached_active = conv._cache["inventory.orders"]

        for ts_value, expect_schema in (
            (None, False),
            (
                json.dumps(
                    {"schema": {"type": "bytes", "optional": True}, "payload": None}
                ),
                False,
            ),
            (
                json.dumps(
                    {"schema": {"type": "struct", "fields": []}, "payload": None}
                ),
                False,
            ),
            (
                json.dumps(
                    {
                        "schema": {
                            "type": "struct",
                            "name": "inventory.orders.Value",
                            "fields": [],
                        },
                        "payload": None,
                    }
                ),
                False,
            ),
            (
                json.dumps(
                    {
                        "schema": {
                            "type": "struct",
                            "fields": [{"type": "int32", "field": "legacy_id"}],
                        },
                        "payload": None,
                    }
                ),
                True,
            ),
        ):
            ts_rec = conv.convert_event(
                MockChangeEvent(
                    key=_wrap_json_key({"id": 1}),
                    value=ts_value,
                    destination="inventory.orders",
                )
            )
            self.assertEqual(ts_rec.kind, EventKind.TOMBSTONE)
            self.assertEqual(ts_rec.key, {"id": 1})
            self.assertEqual(ts_rec.schema is not None, expect_schema)
            # Active table cache must NEVER be evicted by a tombstone!
            self.assertIs(conv._cache["inventory.orders"], cached_active)

        r2 = conv.convert_event(ev_data)
        self.assertIs(r2.schema, r1.schema)

    def test_slotted_handlers_int_overflow_variable_scale_clamping_and_field_error_context(
        self,
    ) -> None:
        # 1. Zero __dict__ overhead + pickle/copy/deepcopy across SchemaField and all DataTypeHandler subclasses
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

        # 2. IntTypeHandler bit-width bounds validation (JSON and Connect)
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

        # 3. VariableScaleDecimal clamps scale when precision < default_variable_scale
        reg = TypeRegistry(default_variable_scale=10)
        small_vardec = reg.compile_handler(
            FieldSpec(
                name="rate",
                type_name="struct",
                logical_name="io.debezium.data.VariableScaleDecimal",
                parameters={"connect.decimal.precision": "5"},
            )
        )
        self.assertEqual(small_vardec.arrow_type, pa.decimal128(5, 5))
        b64_12345 = base64.b64encode((12345).to_bytes(2, "big", signed=True)).decode(
            "ascii"
        )
        self.assertEqual(
            small_vardec.convert_json({"scale": 5, "value": b64_12345}),
            Decimal("0.12345"),
        )

        # 4. FieldSpec strict validation
        for bad_name, bad_type in (
            ("", "int32"),
            ("   ", "int32"),
            ("x", ""),
            ("x", "  "),
        ):
            with self.assertRaises(ValueError):
                FieldSpec(name=bad_name, type_name=bad_type)
        with self.assertRaises(ValueError):
            FieldSpec.from_json({"field": "x", "type": "string", "name": 123})
        with self.assertRaises(ValueError):
            FieldSpec.from_json({"field": "x", "type": "string", "doc": 456})

        # 5. Actionable nested field-path error context in SchemaField
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

    def test_schemaless_float_stability_decimal_single_rounding_and_missing_key_schema(
        self,
    ) -> None:
        # 1. Schemaless float64 column stays float64 when subsequent batch has whole integers
        conv = SchemalessJsonEventConverter()
        b1 = conv.convert(
            [
                MockChangeEvent(
                    key=json.dumps({"id": 1}),
                    value=json.dumps({"id": 1, "amount": 10.5}),
                    destination="payments",
                )
            ]
        )
        b2 = conv.convert(
            [
                MockChangeEvent(
                    key=json.dumps({"id": 2}),
                    value=json.dumps({"id": 2, "amount": 10}),
                    destination="payments",
                )
            ]
        )
        assert b1.records[0].schema is not None and b2.records[0].schema is not None
        self.assertEqual(b2.records[0].schema, b1.records[0].schema)
        self.assertEqual(
            b2.table_batches()[0].to_arrow().to_pylist(),
            [{"id": 2, "amount": 10.0}],
        )

        # 2. >38-digit unscaled integer rounds once at target scale (no intermediate 38-digit double rounding)
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

        # 3. Missing key schema in _convert_key raises explicit ValueError
        with self.assertRaises(ValueError):
            JsonEventConverter()._convert_key("payments", {"id": 1}, None)


if __name__ == "__main__":
    unittest.main()
