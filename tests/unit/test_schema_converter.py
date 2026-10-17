from __future__ import annotations

import base64
import datetime
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from pydbzengine.engine.base import BasePythonChangeHandler, ChangeEvent
from pydbzengine.schema import (
    SchemaConverter,
    is_heartbeat,
    is_schema_change,
    is_tombstone,
    unify_arrow_schemas,
)
from pydbzengine.schema.coercers import (
    coerce_binary,
    coerce_date_iso,
    coerce_time_iso,
    coerce_timestamp_iso_naive,
    coerce_timestamp_iso_utc,
    decode_decimal,
    decode_variable_scale_decimal,
)
from tests.support import MockChangeEvent


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
    key_str = (
        json.dumps(key)
        if isinstance(key, dict)
        else (key if key is not None else json.dumps({"id": 1}))
    )
    return MockChangeEvent(
        key=key_str,
        value=json.dumps(val_json),
        destination=dest,
    )


def make_envelope_event(
    dest: str,
    fields: list[dict[str, Any]],
    after: dict[str, Any] | None,
    key: dict[str, Any] | str | None = None,
    op: str = "c",
    before: dict[str, Any] | None = None,
) -> MockChangeEvent:
    val_json = {
        "schema": {
            "type": "struct",
            "name": f"{dest}.Envelope",
            "fields": [
                {
                    "type": "struct",
                    "field": "before",
                    "fields": fields,
                    "optional": True,
                },
                {
                    "type": "struct",
                    "field": "after",
                    "fields": fields,
                    "optional": True,
                },
                {"type": "string", "field": "op", "optional": False},
                {"type": "int64", "field": "ts_ms", "optional": True},
            ],
        },
        "payload": {
            "before": before,
            "after": after,
            "op": op,
            "ts_ms": 1620000000000,
        },
    }
    key_str = (
        json.dumps(key)
        if isinstance(key, dict)
        else (key if key is not None else json.dumps({"id": 1}))
    )
    return MockChangeEvent(
        key=key_str,
        value=json.dumps(val_json),
        destination=dest,
    )


class TestSchemaConverterAndCoercers(unittest.TestCase):
    def setUp(self) -> None:
        self.converter = SchemaConverter()

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
                    "fields": [{"type": "int32", "field": "id"}],
                },
                "payload": {"id": 1001},
            },
        )

        extracted = self.converter.extract_event(rec)
        self.assertIsNotNone(extracted)
        assert extracted is not None
        table_schema, row = extracted
        self.assertEqual(table_schema.destination, "inventory.customers")
        self.assertEqual(table_schema.primary_keys, ("id",))
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

        coerced_cols = [c for c, _ in table_schema.row_coercers]
        self.assertEqual(coerced_cols, ["updated_at", "salary"])

        pa_table = self.converter.to_arrow(rec)
        self.assertIsNotNone(pa_table)
        assert pa_table is not None
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

        tbl = self.converter.to_arrow([create_rec, delete_rec])
        self.assertIsNotNone(tbl)
        assert tbl is not None
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

        # 1. Slicing by schema version via to_arrow_batches()
        batches = self.converter.to_arrow_batches(records)
        self.assertEqual(len(batches), 2)
        s1, t1 = batches[0]
        s2, t2 = batches[1]
        self.assertEqual(t1.num_rows, 2)
        self.assertEqual(s1.arrow_schema.field("amount").type, pa.int32())
        self.assertEqual(t2.num_rows, 1)
        self.assertEqual(s2.arrow_schema.field("amount").type, pa.int64())
        self.assertIn("discount_code", s2)

        # 2. Additive unification via to_arrow()
        unified_table = self.converter.to_arrow(records)
        self.assertIsNotNone(unified_table)
        assert unified_table is not None
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
        fields = [
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
                "f_point": {"x": 1.5, "y": 2.5, "srid": 4326},
                "f_tags": ["a", "b"],
            },
        )
        pa_table = self.converter.to_arrow([rec])
        self.assertIsNotNone(pa_table)
        assert pa_table is not None

        # Verify strict resolution alignment in schema
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
        self.assertEqual(row["f_point"], {"x": 1.5, "y": 2.5, "srid": 4326})
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
                logical_name: str | None = None,
                params: dict | None = None,
            ) -> None:
                self._type = MockConnectType(type_name)
                self._logical_name = logical_name
                self._params = params or {}

            def type(self) -> MockConnectType:
                return self._type

            def name(self) -> str | None:
                return self._logical_name

            def isOptional(self) -> bool:
                return True

            def doc(self) -> str | None:
                return "doc"

            def parameters(self) -> dict:
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

        class MockStruct:
            def get(self, name: str) -> Any:
                return {
                    "id": 7,
                    "amount": "12.50",
                    "col_default": "default_substituted",
                    "col_struct": MockNestedStruct(),
                }[name]

            def getWithoutDefault(self, name: str) -> Any:
                return {
                    "id": 7,
                    "amount": "12.50",
                    "col_default": None,
                    "col_struct": MockNestedStruct(),
                }[name]

        val_schema = MockValueSchema()

        class ConnectRecord(MockChangeEvent):
            def valueSchema(self) -> MockValueSchema:
                return val_schema

            def keySchema(self) -> Any:
                class KS:
                    def fields(self) -> list[MockField]:
                        return [MockField("id", MockFieldSchema("INT64"))]

                return KS()

        rec = ConnectRecord(key=None, value=MockStruct(), destination="connect.orders")
        connect_converter = SchemaConverter(format="connect")
        self.assertTrue(connect_converter.config.is_connect)
        schema, rows = connect_converter.to_rows([rec, rec])
        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(schema.primary_keys, ("id",))
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

    def test_coercers_38_digit_decimal_and_error_cases(self) -> None:
        val_38 = 12345678901234567890123456789012345678
        raw_bytes = val_38.to_bytes(
            (val_38.bit_length() + 8) // 8, byteorder="big", signed=True
        )
        b64_val = base64.b64encode(raw_bytes).decode("ascii")
        self.assertEqual(
            decode_decimal(b64_val, scale=10),
            Decimal("1234567890123456789012345678.9012345678"),
        )
        self.assertEqual(decode_decimal("1000", scale=2), Decimal("1000"))
        self.assertEqual(coerce_binary("0xdeadbeef"), b"\xde\xad\xbe\xef")
        self.assertEqual(
            coerce_date_iso("2026-09-07T12:00:00.123456789Z"), datetime.date(2026, 9, 7)
        )
        self.assertEqual(
            coerce_time_iso("12:00:00.123456789Z"),
            datetime.time(12, 0, 0, 123456, tzinfo=datetime.timezone.utc),
        )
        self.assertEqual(
            coerce_timestamp_iso_naive("2026-09-07T12:00:00Z"),
            datetime.datetime(2026, 9, 7, 12, 0, 0),
        )
        self.assertEqual(
            coerce_timestamp_iso_utc("2026-09-07T12:00:00Z"),
            datetime.datetime(2026, 9, 7, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )

        with self.assertRaises(ValueError):
            decode_decimal("   ", scale=2)
        with self.assertRaises(TypeError):
            decode_decimal(object(), scale=2)
        with self.assertRaises(ValueError):
            decode_variable_scale_decimal({"scale": 2})
        with self.assertRaises(TypeError):
            decode_variable_scale_decimal("not_a_mapping")

    def test_strict_converter_errors_on_unexpected_inputs(self) -> None:
        with self.assertRaises(ValueError):
            self.converter.extract_event(
                MockChangeEvent(key="{}", value="{}", destination="")
            )

        with self.assertRaises(ValueError):
            self.converter.extract_event(
                MockChangeEvent(key="{}", value='{"id": 1}', destination="t")
            )

        with self.assertRaises(ValueError):
            self.converter.extract_event(
                MockChangeEvent(key="{}", value="[1, 2, 3]", destination="t")
            )

        schemaless_conv = SchemaConverter(schemas_enabled=False)
        with self.assertRaises(ValueError):
            schemaless_conv.extract_event(
                MockChangeEvent(
                    key="plain_string_key", value='{"id": 1}', destination="t"
                )
            )
        with self.assertRaises(ValueError):
            schemaless_conv.extract_event(
                MockChangeEvent(key="123", value='{"id": 1}', destination="t")
            )

        with self.assertRaises(ValueError):
            self.converter.compile_connect_schema(
                "t",
                {"type": "struct", "fields": [{"type": "unknown_type", "field": "x"}]},
            )

        with self.assertRaises(ValueError):
            self.converter.compile_connect_schema(
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

    def test_end_user_handler_slicing_batches_with_to_arrow_batches(self) -> None:
        class LakehouseParquetHandler(BasePythonChangeHandler):
            def __init__(self, base_dir: Path) -> None:
                self.base_dir = base_dir
                self.converter = SchemaConverter()
                self.target_schemas: dict[str, pa.Schema] = {}
                self.written_files: list[Path] = []

            def handle_batch(self, records: list[ChangeEvent]) -> int | None:
                for dest, table_records in self.group_by_destination(records).items():
                    for schema, batch_tbl in self.converter.to_arrow_batches(
                        table_records
                    ):
                        if dest not in self.target_schemas:
                            self.target_schemas[dest] = schema.arrow_schema
                        else:
                            self.target_schemas[dest] = unify_arrow_schemas(
                                [self.target_schemas[dest], schema.arrow_schema]
                            )
                        aligned = schema.to_arrow(
                            batch_tbl.to_pylist(),
                            coerce=False,
                            target_schema=self.target_schemas[dest],
                        )
                        out_file = (
                            self.base_dir
                            / f"{schema.table_name}_{len(self.written_files)}.parquet"
                        )
                        pq.write_table(aligned, out_file)
                        self.written_files.append(out_file)
                return len(records)

        with tempfile.TemporaryDirectory() as tmpdir:
            handler = LakehouseParquetHandler(Path(tmpdir))

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

            handler.handleJsonBatch([rec1, rec2])

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

    def test_predicate_helpers_and_connect_date_unwrapping(self) -> None:
        class MockJavaUtilDate:
            def getTime(self) -> int:
                return 19118 * 86_400_000

        unwrapped_date = SchemaConverter._unwrap_java_value(
            MockJavaUtilDate(),
            logical_name="org.apache.kafka.connect.data.Date",
        )
        self.assertEqual(coerce_date_iso(unwrapped_date), 19118)

        heartbeat_rec = MockChangeEvent(
            key='{"id": 1}',
            value='{"ts_ms": 1620000000000}',
            destination="__debezium-heartbeat.postgres",
        )
        ddl_rec = MockChangeEvent(
            key='{"databaseName": "inventory"}',
            value=json.dumps(
                {
                    "ddl": "ALTER TABLE customers ADD COLUMN age INT",
                    "databaseName": "inventory",
                    "tableChanges": [],
                }
            ),
            destination="inventory",
        )
        tombstone_rec = MockChangeEvent(
            key='{"id": 1}',
            value=None,
            destination="inventory.customers",
        )
        tombstone_json_rec = MockChangeEvent(
            key='{"id": 1}',
            value='{"schema": {"type": "struct", "fields": []}, "payload": null}',
            destination="inventory.customers",
        )

        self.assertTrue(is_heartbeat(heartbeat_rec))
        self.assertTrue(is_heartbeat("__debezium-heartbeat.postgres"))
        self.assertFalse(is_heartbeat(ddl_rec))

        self.assertTrue(is_schema_change(ddl_rec))
        self.assertTrue(
            is_schema_change({"ddl": "x", "databaseName": "d", "tableChanges": []})
        )
        self.assertFalse(is_schema_change(heartbeat_rec))

        self.assertTrue(is_tombstone(tombstone_rec))
        self.assertTrue(is_tombstone(tombstone_json_rec))
        self.assertFalse(is_tombstone(heartbeat_rec))
        self.assertIsNone(self.converter.to_arrow([tombstone_rec, tombstone_json_rec]))

        # Map key cannot be nested type
        with self.assertRaises(ValueError):
            self.converter.compile_connect_schema(
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
            self.converter.compile_connect_schema(
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

    def test_java_map_key_extraction(self) -> None:
        class MockJavaMapKey:
            def keySet(self) -> list[str]:
                return ["id", "org_id"]

        map_key_rec = MockChangeEvent(
            key=MockJavaMapKey(),  # type: ignore[arg-type]
            value='{"id": 10, "org_id": 20}',
            destination="inventory.orgs",
        )
        self.assertEqual(
            SchemaConverter(format="connect").extract_primary_keys(map_key_rec),
            ["id", "org_id"],
        )

    def test_schemaless_json_mode_none_in_first_row_and_null_guard(self) -> None:
        conv = SchemaConverter(schemas_enabled=False)
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
        schema, rows = conv.to_rows([r1, r2])
        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(schema.primary_keys, ("id",))
        self.assertEqual(schema.arrow_schema.field("age").type, pa.int64())
        self.assertEqual(schema.arrow_schema.field("note").type, pa.string())
        self.assertEqual(len(rows), 2)

        tbl = conv.to_arrow([r1, r2])
        self.assertIsNotNone(tbl)
        assert tbl is not None
        self.assertEqual(tbl.num_rows, 2)
        self.assertEqual(tbl.column("age").to_pylist(), [None, 25])


if __name__ == "__main__":
    unittest.main()
