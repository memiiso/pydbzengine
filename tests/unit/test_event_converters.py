from __future__ import annotations

import base64
import datetime
import json
import unittest
from decimal import Decimal
from typing import Any

import pyarrow as pa

from pydbzengine import EventFormat
from pydbzengine.schema import (
    ConnectEventConverter,
    EventKind,
    JsonEventConverter,
)
from tests.support import MockChangeEvent


def wrap_json_key(key: dict[str, Any] | str | None) -> str:
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
        key=wrap_json_key(key),
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
        key=wrap_json_key(key),
        value=json.dumps(val_json),
        destination=dest,
    )


class TestEventConverters(unittest.TestCase):
    def setUp(self) -> None:
        self.converter = JsonEventConverter()

    def test_json_flattened_event_extraction_and_to_arrow(self) -> None:
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
        assert table_schema is not None and row is not None
        self.assertEqual(table_schema.destination, "inventory.customers")
        self.assertEqual(table_schema.key_fields, ("id",))
        self.assertIsNotNone(table_schema.key_schema)
        assert table_schema.key_schema is not None
        self.assertEqual(table_schema.key_schema.field("id").type, pa.int32())
        self.assertEqual(record.key, {"id": 1001})

        pa_table = batch.table_batches()[0].to_arrow()
        out_row = pa_table.to_pylist()[0]
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

    def test_json_numeric_decimal_preserves_full_precision_over_17_digits(self) -> None:
        raw_json = json.dumps(
            {
                "schema": {
                    "type": "struct",
                    "fields": [
                        {
                            "type": "bytes",
                            "optional": False,
                            "name": "org.apache.kafka.connect.data.Decimal",
                            "parameters": {
                                "scale": "6",
                                "connect.decimal.precision": "26",
                            },
                            "field": "amount",
                        },
                        {"type": "double", "optional": False, "field": "ratio"},
                    ],
                },
                "payload": {"amount": 0, "ratio": 0.25},
            }
        ).replace('"amount": 0', '"amount": 12345678901234567890.123456')

        rec = self.converter.convert_event(
            MockChangeEvent(
                key=wrap_json_key({"id": 1}),
                value=raw_json,
                destination="finance.ledger",
            )
        )
        assert rec.value is not None
        self.assertEqual(rec.value["amount"], Decimal("12345678901234567890.123456"))
        self.assertIsInstance(rec.value["ratio"], float)
        self.assertEqual(rec.value["ratio"], 0.25)

    def test_json_pure_envelope_pass_through_and_mid_batch_evolution(self) -> None:
        v1_fields = [
            {"type": "int32", "optional": False, "field": "id"},
            {"type": "int32", "optional": True, "field": "amount"},
        ]
        v2_fields = [
            {"type": "int32", "optional": False, "field": "id"},
            {"type": "int64", "optional": True, "field": "amount"},
            {"type": "string", "optional": True, "field": "discount_code"},
        ]
        env_batch = self.converter.convert(
            [
                make_envelope_event(
                    "inventory.orders", v1_fields, after={"id": 1, "amount": 10}, op="c"
                ),
                make_envelope_event(
                    "inventory.orders",
                    v1_fields,
                    before={"id": 1, "amount": 10},
                    after=None,
                    op="d",
                ),
            ]
        )
        env_tbl = env_batch.table_batches()[0].to_arrow()
        self.assertEqual(env_tbl.schema.names, ["before", "after", "op", "ts_ms"])
        self.assertEqual(env_tbl.num_rows, 2)

        # Mid-batch evolution splits into two TableBatches and unifies cleanly
        batch = self.converter.convert(
            [
                make_flat_event("orders", v1_fields, {"id": 1, "amount": 100}),
                make_flat_event("orders", v1_fields, {"id": 2, "amount": 200}),
                make_flat_event(
                    "orders",
                    v2_fields,
                    {"id": 3, "amount": 300, "discount_code": "SAVE10"},
                ),
            ]
        )
        b1, b2 = batch.table_batches()
        unified_schema = pa.unify_schemas(
            [b1.schema.arrow_schema, b2.schema.arrow_schema],
            promote_options="permissive",
        )
        unified = pa.concat_tables(
            [
                b1.to_arrow(target_schema=unified_schema),
                b2.to_arrow(target_schema=unified_schema),
            ]
        )
        self.assertEqual(
            unified.to_pylist(),
            [
                {"id": 1, "amount": 100, "discount_code": None},
                {"id": 2, "amount": 200, "discount_code": None},
                {"id": 3, "amount": 300, "discount_code": "SAVE10"},
            ],
        )

    def test_json_logical_and_structural_wire_types(self) -> None:
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
            {"type": "int32", "name": "io.debezium.time.Time", "field": "f_time"},
            {"type": "int64", "name": "io.debezium.time.Timestamp", "field": "f_ts_ms"},
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
            {"type": "bytes", "name": "io.debezium.data.Bits", "field": "f_bits"},
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
                "keys": {"type": "string", "optional": False},
                "values": {"type": "string", "optional": True},
                "field": "f_hstore",
            },
            {
                "type": "struct",
                "name": "io.debezium.data.geometry.Point",
                "fields": [
                    {"type": "double", "optional": False, "field": "x"},
                    {"type": "double", "optional": False, "field": "y"},
                    {"type": "bytes", "optional": True, "field": "wkb"},
                    {"type": "int32", "optional": True, "field": "srid"},
                ],
                "field": "f_point",
            },
            {
                "type": "array",
                "name": "io.debezium.data.vector.FloatVector",
                "items": {"type": "float", "optional": False},
                "field": "f_vec",
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
                "f_bits": "AQ==",
                "f_vardec": {"scale": 2, "value": vardec_b64},
                "f_hstore": {"k1": "v1"},
                "f_point": {"x": 1.5, "y": 2.5, "wkb": "AQ==", "srid": 4326},
                "f_vec": [1.0, 2.5],
            },
        )
        tbl = self.converter.convert([rec]).table_batches()[0].to_arrow()
        row = tbl.to_pylist()[0]
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
        self.assertEqual(row["f_bits"], b"\x01")
        self.assertEqual(row["f_vardec"], Decimal("123.45"))
        self.assertEqual(row["f_hstore"], [("k1", "v1")])
        self.assertEqual(
            row["f_point"], {"x": 1.5, "y": 2.5, "wkb": b"\x01", "srid": 4326}
        )
        self.assertEqual(row["f_vec"], [1.0, 2.5])

    def test_connect_converter_real_source_record_and_empty_key_schema(self) -> None:
        from pydbzengine.engine.jvm import ensure_jvm_started

        ensure_jvm_started()
        import jpype

        Schema = jpype.JClass("org.apache.kafka.connect.data.Schema")
        SchemaBuilder = jpype.JClass("org.apache.kafka.connect.data.SchemaBuilder")
        Struct = jpype.JClass("org.apache.kafka.connect.data.Struct")
        ConnectDecimal = jpype.JClass("org.apache.kafka.connect.data.Decimal")
        SourceRecord = jpype.JClass("org.apache.kafka.connect.source.SourceRecord")
        BigDecimal = jpype.JClass("java.math.BigDecimal")

        dec_schema = (
            ConnectDecimal.builder(2)
            .parameter("connect.decimal.precision", "10")
            .optional()
            .build()
        )
        val_schema = (
            SchemaBuilder.struct()
            .name("connect.orders.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("amount", dec_schema)
            .field(
                "note",
                SchemaBuilder.string().optional().defaultValue("fallback").build(),
            )
            .build()
        )
        key_schema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build()
        val_struct = Struct(val_schema)
        val_struct.put("id", jpype.JLong(7))
        val_struct.put("amount", BigDecimal("12.50"))
        # Leave 'note' unset -> getWithoutDefault must return None, not "fallback"
        key_struct = Struct(key_schema)
        key_struct.put("id", jpype.JLong(7))

        src_rec = SourceRecord(
            None, None, "connect.orders", key_schema, key_struct, val_schema, val_struct
        )
        conv = ConnectEventConverter()
        self.assertEqual(conv.format, EventFormat.CONNECT)
        batch = conv.convert(
            [MockChangeEvent(key=src_rec, value=src_rec, destination="connect.orders")]
        )
        tb = batch.table_batches()[0]
        self.assertEqual(tb.schema.key_fields, ("id",))
        self.assertEqual(batch.records[0].key, {"id": 7})
        self.assertEqual(tb.rows, [{"id": 7, "amount": Decimal("12.50"), "note": None}])

        # Empty-fields struct keySchema (with null key OR empty Struct key) normalizes to (None, None)
        empty_key_schema = SchemaBuilder.struct().name("connect.keyless.Key").build()
        for raw_key in (None, Struct(empty_key_schema)):
            src_keyless = SourceRecord(
                None,
                None,
                "connect.keyless",
                empty_key_schema,
                raw_key,
                val_schema,
                val_struct,
            )
            rec = conv.convert_event(
                MockChangeEvent(
                    key=src_keyless, value=src_keyless, destination="connect.keyless"
                )
            )
            assert rec.schema is not None
            self.assertEqual(rec.schema.key_fields, ())
            self.assertIsNone(rec.schema.key_schema)
            self.assertIsNone(rec.key)

        # Non-struct .schema() paired with empty-fields struct keySchema is not normalized
        class _NonStructSchemaKey:
            def schema(self) -> Any:
                return Schema.INT32_SCHEMA

        self.assertFalse(
            ConnectEventConverter._is_empty_connect_struct(
                empty_key_schema, _NonStructSchemaKey()
            )
        )

        # Scalar key tombstone in Connect mode
        ts_rec = SourceRecord(
            None,
            None,
            "connect.orders",
            Schema.INT64_SCHEMA,
            jpype.JLong(42),
            None,
            None,
        )
        converted_ts = conv.convert_event(
            MockChangeEvent(key=ts_rec, value=ts_rec, destination="connect.orders")
        )
        self.assertEqual(converted_ts.kind, EventKind.TOMBSTONE)
        self.assertEqual(converted_ts.key, {"key": 42})
        self.assertIsNone(converted_ts.schema)

    def test_keys_tombstones_cache_retention_and_lifecycle_classification(self) -> None:
        # Scalar key, map key, and VariableScaleDecimal key
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

        self.assertEqual(
            self.converter._convert_key(
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

        # Valid key schema with null key payload preserves key_fields
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
        self.assertEqual(null_key_payload_rec.schema.key_fields, ("id",))
        self.assertIsNone(null_key_payload_rec.key)

        # Tombstones never evict active table schema cache
        fields = [
            {"type": "int64", "optional": False, "field": "id"},
            {"type": "string", "optional": True, "field": "name"},
        ]
        ev_data = make_flat_event(
            "inventory.orders", fields, {"id": 1, "name": "Order-1"}, key={"id": 1}
        )
        r1 = self.converter.convert_event(ev_data)
        cached_active = self.converter._cache["inventory.orders"]

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
                            "fields": [{"type": "int32", "field": "legacy_id"}],
                        },
                        "payload": None,
                    }
                ),
                True,
            ),
        ):
            ts_rec = self.converter.convert_event(
                MockChangeEvent(
                    key=wrap_json_key({"id": 1}),
                    value=ts_value,
                    destination="inventory.orders",
                )
            )
            self.assertEqual(ts_rec.kind, EventKind.TOMBSTONE)
            self.assertEqual(ts_rec.key, {"id": 1})
            self.assertEqual(ts_rec.schema is not None, expect_schema)
            self.assertIs(self.converter._cache["inventory.orders"], cached_active)

        r2 = self.converter.convert_event(ev_data)
        self.assertIs(r2.schema, r1.schema)

        # Heartbeat and DDL classification
        hb_rec = MockChangeEvent(
            key=wrap_json_key({"id": 1}),
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
            key=wrap_json_key({"databaseName": "inventory"}),
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
        batch = self.converter.convert([hb_rec, ddl_rec])
        self.assertEqual(
            [r.kind for r in batch.records],
            [EventKind.HEARTBEAT, EventKind.SCHEMA_CHANGE],
        )

    def test_converter_strict_error_handling(self) -> None:
        # Empty destination, missing schema/payload envelope, non-object JSON, whitespace
        for bad_event in (
            MockChangeEvent(key=None, value="{}", destination=""),
            MockChangeEvent(key=None, value='{"id": 1}', destination="t"),
            MockChangeEvent(key=None, value="[1, 2, 3]", destination="t"),
            MockChangeEvent(key=wrap_json_key({"id": 1}), value="   ", destination="t"),
            MockChangeEvent(key="   ", value=None, destination="t"),
            MockChangeEvent(key=None, value=None, destination="t"),
        ):
            with self.assertRaises(ValueError):
                self.converter.convert_event(bad_event)

        # Non-null key with null key schema fails fast without polluting _cache
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

        # Non-string destination and non-int partition fail fast with TypeError
        with self.assertRaises(TypeError):
            self.converter.convert_event(
                MockChangeEvent(
                    key=wrap_json_key({"id": 1}),
                    value=None,
                    destination=123,  # type: ignore[arg-type]
                )
            )
        with self.assertRaises(TypeError):
            self.converter.convert_event(
                MockChangeEvent(
                    key=wrap_json_key({"id": 1}),
                    value=None,
                    destination="t",
                    partition="0",  # type: ignore[arg-type]
                )
            )

        # Connect converter rejects None SourceRecord
        with self.assertRaises(TypeError):
            ConnectEventConverter().convert_event(
                MockChangeEvent(key="k", value=None, destination="connect.bad")
            )

        # Malformed falsy non-mapping key payloads paired with empty struct keySchema fail fast
        bad_falsy_keys: tuple[Any, ...] = (0, False, "", [])
        for bad_falsy_key in bad_falsy_keys:
            with self.assertRaises(ValueError):
                self.converter.convert_event(
                    MockChangeEvent(
                        key={
                            "schema": {"type": "struct", "fields": []},
                            "payload": bad_falsy_key,
                        },
                        value={
                            "schema": {
                                "type": "struct",
                                "fields": [{"type": "int32", "field": "id"}],
                            },
                            "payload": {"id": 1},
                        },
                        destination="inventory.bad_empty_key",
                    )
                )

        # Missing key schema in _convert_key
        with self.assertRaises(ValueError):
            self.converter._convert_key("payments", {"id": 1}, None)


if __name__ == "__main__":
    unittest.main()
