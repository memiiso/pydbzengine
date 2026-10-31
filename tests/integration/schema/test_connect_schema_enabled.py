from __future__ import annotations

import datetime
import threading
from decimal import Decimal

import pyarrow as pa
import pytest

from pydbzengine import DebeziumEngine
from pydbzengine.schema import ChangeRecord, ConnectEventConverter
from tests.integration.schema.base import (
    BaseSchemaIntegrationTest,
    CollectingHandler,
)


@pytest.mark.integration
class TestConnectSchemaEnabled(BaseSchemaIntegrationTest):
    """
    Test 2: CONNECT mode with schemas enabled.
    Tests Java Kafka Connect SourceRecord extraction, schema dictionary compilation,
    Java object unwrapping, and Arrow table compilation across all 38+ PostgreSQL data types.
    """

    def test_connect_snapshot_all_types(self) -> None:
        props, _ = self.create_isolated_connector_config()
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="connect")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        converter = ConnectEventConverter()
        batch = converter.convert(records)
        self.assertEqual(len(batch.records), 1)
        record = batch.records[0]
        self.assertIsInstance(record, ChangeRecord)
        assert isinstance(record, ChangeRecord)
        table_schema, envelope_row = record.schema, record.row
        assert table_schema is not None
        assert envelope_row is not None

        # 1. Primary Key
        self.assertEqual(table_schema.primary_keys, ("col_id",))

        # 2. Envelope & Nested 'after' PyArrow Field Assertions
        envelope_schema = table_schema.arrow_schema
        after_struct = envelope_schema.field("after").type
        self.assertTrue(pa.types.is_struct(after_struct))
        self.assertEqual(after_struct.field("col_id").type, pa.int32())
        self.assertEqual(after_struct.field("col_smallint").type, pa.int16())
        self.assertEqual(after_struct.field("col_int").type, pa.int32())
        self.assertEqual(after_struct.field("col_bigint").type, pa.int64())
        self.assertEqual(after_struct.field("col_real").type, pa.float32())
        self.assertEqual(after_struct.field("col_double").type, pa.float64())
        self.assertEqual(
            after_struct.field("col_numeric_fixed").type, pa.decimal128(12, 4)
        )
        self.assertTrue(pa.types.is_decimal(after_struct.field("col_numeric_var").type))
        self.assertEqual(after_struct.field("col_boolean").type, pa.bool_())
        self.assertEqual(after_struct.field("col_char").type, pa.string())
        self.assertEqual(after_struct.field("col_varchar").type, pa.string())
        self.assertEqual(after_struct.field("col_text").type, pa.string())
        self.assertEqual(after_struct.field("col_bytea").type, pa.binary())
        self.assertEqual(after_struct.field("col_date").type, pa.date32())
        self.assertEqual(after_struct.field("col_time_micros").type, pa.time64("us"))
        self.assertEqual(
            after_struct.field("col_timestamp_micros").type, pa.timestamp("us")
        )
        self.assertEqual(
            after_struct.field("col_timestamptz").type, pa.timestamp("us", tz="UTC")
        )
        self.assertEqual(after_struct.field("col_uuid").type, pa.string())
        self.assertEqual(after_struct.field("col_json").type, pa.string())
        self.assertEqual(after_struct.field("col_jsonb").type, pa.string())
        self.assertEqual(after_struct.field("col_xml").type, pa.string())
        self.assertEqual(after_struct.field("col_enum").type, pa.string())

        # Complex / Nested, Geospatial & Vector Types
        self.assertEqual(after_struct.field("col_int_array").type, pa.list_(pa.int32()))
        self.assertEqual(
            after_struct.field("col_text_array").type, pa.list_(pa.string())
        )
        self.assertEqual(
            after_struct.field("col_bool_array").type, pa.list_(pa.bool_())
        )
        self.assertTrue(pa.types.is_struct(after_struct.field("col_point").type))
        self.assertTrue(pa.types.is_struct(after_struct.field("col_geometry").type))
        self.assertTrue(pa.types.is_struct(after_struct.field("col_geography").type))
        self.assertEqual(
            after_struct.field("col_float_vector").type, pa.list_(pa.float64())
        )
        self.assertEqual(
            after_struct.field("col_half_vector").type, pa.list_(pa.float32())
        )
        self.assertTrue(
            pa.types.is_struct(after_struct.field("col_sparse_vector").type)
        )
        self.assertEqual(after_struct.field("col_tsvector").type, pa.string())
        self.assertEqual(after_struct.field("col_hstore").type, pa.string())
        self.assertEqual(after_struct.field("col_ltree").type, pa.string())

        # 3. Unwrapped Java Values in Row
        row = envelope_row["after"]
        self.assertEqual(row["col_smallint"], 32767)
        self.assertEqual(row["col_int"], 2147483647)
        self.assertEqual(row["col_bigint"], 9223372036854775807)
        self.assertEqual(row["col_numeric_fixed"], Decimal("12345678.1234"))
        self.assertEqual(row["col_boolean"], True)
        self.assertEqual(row["col_char"], "ABCDE")
        self.assertEqual(row["col_varchar"], "Hello Debezium")
        self.assertEqual(row["col_bytea"], b"\xde\xad\xbe\xef")
        self.assertEqual(row["col_uuid"], "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
        self.assertEqual(row["col_enum"], "active")
        self.assertEqual(row["col_int_array"], [1, 2, 3])
        self.assertEqual(row["col_text_array"], ["apple", "banana"])
        self.assertEqual(row["col_bool_array"], [True, False])

        # Struct, Geometry, Vector, Tsvector, and Hstore
        self.assertEqual(row["col_point"]["x"], 1.5)
        self.assertEqual(row["col_point"]["y"], 2.5)
        self.assertEqual(row["col_geometry"]["srid"], 4326)
        self.assertIsInstance(row["col_geometry"]["wkb"], bytes)
        self.assertEqual(row["col_geography"]["srid"], 4326)
        self.assertIsInstance(row["col_geography"]["wkb"], bytes)
        self.assertEqual(row["col_float_vector"], [1.5, 2.5, 3.5])
        self.assertEqual(row["col_half_vector"], [0.5, 1.5, 2.5])
        self.assertEqual(row["col_sparse_vector"]["dimensions"], 5)
        self.assertEqual(row["col_sparse_vector"]["vector"], {1: 1.5, 3: 2.5})
        self.assertIn("cat", row["col_tsvector"])
        self.assertIn('"k1":"v1"', str(row["col_hstore"]))

        # 4. End-to-End Arrow Table Construction & Materialized Temporal Types
        arrow_table = batch.table_batches()[0].to_arrow()
        self.assertEqual(arrow_table.num_rows, 1)
        self.assertEqual(arrow_table.num_columns, len(envelope_schema))
        arrow_row = arrow_table.to_pylist()[0]["after"]
        self.assertEqual(arrow_row["col_date"], datetime.date(2024, 3, 15))
        self.assertEqual(
            arrow_row["col_timestamp_micros"],
            datetime.datetime(2024, 3, 15, 14, 30, 0, 123456),
        )

    def test_connect_streaming_update_cdc_event(self) -> None:
        """Verifies Connect CDC streaming update events mutate rows and preserve schema."""
        props, _ = self.create_isolated_connector_config()
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=2)
        engine = DebeziumEngine(properties=props, handler=handler, format="connect")

        def _trigger_update() -> None:
            handler.first_event_received.wait(timeout=15.0)
            self.execute_on_source_db(
                f"UPDATE {self.TABLE_NAME} SET col_varchar = 'Connect Stream Update' WHERE col_id = 1;"
            )

        update_thread = threading.Thread(target=_trigger_update, daemon=True)
        update_thread.start()

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 2)

        converter = ConnectEventConverter()
        batch = converter.convert(records)
        self.assertEqual(len(batch.records), 2)
        table_batches = batch.table_batches()
        self.assertEqual(len(table_batches), 1)
        tb = table_batches[0]
        self.assertIsNotNone(tb.schema)
        self.assertEqual(len(tb.rows), 2)
        self.assertEqual(tb.rows[-1]["after"]["col_varchar"], "Connect Stream Update")

        tbl = tb.to_arrow()
        self.assertEqual(tbl.num_rows, 2)
