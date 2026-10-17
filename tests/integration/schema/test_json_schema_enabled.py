from __future__ import annotations

import datetime
import threading
from decimal import Decimal

import pyarrow as pa
import pytest

from pydbzengine import DebeziumEngine
from pydbzengine.schema import SchemaConverter
from tests.integration.schema.base import (
    BaseSchemaIntegrationTest,
    CollectingHandler,
)


@pytest.mark.integration
class TestJsonSchemaEnabled(BaseSchemaIntegrationTest):
    """
    Test 1: JSON mode with schemas enabled.
    Tests end-to-end schema extraction, type coercion, and Arrow table compilation
    across all 38+ PostgreSQL data types.
    """

    def test_json_snapshot_all_types_schema_and_coercion(self) -> None:
        props, _ = self.create_isolated_connector_config()
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        record = records[0]
        converter = SchemaConverter()
        extracted = converter.extract_event(record)
        self.assertIsNotNone(extracted)
        assert extracted is not None
        table_schema, envelope_row = extracted

        # 1. Primary Key
        self.assertEqual(table_schema.primary_keys, ("col_id",))

        # 2. Envelope & Nested 'after' PyArrow Field Assertions
        envelope_schema = table_schema.arrow_schema
        self.assertIn("before", envelope_schema.names)
        self.assertIn("after", envelope_schema.names)
        self.assertIn("op", envelope_schema.names)

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
        self.assertEqual(after_struct.field("col_bit").type, pa.binary())
        self.assertEqual(after_struct.field("col_bit_varying").type, pa.binary())
        self.assertEqual(after_struct.field("col_date").type, pa.date32())
        self.assertEqual(after_struct.field("col_time_micros").type, pa.time64("us"))
        self.assertEqual(after_struct.field("col_time_millis").type, pa.time32("ms"))
        self.assertEqual(after_struct.field("col_timetz").type, pa.time64("us"))
        self.assertEqual(
            after_struct.field("col_timestamp_micros").type, pa.timestamp("us")
        )
        self.assertEqual(
            after_struct.field("col_timestamp_millis").type, pa.timestamp("ms")
        )
        self.assertEqual(
            after_struct.field("col_timestamptz").type, pa.timestamp("us", tz="UTC")
        )
        self.assertEqual(after_struct.field("col_interval").type, pa.int64())
        self.assertEqual(after_struct.field("col_uuid").type, pa.string())
        self.assertEqual(after_struct.field("col_json").type, pa.string())
        self.assertEqual(after_struct.field("col_jsonb").type, pa.string())
        self.assertEqual(after_struct.field("col_xml").type, pa.string())
        self.assertEqual(after_struct.field("col_enum").type, pa.string())

        # Complex / Nested
        self.assertEqual(after_struct.field("col_int_array").type, pa.list_(pa.int32()))
        self.assertEqual(
            after_struct.field("col_text_array").type, pa.list_(pa.string())
        )
        self.assertEqual(
            after_struct.field("col_bool_array").type, pa.list_(pa.bool_())
        )
        self.assertEqual(after_struct.field("col_hstore").type, pa.string())
        self.assertEqual(after_struct.field("col_ltree").type, pa.string())

        # 3. Coerced Row Values Assertions
        row = envelope_row["after"]
        self.assertEqual(row["col_smallint"], 32767)
        self.assertEqual(row["col_int"], 2147483647)
        self.assertEqual(row["col_bigint"], 9223372036854775807)
        self.assertAlmostEqual(row["col_real"], 3.14, places=2)
        self.assertAlmostEqual(row["col_double"], 2.7182818284, places=6)
        self.assertEqual(row["col_numeric_fixed"], Decimal("12345678.1234"))
        self.assertEqual(row["col_numeric_var"], Decimal("999999.99999"))
        self.assertEqual(row["col_boolean"], True)
        self.assertEqual(row["col_char"], "ABCDE")
        self.assertEqual(row["col_varchar"], "Hello Debezium")
        self.assertEqual(row["col_bytea"], b"\xde\xad\xbe\xef")
        self.assertEqual(row["col_uuid"], "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
        self.assertEqual(row["col_enum"], "active")
        self.assertEqual(row["col_int_array"], [1, 2, 3])
        self.assertEqual(row["col_text_array"], ["apple", "banana"])
        self.assertEqual(row["col_bool_array"], [True, False])

        # Struct and Hstore
        self.assertEqual(row["col_point"]["x"], 1.5)
        self.assertEqual(row["col_point"]["y"], 2.5)
        self.assertIn('"k1":"v1"', str(row["col_hstore"]))

        # 4. End-to-End Arrow Table Construction & Materialized Temporal Types
        arrow_table = converter.to_arrow(records)
        self.assertIsNotNone(arrow_table)
        assert arrow_table is not None
        self.assertEqual(arrow_table.num_rows, 1)
        self.assertEqual(arrow_table.num_columns, len(envelope_schema))
        arrow_row = arrow_table.to_pylist()[0]["after"]
        self.assertEqual(arrow_row["col_date"], datetime.date(2024, 3, 15))
        self.assertEqual(
            arrow_row["col_timestamp_micros"],
            datetime.datetime(2024, 3, 15, 14, 30, 0, 123456),
        )

    def test_json_streaming_update_cdc_event(self) -> None:
        """Verifies CDC streaming update events mutate rows and preserve schema."""
        props, _ = self.create_isolated_connector_config()
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=2)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        def _trigger_update() -> None:
            handler.first_event_received.wait(timeout=15.0)
            self.execute_on_source_db(
                f"UPDATE {self.TABLE_NAME} SET col_varchar = 'Updated Value' WHERE col_id = 1;"
            )

        update_thread = threading.Thread(target=_trigger_update, daemon=True)
        update_thread.start()

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 2)

        converter = SchemaConverter()
        schema, rows = converter.to_rows(records)
        self.assertIsNotNone(schema)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[-1]["after"]["col_varchar"], "Updated Value")

        tbl = converter.to_arrow(records)
        self.assertIsNotNone(tbl)
        assert tbl is not None
        self.assertEqual(tbl.num_rows, 2)
