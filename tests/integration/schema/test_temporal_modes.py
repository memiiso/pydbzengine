from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

from pydbzengine import DebeziumEngine
from pydbzengine.schema import SchemaConverter
from tests.integration.schema.base import (
    BaseSchemaIntegrationTest,
    CollectingHandler,
)


@pytest.mark.integration
class TestTemporalModes(BaseSchemaIntegrationTest):
    """
    Test 3: Temporal precision modes.
    Tests Debezium connector configs:
    - time.precision.mode = "adaptive"
    - time.precision.mode = "connect"
    - time.precision.mode = "adaptive_time_microseconds"
    Verifies that SchemaConverter compiles exact PyArrow temporal resolutions
    and Python date/time/datetime values.
    """

    def test_temporal_mode_adaptive(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={"time.precision.mode": "adaptive"}
        )
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        converter = SchemaConverter()
        extracted = converter.extract_event(records[0])
        assert extracted is not None
        table_schema, _ = extracted

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertEqual(after_struct.field("col_date").type, pa.date32())
        self.assertEqual(after_struct.field("col_time_micros").type, pa.time64("us"))
        self.assertEqual(after_struct.field("col_time_millis").type, pa.time32("ms"))
        self.assertEqual(
            after_struct.field("col_timestamp_micros").type, pa.timestamp("us")
        )
        self.assertEqual(
            after_struct.field("col_timestamp_millis").type, pa.timestamp("ms")
        )

        tbl = converter.to_arrow(records)
        assert tbl is not None
        self.assertEqual(tbl.num_rows, 1)
        arrow_row = tbl.to_pylist()[0]["after"]
        self.assertEqual(arrow_row["col_date"], datetime.date(2024, 3, 15))
        self.assertEqual(arrow_row["col_time_micros"], datetime.time(14, 30, 0, 123456))
        self.assertEqual(arrow_row["col_time_millis"], datetime.time(14, 30, 0, 123000))
        self.assertEqual(
            arrow_row["col_timestamp_micros"],
            datetime.datetime(2024, 3, 15, 14, 30, 0, 123456),
        )
        self.assertEqual(
            arrow_row["col_timestamp_millis"],
            datetime.datetime(2024, 3, 15, 14, 30, 0, 123000),
        )

    def test_temporal_mode_connect(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={"time.precision.mode": "connect"}
        )
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        converter = SchemaConverter()
        extracted = converter.extract_event(records[0])
        assert extracted is not None
        table_schema, _ = extracted

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertEqual(after_struct.field("col_date").type, pa.date32())
        self.assertEqual(after_struct.field("col_time_micros").type, pa.time32("ms"))
        self.assertEqual(after_struct.field("col_time_millis").type, pa.time32("ms"))
        self.assertEqual(
            after_struct.field("col_timestamp_micros").type, pa.timestamp("ms")
        )
        self.assertEqual(
            after_struct.field("col_timestamp_millis").type, pa.timestamp("ms")
        )

        tbl = converter.to_arrow(records)
        assert tbl is not None
        self.assertEqual(tbl.num_rows, 1)
        arrow_row = tbl.to_pylist()[0]["after"]
        self.assertEqual(arrow_row["col_date"], datetime.date(2024, 3, 15))
        self.assertEqual(arrow_row["col_time_millis"], datetime.time(14, 30, 0, 123000))
        self.assertEqual(
            arrow_row["col_timestamp_millis"],
            datetime.datetime(2024, 3, 15, 14, 30, 0, 123000),
        )

    def test_temporal_mode_adaptive_time_microseconds(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={"time.precision.mode": "adaptive_time_microseconds"}
        )
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        converter = SchemaConverter()
        extracted = converter.extract_event(records[0])
        assert extracted is not None
        table_schema, _ = extracted

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertEqual(after_struct.field("col_time_micros").type, pa.time64("us"))
        self.assertEqual(after_struct.field("col_time_millis").type, pa.time64("us"))

        tbl = converter.to_arrow(records)
        assert tbl is not None
        self.assertEqual(tbl.num_rows, 1)
        arrow_row = tbl.to_pylist()[0]["after"]
        self.assertEqual(arrow_row["col_time_micros"], datetime.time(14, 30, 0, 123456))
        self.assertEqual(arrow_row["col_time_millis"], datetime.time(14, 30, 0, 123000))
