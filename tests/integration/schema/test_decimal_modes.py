from __future__ import annotations

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
class TestDecimalModes(BaseSchemaIntegrationTest):
    """
    Test 4: Decimal handling modes.
    Tests Debezium connector configs:
    - decimal.handling.mode = "precise"
    - decimal.handling.mode = "double"
    - decimal.handling.mode = "string"
    Verifies that SchemaConverter compiles matching Arrow decimal/float/string types
    and properly builds Arrow tables.
    """

    def test_decimal_mode_precise(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={"decimal.handling.mode": "precise"}
        )
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        converter = SchemaConverter()
        extracted = converter.extract_event(records[0])
        assert extracted is not None
        table_schema, env_row = extracted

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertEqual(
            after_struct.field("col_numeric_fixed").type, pa.decimal128(12, 4)
        )
        self.assertTrue(pa.types.is_decimal(after_struct.field("col_numeric_var").type))

        row = env_row["after"]
        self.assertIsInstance(row["col_numeric_fixed"], Decimal)
        self.assertEqual(row["col_numeric_fixed"], Decimal("12345678.1234"))
        self.assertEqual(row["col_numeric_var"], Decimal("999999.99999"))

        tbl = converter.to_arrow(records)
        assert tbl is not None
        self.assertEqual(tbl.num_rows, 1)
        self.assertEqual(
            tbl.to_pylist()[0]["after"]["col_numeric_fixed"], Decimal("12345678.1234")
        )

    def test_decimal_mode_double(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={"decimal.handling.mode": "double"}
        )
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        converter = SchemaConverter()
        extracted = converter.extract_event(records[0])
        assert extracted is not None
        table_schema, env_row = extracted

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertEqual(after_struct.field("col_numeric_fixed").type, pa.float64())
        self.assertEqual(after_struct.field("col_numeric_var").type, pa.float64())

        row = env_row["after"]
        self.assertIsInstance(row["col_numeric_fixed"], float)
        self.assertAlmostEqual(row["col_numeric_fixed"], 12345678.1234, places=2)

        tbl = converter.to_arrow(records)
        assert tbl is not None
        self.assertEqual(tbl.num_rows, 1)

    def test_decimal_mode_string(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={"decimal.handling.mode": "string"}
        )
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        converter = SchemaConverter()
        extracted = converter.extract_event(records[0])
        assert extracted is not None
        table_schema, env_row = extracted

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertEqual(after_struct.field("col_numeric_fixed").type, pa.string())
        self.assertEqual(after_struct.field("col_numeric_var").type, pa.string())

        row = env_row["after"]
        self.assertIsInstance(row["col_numeric_fixed"], str)
        self.assertEqual(row["col_numeric_fixed"], "12345678.1234")

        tbl = converter.to_arrow(records)
        assert tbl is not None
        self.assertEqual(tbl.num_rows, 1)
