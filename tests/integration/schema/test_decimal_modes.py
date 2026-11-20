from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pytest

from pydbzengine import DebeziumEngine
from pydbzengine.schema import ChangeRecord, JsonEventConverter
from tests.integration.schema.base import (
    BaseSchemaIntegrationTest,
    CollectingHandler,
)


@pytest.mark.integration
class TestDecimalModes(BaseSchemaIntegrationTest):
    """
    Decimal handling modes against real PostgreSQL CDC streams:
    - decimal.handling.mode = "precise" (BASE64 and NUMERIC wire formats)
    - decimal.handling.mode = "double"
    - decimal.handling.mode = "string"
    """

    def test_decimal_mode_precise(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={"decimal.handling.mode": "precise"}
        )
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        converter = JsonEventConverter()
        batch = converter.convert(records)
        self.assertEqual(len(batch.records), 1)
        rec = batch.records[0]
        assert isinstance(rec, ChangeRecord)
        table_schema, env_row = rec.schema, rec.value
        assert table_schema is not None
        assert env_row is not None

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertEqual(
            after_struct.field("col_numeric_fixed").type, pa.decimal128(12, 4)
        )
        self.assertTrue(pa.types.is_decimal(after_struct.field("col_numeric_var").type))

        row = env_row["after"]
        self.assertIsInstance(row["col_numeric_fixed"], Decimal)
        self.assertEqual(row["col_numeric_fixed"], Decimal("12345678.1234"))
        self.assertEqual(row["col_numeric_var"], Decimal("999999.99999"))

        tbl = batch.table_batches()[0].to_arrow()
        self.assertEqual(tbl.num_rows, 1)
        self.assertEqual(
            tbl.to_pylist()[0]["after"]["col_numeric_fixed"], Decimal("12345678.1234")
        )

    def test_decimal_mode_precise_json_numeric_format_high_precision(self) -> None:
        """
        Verifies Kafka Connect JsonConverter decimal.format=NUMERIC preserves >17-digit
        positive, negative, and variable-scale NUMERIC precision end-to-end without
        IEEE-754 float truncation.
        """
        table_name = "test_decimal_numeric_format"
        exact_pos = Decimal("12345678901234567890.1234567890")
        exact_neg = Decimal("-98765432109876543210.9876543210")
        exact_var = Decimal("1234567890123456789.1234567890")
        self.execute_on_source_db(
            f"""
            DROP TABLE IF EXISTS public.{table_name};
            CREATE TABLE public.{table_name} (
                id SERIAL PRIMARY KEY,
                amount_pos NUMERIC(30, 10) NOT NULL,
                amount_neg NUMERIC(30, 10) NOT NULL,
                amount_var NUMERIC
            );
            INSERT INTO public.{table_name} (amount_pos, amount_neg, amount_var)
            VALUES ({exact_pos}, {exact_neg}, {exact_var});
            """
        )
        self.addCleanup(
            self.execute_on_source_db, f"DROP TABLE IF EXISTS public.{table_name};"
        )

        props, _ = self.create_isolated_connector_config(
            custom_props={
                "table.include.list": f"public.{table_name}",
                "decimal.handling.mode": "precise",
                "converter.decimal.format": "NUMERIC",
                "value.converter.decimal.format": "NUMERIC",
            }
        )
        handler = CollectingHandler(target_table=table_name, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)
        raw_json = str(records[0].value())
        self.assertIn("12345678901234567890.1234567890", raw_json)
        self.assertIn("-98765432109876543210.9876543210", raw_json)

        batch = JsonEventConverter().convert(records)
        rec = batch.records[0]
        assert rec.schema is not None
        assert rec.value is not None
        after_struct = rec.schema.arrow_schema.field("after").type
        self.assertEqual(after_struct.field("amount_pos").type, pa.decimal128(30, 10))
        self.assertEqual(after_struct.field("amount_neg").type, pa.decimal128(30, 10))
        self.assertTrue(pa.types.is_decimal(after_struct.field("amount_var").type))
        self.assertEqual(rec.value["after"]["amount_pos"], exact_pos)
        self.assertEqual(rec.value["after"]["amount_neg"], exact_neg)
        self.assertEqual(rec.value["after"]["amount_var"], exact_var)

        tbl = batch.table_batches()[0].to_arrow()
        arrow_after = tbl.to_pylist()[0]["after"]
        self.assertEqual(arrow_after["amount_pos"], exact_pos)
        self.assertEqual(arrow_after["amount_neg"], exact_neg)
        self.assertEqual(arrow_after["amount_var"], exact_var)

    def test_decimal_mode_double(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={"decimal.handling.mode": "double"}
        )
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        converter = JsonEventConverter()
        batch = converter.convert(records)
        self.assertEqual(len(batch.records), 1)
        rec = batch.records[0]
        assert isinstance(rec, ChangeRecord)
        table_schema, env_row = rec.schema, rec.value
        assert table_schema is not None
        assert env_row is not None

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertEqual(after_struct.field("col_numeric_fixed").type, pa.float64())
        self.assertEqual(after_struct.field("col_numeric_var").type, pa.float64())

        row = env_row["after"]
        self.assertIsInstance(row["col_numeric_fixed"], float)
        self.assertAlmostEqual(row["col_numeric_fixed"], 12345678.1234, places=2)

        tbl = batch.table_batches()[0].to_arrow()
        self.assertEqual(tbl.num_rows, 1)

    def test_decimal_mode_string(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={"decimal.handling.mode": "string"}
        )
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        converter = JsonEventConverter()
        batch = converter.convert(records)
        self.assertEqual(len(batch.records), 1)
        rec = batch.records[0]
        assert isinstance(rec, ChangeRecord)
        table_schema, env_row = rec.schema, rec.value
        assert table_schema is not None
        assert env_row is not None

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertEqual(after_struct.field("col_numeric_fixed").type, pa.string())
        self.assertEqual(after_struct.field("col_numeric_var").type, pa.string())

        row = env_row["after"]
        self.assertIsInstance(row["col_numeric_fixed"], str)
        self.assertEqual(row["col_numeric_fixed"], "12345678.1234")

        tbl = batch.table_batches()[0].to_arrow()
        self.assertEqual(tbl.num_rows, 1)
