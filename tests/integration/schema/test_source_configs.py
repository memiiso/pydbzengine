from __future__ import annotations

import pyarrow as pa
import pytest

from pydbzengine import DebeziumEngine
from pydbzengine.schema import ConverterConfig, SchemaConverter
from tests.integration.schema.base import (
    BaseSchemaIntegrationTest,
    CollectingHandler,
)


@pytest.mark.integration
class TestSourceConfigs(BaseSchemaIntegrationTest):
    """
    Test 5: Source system configurations.
    Tests connector configs:
    - hstore.handling.mode (map vs json)
    - interval.handling.mode (numeric vs string)
    - SMT unwrap flattening (ExtractNewRecordState)
    """

    def test_hstore_handling_modes(self) -> None:
        """Tests hstore handling modes: map vs json."""
        # 1. Hstore Mode: Map
        props_map, _ = self.create_isolated_connector_config(
            custom_props={"hstore.handling.mode": "map"}
        )
        handler_map = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine_map = DebeziumEngine(
            properties=props_map, handler=handler_map, format="json"
        )

        records = self.run_engine_and_collect(engine_map, handler_map, timeout_sec=30.0)
        converter = SchemaConverter()
        extracted_map = converter.extract_event(records[0])
        assert extracted_map is not None
        table_schema, env_row = extracted_map

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertTrue(pa.types.is_map(after_struct.field("col_hstore").type))
        self.assertEqual(env_row["after"]["col_hstore"], {"k1": "v1", "k2": "v2"})

        # 2. Hstore Mode: JSON string
        props_json, _ = self.create_isolated_connector_config(
            custom_props={"hstore.handling.mode": "json"}
        )
        handler_json = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine_json = DebeziumEngine(
            properties=props_json, handler=handler_json, format="json"
        )

        records_json = self.run_engine_and_collect(
            engine_json, handler_json, timeout_sec=30.0
        )
        extracted_json = converter.extract_event(records_json[0])
        assert extracted_json is not None
        table_schema_json, env_row_json = extracted_json

        after_struct_json = table_schema_json.arrow_schema.field("after").type
        self.assertEqual(after_struct_json.field("col_hstore").type, pa.string())
        self.assertIn("k1", str(env_row_json["after"]["col_hstore"]))

    def test_interval_handling_modes(self) -> None:
        """Tests interval handling modes: numeric (microseconds) vs string."""
        # 1. Interval: Numeric
        props_num, _ = self.create_isolated_connector_config(
            custom_props={"interval.handling.mode": "numeric"}
        )
        handler_num = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine_num = DebeziumEngine(
            properties=props_num, handler=handler_num, format="json"
        )

        records = self.run_engine_and_collect(engine_num, handler_num, timeout_sec=30.0)
        converter = SchemaConverter()
        extracted_num = converter.extract_event(records[0])
        assert extracted_num is not None
        table_schema, env_row = extracted_num

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertEqual(after_struct.field("col_interval").type, pa.int64())
        self.assertIsInstance(env_row["after"]["col_interval"], int)

        # 2. Interval: String
        props_str, _ = self.create_isolated_connector_config(
            custom_props={"interval.handling.mode": "string"}
        )
        handler_str = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine_str = DebeziumEngine(
            properties=props_str, handler=handler_str, format="json"
        )

        records_str = self.run_engine_and_collect(
            engine_str, handler_str, timeout_sec=30.0
        )
        extracted_str = converter.extract_event(records_str[0])
        assert extracted_str is not None
        table_schema_str, env_row_str = extracted_str

        after_struct_str = table_schema_str.arrow_schema.field("after").type
        self.assertEqual(after_struct_str.field("col_interval").type, pa.string())
        self.assertIsInstance(env_row_str["after"]["col_interval"], str)

    def test_smt_unwrap_flattening(self) -> None:
        """Verifies Debezium ExtractNewRecordState SMT produces flattened top-level schema."""
        props, _ = self.create_isolated_connector_config(unwrap_messages=True)
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        cfg = ConverterConfig.from_properties(props)
        converter = SchemaConverter(cfg)
        extracted = converter.extract_event(records[0])
        assert extracted is not None
        table_schema, row = extracted

        arrow_schema = table_schema.arrow_schema
        self.assertIn("col_id", arrow_schema.names)
        self.assertIn("__op", arrow_schema.names)
        self.assertIn("__table", arrow_schema.names)
        self.assertEqual(row.get("__op"), "r")
        self.assertEqual(row.get("__table"), self.TABLE_NAME)

        tbl = converter.to_arrow(records)
        assert tbl is not None
        self.assertEqual(tbl.num_rows, 1)
