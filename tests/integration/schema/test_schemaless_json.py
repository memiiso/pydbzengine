from __future__ import annotations

import json

import pytest

from pydbzengine import DebeziumEngine
from pydbzengine.schema import JsonEventConverter
from tests.integration.schema.base import (
    BaseSchemaIntegrationTest,
    CollectingHandler,
    RecordingSchemaChangeHandler,
)


@pytest.mark.integration
class TestSchemalessJsonRejection(BaseSchemaIntegrationTest):
    """
    Verifies that pydbzengine.schema strictly rejects schemaless JSON streams
    (converter.schemas.enable = false) while BasePythonChangeHandler consumes them directly.
    """

    def test_schema_aware_handler_rejects_schemaless_json_config(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={
                "converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                "key.converter.schemas.enable": "false",
            }
        )
        handler = RecordingSchemaChangeHandler(
            target_table=self.TABLE_NAME, expected_count=1
        )
        with self.assertRaisesRegex(ValueError, "BasePythonChangeHandler"):
            DebeziumEngine(properties=props, handler=handler, format="json")

    def test_json_event_converter_rejects_live_schemaless_json_payloads(self) -> None:
        props, _ = self.create_isolated_connector_config(
            custom_props={
                "converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                "key.converter.schemas.enable": "false",
            }
        )
        handler = CollectingHandler(target_table=self.TABLE_NAME, expected_count=1)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 1)

        raw_payload = json.loads(str(records[0].value()))
        self.assertNotIn("schema", raw_payload)
        self.assertEqual(raw_payload["after"]["col_varchar"], "Hello Debezium")

        converter = JsonEventConverter()
        with self.assertRaisesRegex(
            ValueError, "Missing required 'schema' and 'payload'"
        ):
            converter.convert(records)
