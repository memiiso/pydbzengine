from __future__ import annotations

import threading

import pyarrow as pa
import pytest

from pydbzengine import DebeziumEngine
from pydbzengine.schema import (
    ChangeOp,
    ChangeRecord,
    EventKind,
    SchemalessJsonEventConverter,
)
from tests.integration.schema.base import (
    BaseSchemaIntegrationTest,
    CollectingHandler,
)


@pytest.mark.integration
class TestSchemalessJson(BaseSchemaIntegrationTest):
    """
    Integration tests for SchemalessJsonEventConverter against a real PostgreSQL
    source database with converter.schemas.enable = false.
    """

    def test_schemaless_json_snapshot_all_types_and_envelope_symmetry(self) -> None:
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

        converter = SchemalessJsonEventConverter()
        batch = converter.convert(records)
        self.assertEqual(len(batch.records), 1)
        record = batch.records[0]
        self.assertIsInstance(record, ChangeRecord)
        self.assertEqual(record.kind, EventKind.DATA)
        self.assertEqual(record.op, ChangeOp.READ)

        table_schema = record.schema
        assert table_schema is not None
        self.assertIsNone(table_schema.value_handler)
        self.assertEqual(table_schema.primary_keys, ("col_id",))

        # Envelope symmetry: even though 'before' is None on snapshot READ,
        # SchemalessJsonEventConverter mirrors the 'after' struct type onto 'before'.
        envelope_schema = table_schema.arrow_schema
        before_type = envelope_schema.field("before").type
        after_type = envelope_schema.field("after").type
        self.assertTrue(pa.types.is_struct(before_type))
        self.assertTrue(pa.types.is_struct(after_type))
        self.assertEqual(before_type, after_type)

        data = record.data
        assert data is not None
        self.assertEqual(data["col_smallint"], 32767)
        self.assertEqual(data["col_int"], 2147483647)
        self.assertEqual(data["col_boolean"], True)
        self.assertEqual(data["col_varchar"], "Hello Debezium")
        self.assertEqual(data["col_int_array"], [1, 2, 3])
        self.assertEqual(data["col_float_vector"], [1.5, 2.5, 3.5])
        self.assertEqual(data["col_point"]["x"], 1.5)
        self.assertEqual(data["col_geometry"]["srid"], 4326)
        self.assertIn("cat", data["col_tsvector"])

        # Materialize both envelope and unpacked table rows into PyArrow Tables
        tb = batch.table_batches()[0]
        envelope_table = tb.to_arrow()
        self.assertEqual(envelope_table.num_rows, 1)

        unpacked_table = tb.to_arrow(extract_rows=True)
        self.assertEqual(unpacked_table.num_rows, 1)
        self.assertIn("col_id", unpacked_table.schema.names)
        self.assertEqual(unpacked_table.to_pylist()[0]["col_varchar"], "Hello Debezium")

    def test_schemaless_json_streaming_update_and_null_promotion(self) -> None:
        """Verifies schemaless JSON null sanitization on initial row and concrete promotion on update."""
        table_name = "test_schemaless_null_promo"
        self.execute_on_source_db(
            f"""
            DROP TABLE IF EXISTS public.{table_name};
            CREATE TABLE public.{table_name} (
                id SERIAL PRIMARY KEY,
                optional_note VARCHAR(100)
            );
            INSERT INTO public.{table_name} (optional_note) VALUES (NULL);
            """
        )
        self.addCleanup(
            self.execute_on_source_db, f"DROP TABLE IF EXISTS public.{table_name};"
        )

        props, _ = self.create_isolated_connector_config(
            custom_props={
                "table.include.list": f"public.{table_name}",
                "converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                "key.converter.schemas.enable": "false",
            }
        )
        handler = CollectingHandler(target_table=table_name, expected_count=2)
        engine = DebeziumEngine(properties=props, handler=handler, format="json")

        def _trigger_update() -> None:
            handler.first_event_received.wait(timeout=15.0)
            self.execute_on_source_db(
                f"UPDATE public.{table_name} SET optional_note = 'now populated' WHERE id = 1;"
            )

        update_thread = threading.Thread(target=_trigger_update, daemon=True)
        update_thread.start()

        records = self.run_engine_and_collect(engine, handler, timeout_sec=30.0)
        self.assertGreaterEqual(len(records), 2)

        converter = SchemalessJsonEventConverter()
        batch1 = converter.convert([records[0]])
        schema1 = batch1.records[0].schema
        assert schema1 is not None
        # All-null column in batch 1 is sanitized to pa.string() with debezium.inferred_null metadata
        note_field1 = schema1.row_schema.field("optional_note")
        self.assertEqual(note_field1.type, pa.string())
        assert note_field1.metadata is not None
        self.assertEqual(note_field1.metadata.get(b"debezium.inferred_null"), b"true")

        batch2 = converter.convert([records[1]])
        schema2 = batch2.records[0].schema
        assert schema2 is not None
        note_field2 = schema2.row_schema.field("optional_note")
        self.assertEqual(note_field2.type, pa.string())
        self.assertEqual(batch2.records[0].op, ChangeOp.UPDATE)
        assert batch2.records[0].data is not None
        self.assertEqual(batch2.records[0].data["optional_note"], "now populated")
