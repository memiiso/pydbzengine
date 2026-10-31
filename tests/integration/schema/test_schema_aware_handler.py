from __future__ import annotations

import threading

import pyarrow as pa
import pytest

from pydbzengine import DebeziumEngine, EventFormat
from pydbzengine.schema import (
    ChangeOp,
    ConnectEventConverter,
    EventKind,
    JsonEventConverter,
    SchemalessJsonEventConverter,
)
from tests.integration.schema.base import (
    BaseSchemaIntegrationTest,
    RecordingSchemaChangeHandler,
)


@pytest.mark.integration
class TestSchemaAwareChangeHandlerIntegration(BaseSchemaIntegrationTest):
    """
    End-to-end integration tests for SchemaAwareChangeHandler consuming live
    PostgreSQL CDC events across all three converter modes:
    1. JSON with schemas enabled (JsonEventConverter)
    2. Connect mode (ConnectEventConverter)
    3. Schemaless JSON mode (SchemalessJsonEventConverter)
    """

    def _create_isolated_crud_table(self, suffix: str) -> str:
        table_name = f"test_consumer_crud_{suffix}"
        self.execute_on_source_db(
            f"""
            DROP TABLE IF EXISTS public.{table_name};
            CREATE TABLE public.{table_name} (
                id SERIAL PRIMARY KEY,
                title VARCHAR(100) NOT NULL,
                amount NUMERIC(10, 2),
                tags TEXT[],
                coords POINT,
                embedding vector(3)
            );
            INSERT INTO public.{table_name} (title, amount, tags, coords, embedding)
            VALUES ('initial_row', 42.50, ARRAY['alpha', 'beta'], POINT(1.0, 2.0), '[1.0, 2.0, 3.0]');
            """
        )
        self.addCleanup(
            self.execute_on_source_db, f"DROP TABLE IF EXISTS public.{table_name};"
        )
        return table_name

    def test_consumer_json_schema_enabled_crud_and_schema_evolution(self) -> None:
        table_name = self._create_isolated_crud_table("json_schema")
        props, _ = self.create_isolated_connector_config(
            custom_props={"table.include.list": f"public.{table_name}"}
        )
        # Expect: 1 READ + 1 UPDATE + 1 INSERT (after ALTER TABLE) + 1 DELETE + 1 TOMBSTONE = 5 records
        handler = RecordingSchemaChangeHandler(
            target_table=table_name,
            expected_count=5,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format="json")
        self.assertIsInstance(handler.converter, JsonEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.JSON)
        self.assertTrue(handler.converter.schemas_enabled)

        def _mutate_db() -> None:
            handler.first_event_received.wait(timeout=15.0)
            self.execute_on_source_db(
                f"""
                UPDATE public.{table_name} SET title = 'updated_row' WHERE id = 1;
                ALTER TABLE public.{table_name} ADD COLUMN extra_flag BOOLEAN DEFAULT true;
                INSERT INTO public.{table_name} (title, amount, tags, coords, embedding, extra_flag)
                VALUES ('evolved_row', 99.95, ARRAY['gamma'], POINT(3.0, 4.0), '[4.0, 5.0, 6.0]', true);
                DELETE FROM public.{table_name} WHERE id = 1;
                """
            )

        threading.Thread(target=_mutate_db, daemon=True).start()
        records = self.run_engine_with_consumer(engine, handler, timeout_sec=35.0)

        # 1. Verify Table Creation and Schema Evolution Hooks
        self.assertEqual(len(handler.created_schemas), 1)
        self.assertEqual(handler.created_schemas[0].primary_keys, ("id",))
        self.assertGreaterEqual(len(handler.evolutions), 1)
        new_schema, old_schema, diff = handler.evolutions[-1]
        self.assertIn("extra_flag", [f.name for f in diff.added_fields])
        self.assertTrue(diff.is_additive)
        self.assertTrue(diff.is_compatible)
        self.assertNotIn("extra_flag", old_schema.row_schema.names)
        self.assertIn("extra_flag", new_schema.row_schema.names)

        # 2. Verify Operations and Tombstone in Arrival Order
        ops = [r.op for r in records if r.kind is EventKind.DATA]
        self.assertIn(ChangeOp.READ, ops)
        self.assertIn(ChangeOp.UPDATE, ops)
        self.assertIn(ChangeOp.INSERT, ops)
        self.assertIn(ChangeOp.DELETE, ops)

        tombstones = [r for r in records if r.kind is EventKind.TOMBSTONE]
        self.assertGreaterEqual(len(tombstones), 1)
        self.assertIsNone(tombstones[0].row)
        self.assertIsNone(tombstones[0].op)
        self.assertEqual(tombstones[0].key, {"id": 1})

        # 3. Verify Materialized Arrow Tables from TableBatches
        self.assertGreaterEqual(len(handler.table_batches), 2)
        unpacked_tables = [
            tb.to_arrow(extract_rows=True) for tb in handler.table_batches
        ]
        self.assertTrue(any("extra_flag" in t.schema.names for t in unpacked_tables))

    def test_consumer_connect_mode_crud_and_schema_evolution(self) -> None:
        table_name = self._create_isolated_crud_table("connect_mode")
        props, _ = self.create_isolated_connector_config(
            custom_props={
                "table.include.list": f"public.{table_name}",
                "format": "connect",
            }
        )
        handler = RecordingSchemaChangeHandler(
            target_table=table_name,
            expected_count=5,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format="connect")
        self.assertIsInstance(handler.converter, ConnectEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.CONNECT)
        self.assertTrue(handler.converter.schemas_enabled)

        def _mutate_db() -> None:
            handler.first_event_received.wait(timeout=15.0)
            self.execute_on_source_db(
                f"""
                UPDATE public.{table_name} SET title = 'connect_updated' WHERE id = 1;
                ALTER TABLE public.{table_name} ADD COLUMN extra_note VARCHAR(50);
                INSERT INTO public.{table_name} (title, amount, tags, coords, embedding, extra_note)
                VALUES ('connect_inserted', 10.00, ARRAY['delta'], POINT(5.0, 6.0), '[7.0, 8.0, 9.0]', 'note1');
                DELETE FROM public.{table_name} WHERE id = 1;
                """
            )

        threading.Thread(target=_mutate_db, daemon=True).start()
        records = self.run_engine_with_consumer(engine, handler, timeout_sec=35.0)

        self.assertEqual(len(handler.created_schemas), 1)
        self.assertGreaterEqual(len(handler.evolutions), 1)
        _, _, diff = handler.evolutions[-1]
        self.assertIn("extra_note", [f.name for f in diff.added_fields])
        self.assertTrue(diff.is_additive)

        ops = [r.op for r in records if r.kind is EventKind.DATA]
        self.assertIn(ChangeOp.READ, ops)
        self.assertIn(ChangeOp.UPDATE, ops)
        self.assertIn(ChangeOp.INSERT, ops)
        self.assertIn(ChangeOp.DELETE, ops)

        tombstones = [r for r in records if r.kind is EventKind.TOMBSTONE]
        self.assertGreaterEqual(len(tombstones), 1)
        self.assertEqual(tombstones[0].key, {"id": 1})

    def test_consumer_schemaless_json_crud_and_schema_evolution(self) -> None:
        table_name = self._create_isolated_crud_table("schemaless_json")
        props, _ = self.create_isolated_connector_config(
            custom_props={
                "table.include.list": f"public.{table_name}",
                "converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                "key.converter.schemas.enable": "false",
            }
        )
        handler = RecordingSchemaChangeHandler(
            target_table=table_name,
            expected_count=4,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format="json")
        self.assertIsInstance(handler.converter, SchemalessJsonEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.JSON)
        self.assertFalse(handler.converter.schemas_enabled)

        def _mutate_db() -> None:
            handler.first_event_received.wait(timeout=15.0)
            self.execute_on_source_db(
                f"""
                ALTER TABLE public.{table_name} ADD COLUMN dynamic_col INTEGER;
                INSERT INTO public.{table_name} (title, amount, tags, coords, embedding, dynamic_col)
                VALUES ('schemaless_row', 75.25, ARRAY['omega'], POINT(7.0, 8.0), '[0.1, 0.2, 0.3]', 42);
                DELETE FROM public.{table_name} WHERE id = 1;
                """
            )

        threading.Thread(target=_mutate_db, daemon=True).start()
        records = self.run_engine_with_consumer(engine, handler, timeout_sec=35.0)

        self.assertEqual(len(handler.created_schemas), 1)
        self.assertGreaterEqual(len(handler.evolutions), 1)
        _, _, diff = handler.evolutions[-1]
        self.assertIn("dynamic_col", [f.name for f in diff.added_fields])

        ops = [r.op for r in records if r.kind is EventKind.DATA]
        self.assertIn(ChangeOp.READ, ops)
        self.assertIn(ChangeOp.INSERT, ops)
        self.assertIn(ChangeOp.DELETE, ops)

        unpacked_tables = [
            tb.to_arrow(extract_rows=True) for tb in handler.table_batches
        ]
        self.assertTrue(
            any(
                "dynamic_col" in t.schema.names
                and pa.types.is_integer(t.schema.field("dynamic_col").type)
                for t in unpacked_tables
            )
        )
