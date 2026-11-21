from __future__ import annotations

import threading

import pyarrow as pa
import pytest

from pydbzengine import DebeziumEngine, EventFormat
from pydbzengine.schema import (
    ConnectEventConverter,
    EventKind,
    JsonEventConverter,
)
from tests.integration.schema.base import (
    BaseSchemaIntegrationTest,
    RecordingSchemaChangeHandler,
)


@pytest.mark.integration
class TestSchemaAwareChangeHandlerIntegration(BaseSchemaIntegrationTest):
    """
    End-to-end integration tests for SchemaAwareChangeHandler consuming live
    PostgreSQL CDC events in both schema-enabled converter modes:
    1. JSON with schemas enabled (JsonEventConverter)
    2. Connect mode (ConnectEventConverter)
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

        # 1. Verify Table Creation and Struct-Aware Schema Evolution Hooks
        self.assertEqual(len(handler.created_schemas), 1)
        self.assertEqual(handler.created_schemas[0].key_fields, ("id",))
        self.assertGreaterEqual(len(handler.evolutions), 1)
        new_schema, old_schema, diff = handler.evolutions[-1]
        self.assertTrue(diff.is_additive)
        self.assertTrue(diff.is_compatible)
        self.assertFalse(diff.key_fields_changed)
        self.assertFalse(diff.key_schema_changed)
        self.assertEqual(diff.removed_fields, ())
        self.assertEqual(diff.added_fields, ())
        old_after = old_schema.arrow_schema.field("after").type
        new_after = new_schema.arrow_schema.field("after").type
        self.assertTrue(pa.types.is_struct(old_after))
        self.assertTrue(pa.types.is_struct(new_after))
        self.assertNotIn("extra_flag", [f.name for f in old_after])
        self.assertIn("extra_flag", [f.name for f in new_after])

        # 2. Verify Operations and Tombstone in Arrival Order
        ops = [
            r.value["op"]
            for r in records
            if r.kind is EventKind.DATA and r.value is not None
        ]
        self.assertIn("r", ops)
        self.assertIn("u", ops)
        self.assertIn("c", ops)
        self.assertIn("d", ops)

        insert_rec = next(
            r
            for r in records
            if r.kind is EventKind.DATA
            and r.value is not None
            and r.value.get("op") == "c"
        )
        assert insert_rec.value is not None
        self.assertEqual(insert_rec.value["after"]["title"], "evolved_row")
        self.assertIs(insert_rec.value["after"]["extra_flag"], True)

        tombstones = [r for r in records if r.kind is EventKind.TOMBSTONE]
        self.assertGreaterEqual(len(tombstones), 1)
        self.assertIsNone(tombstones[0].value)
        self.assertEqual(tombstones[0].key, {"id": 1})

        # 3. Verify Materialized Arrow Tables and Struct-Evolution Alignment
        self.assertGreaterEqual(len(handler.table_batches), 2)
        self.assertTrue(
            any(
                "extra_flag" in [f.name for f in t.schema.field("after").type]
                for t in handler.arrow_tables
            )
        )
        aligned_initial = handler.table_batches[0].to_arrow(
            target_schema=new_schema.arrow_schema
        )
        self.assertEqual(aligned_initial.schema, new_schema.arrow_schema)
        self.assertIsNone(aligned_initial.to_pylist()[0]["after"]["extra_flag"])

    def test_consumer_connect_mode_crud_and_schema_evolution(self) -> None:
        table_name = self._create_isolated_crud_table("connect_mode")
        props, _ = self.create_isolated_connector_config(
            custom_props={"table.include.list": f"public.{table_name}"}
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
        self.assertEqual(handler.created_schemas[0].key_fields, ("id",))
        self.assertGreaterEqual(len(handler.evolutions), 1)
        new_schema, old_schema, diff = handler.evolutions[-1]
        self.assertTrue(diff.is_additive)
        self.assertTrue(diff.is_compatible)
        self.assertFalse(diff.key_fields_changed)
        self.assertFalse(diff.key_schema_changed)
        self.assertEqual(diff.removed_fields, ())
        self.assertEqual(diff.added_fields, ())
        old_after = old_schema.arrow_schema.field("after").type
        new_after = new_schema.arrow_schema.field("after").type
        self.assertTrue(pa.types.is_struct(old_after))
        self.assertTrue(pa.types.is_struct(new_after))
        self.assertNotIn("extra_note", [f.name for f in old_after])
        self.assertIn("extra_note", [f.name for f in new_after])

        ops = [
            r.value["op"]
            for r in records
            if r.kind is EventKind.DATA and r.value is not None
        ]
        self.assertIn("r", ops)
        self.assertIn("u", ops)
        self.assertIn("c", ops)
        self.assertIn("d", ops)

        insert_rec = next(
            r
            for r in records
            if r.kind is EventKind.DATA
            and r.value is not None
            and r.value.get("op") == "c"
        )
        assert insert_rec.value is not None
        self.assertEqual(insert_rec.value["after"]["title"], "connect_inserted")
        self.assertEqual(insert_rec.value["after"]["extra_note"], "note1")

        tombstones = [r for r in records if r.kind is EventKind.TOMBSTONE]
        self.assertGreaterEqual(len(tombstones), 1)
        self.assertIsNone(tombstones[0].value)
        self.assertEqual(tombstones[0].key, {"id": 1})

        self.assertGreaterEqual(len(handler.table_batches), 2)
        self.assertTrue(
            any(
                "extra_note" in [f.name for f in t.schema.field("after").type]
                for t in handler.arrow_tables
            )
        )
        aligned_initial = handler.table_batches[0].to_arrow(
            target_schema=new_schema.arrow_schema
        )
        self.assertEqual(aligned_initial.schema, new_schema.arrow_schema)
        self.assertIsNone(aligned_initial.to_pylist()[0]["after"]["extra_note"])
