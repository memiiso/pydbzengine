from __future__ import annotations

import base64
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from pydbzengine import DebeziumConfig
from pydbzengine.schema import (
    ChangeBatch,
    ChangeRecord,
    EventKind,
    JsonEventConverter,
    SchemaAwareChangeHandler,
    SchemaDiff,
    TableBatch,
    TableSchema,
)
from tests.support import MockChangeEvent
from tests.unit.test_event_converters import make_flat_event, wrap_json_key


class TestSchemaAwareChangeHandler(unittest.TestCase):
    def test_handler_lifecycle_evolution_and_parquet_writing(self) -> None:
        class LakehouseParquetHandler(SchemaAwareChangeHandler):
            def __init__(self, base_dir: Path) -> None:
                super().__init__()
                self.base_dir = base_dir
                self.target_schemas: dict[str, pa.Schema] = {}
                self.written_files: list[Path] = []
                self.created_tables: list[str] = []
                self.evolutions: list[tuple[str, SchemaDiff]] = []
                self.tombstones: list[ChangeRecord] = []

            def on_table_created(self, schema: TableSchema) -> None:
                self.created_tables.append(schema.destination)
                self.target_schemas[schema.destination] = schema.arrow_schema

            def on_schema_evolved(
                self,
                new_schema: TableSchema,
                old_schema: TableSchema,
                diff: SchemaDiff,
            ) -> None:
                self.evolutions.append((new_schema.destination, diff))
                self.target_schemas[new_schema.destination] = pa.unify_schemas(
                    [
                        self.target_schemas[new_schema.destination],
                        new_schema.arrow_schema,
                    ],
                    promote_options="permissive",
                )

            def handle_change_batch(self, batch: ChangeBatch) -> None:
                super().handle_change_batch(batch)
                if tombstones := batch.of_kind(EventKind.TOMBSTONE):
                    self.tombstones.extend(tombstones)

            def handle_table_batch(self, batch: TableBatch) -> None:
                dest = batch.destination
                aligned = batch.to_arrow(target_schema=self.target_schemas[dest])
                out_file = (
                    self.base_dir
                    / f"{batch.schema.table_name}_{len(self.written_files)}.parquet"
                )
                pq.write_table(aligned, out_file)
                self.written_files.append(out_file)

        with tempfile.TemporaryDirectory() as tmpdir:
            handler = LakehouseParquetHandler(Path(tmpdir))
            handler.configure(DebeziumConfig({}))

            price_b64 = base64.b64encode(
                (4999).to_bytes(4, byteorder="big", signed=True)
            ).decode("ascii")
            v1_fields = [
                {"type": "int32", "optional": False, "field": "id"},
                {"type": "string", "optional": True, "field": "legacy_col"},
                {
                    "type": "bytes",
                    "optional": True,
                    "name": "org.apache.kafka.connect.data.Decimal",
                    "parameters": {
                        "scale": "2",
                        "connect.decimal.precision": "10",
                    },
                    "field": "price",
                },
            ]
            v2_fields = [
                {"type": "int64", "optional": False, "field": "id"},
                {
                    "type": "bytes",
                    "optional": True,
                    "name": "org.apache.kafka.connect.data.Decimal",
                    "parameters": {
                        "scale": "2",
                        "connect.decimal.precision": "10",
                    },
                    "field": "price",
                },
                {"type": "string", "optional": True, "field": "email"},
            ]

            rec1 = make_flat_event(
                "inventory.orders",
                v1_fields,
                {"id": 1, "legacy_col": "old", "price": price_b64},
                key={"id": 1},
            )
            rec2 = make_flat_event(
                "inventory.orders",
                v2_fields,
                {"id": 2, "price": price_b64, "email": "alice@example.com"},
                key={"id": 2},
            )
            ts_rec = MockChangeEvent(
                key=wrap_json_key({"id": 99}),
                value=None,
                destination="inventory.orders",
            )

            handler.handleJsonBatch([rec1, rec2, ts_rec])

            self.assertEqual(handler.created_tables, ["inventory.orders"])
            self.assertEqual(len(handler.evolutions), 1)
            dest, diff = handler.evolutions[0]
            self.assertEqual(dest, "inventory.orders")
            self.assertTrue(diff.has_changes)
            self.assertEqual([f.name for f in diff.added_fields], ["email"])
            self.assertEqual([f.name for f in diff.removed_fields], ["legacy_col"])
            self.assertEqual(len(diff.changed_fields), 1)
            self.assertIsNotNone(handler.get_schema("inventory.orders"))
            self.assertIn("inventory.orders", handler.schemas)
            self.assertEqual(len(handler.tombstones), 1)
            self.assertEqual(handler.tombstones[0].key, {"id": 99})
            self.assertEqual(handler.tombstones[0].kind, EventKind.TOMBSTONE)

            self.assertEqual(len(handler.written_files), 2)
            b2_rows = pq.read_table(handler.written_files[1]).to_pylist()
            self.assertEqual(
                b2_rows,
                [
                    {
                        "id": 2,
                        "legacy_col": None,
                        "price": Decimal("49.99"),
                        "email": "alice@example.com",
                    }
                ],
            )

    def test_handler_dispatches_on_schema_evolved_when_key_schema_changes(self) -> None:
        evolutions: list[SchemaDiff] = []

        class KeyTrackingHandler(SchemaAwareChangeHandler):
            def on_schema_evolved(
                self,
                new_schema: TableSchema,
                old_schema: TableSchema,
                diff: SchemaDiff,
            ) -> None:
                evolutions.append(diff)

        handler = KeyTrackingHandler(converter=JsonEventConverter())
        val_fields = [{"type": "string", "optional": True, "field": "name"}]
        ev1 = make_flat_event(
            "inventory.users",
            val_fields,
            {"name": "Alice"},
            key={
                "schema": {
                    "type": "struct",
                    "fields": [{"type": "int32", "field": "id", "optional": False}],
                },
                "payload": {"id": 1},
            },
        )
        ev2 = make_flat_event(
            "inventory.users",
            val_fields,
            {"name": "Bob"},
            key={
                "schema": {
                    "type": "struct",
                    "fields": [{"type": "int64", "field": "id", "optional": False}],
                },
                "payload": {"id": 2},
            },
        )
        handler.handle_batch([ev1, ev2])
        self.assertEqual(len(evolutions), 1)
        self.assertTrue(evolutions[0].key_schema_changed)
        self.assertFalse(evolutions[0].key_fields_changed)

    def test_custom_handle_change_batch_supports_causal_kv_replay_and_kind_groups(
        self,
    ) -> None:
        class StatefulOrderHandler(SchemaAwareChangeHandler):
            def __init__(self) -> None:
                super().__init__()
                self.kv_state: dict[int, str] = {}
                self.events_log: list[str] = []

            def on_table_created(self, schema: TableSchema) -> None:
                self.events_log.append(f"created:{schema.destination}")

            def handle_change_batch(self, batch: ChangeBatch) -> None:
                mutations = batch.of_kind(EventKind.DATA, EventKind.TOMBSTONE)
                for record in mutations:
                    if record.kind is EventKind.DATA:
                        assert record.schema is not None
                        assert record.value is not None
                        self.reconcile_schema(record.schema)
                        self.events_log.append(f"data:{record.value['name']}")
                        self.kv_state[record.value["id"]] = record.value["name"]
                    else:
                        assert record.key is not None
                        self.events_log.append(f"tombstone:{record.key['id']}")
                        self.kv_state.pop(record.key["id"], None)

                if ddls := batch.of_kind(EventKind.SCHEMA_CHANGE):
                    self.events_log.append(f"ddls:{len(ddls)}")
                if heartbeats := batch.of_kind(EventKind.HEARTBEAT):
                    self.events_log.append(f"heartbeats:{len(heartbeats)}")

        handler = StatefulOrderHandler()
        handler.configure(DebeziumConfig({}))
        fields = [
            {"type": "int32", "optional": False, "field": "id"},
            {"type": "string", "optional": False, "field": "name"},
        ]
        ev_alice = make_flat_event(
            "inventory.users", fields, {"id": 1, "name": "Alice"}, key={"id": 1}
        )
        ev_ts = MockChangeEvent(
            key=wrap_json_key({"id": 1}),
            value=None,
            destination="inventory.users",
        )
        ev_bob = make_flat_event(
            "inventory.users", fields, {"id": 1, "name": "Bob"}, key={"id": 1}
        )
        ev_ddl = MockChangeEvent(
            key=wrap_json_key({"databaseName": "inventory"}),
            value=json.dumps(
                {
                    "schema": {
                        "type": "struct",
                        "fields": [
                            {"type": "string", "field": "ddl"},
                            {"type": "string", "field": "databaseName"},
                            {"type": "string", "field": "tableChanges"},
                        ],
                    },
                    "payload": {
                        "ddl": "ALTER TABLE users ADD COLUMN c INT",
                        "databaseName": "inventory",
                        "tableChanges": "[]",
                    },
                }
            ),
            destination="inventory",
        )
        ev_hb = MockChangeEvent(
            key=wrap_json_key({"id": 1}),
            value=json.dumps(
                {
                    "schema": {
                        "type": "struct",
                        "fields": [{"type": "int64", "field": "ts_ms"}],
                    },
                    "payload": {"ts_ms": 1700000000000},
                }
            ),
            destination="__debezium-heartbeat.pg",
        )

        handler.handleJsonBatch([ev_alice, ev_ts, ev_bob, ev_ddl, ev_hb])
        self.assertEqual(handler.kv_state, {1: "Bob"})
        self.assertEqual(
            handler.events_log,
            [
                "created:inventory.users",
                "data:Alice",
                "tombstone:1",
                "data:Bob",
                "ddls:1",
                "heartbeats:1",
            ],
        )

    def test_handler_bulk_deletes_with_tombstones_produce_single_table_batch(
        self,
    ) -> None:
        class BulkDeleteCollector(SchemaAwareChangeHandler):
            def __init__(self) -> None:
                super().__init__(converter=JsonEventConverter())
                self.table_batches: list[TableBatch] = []
                self.tombstone_batches: list[dict[str, ChangeBatch]] = []

            def handle_change_batch(self, batch: ChangeBatch) -> None:
                super().handle_change_batch(batch)
                if tombstones := batch.of_kind(EventKind.TOMBSTONE):
                    self.tombstone_batches.append(tombstones.group_by_destination())

            def handle_table_batch(self, batch: TableBatch) -> None:
                self.table_batches.append(batch)

        handler = BulkDeleteCollector()
        fields = [
            {"type": "int32", "optional": False, "field": "id"},
            {"type": "string", "optional": False, "field": "__op"},
        ]
        events: list[MockChangeEvent] = []
        for row_id in range(1, 11):
            events.append(
                make_flat_event(
                    "inventory.orders",
                    fields,
                    {"id": row_id, "__op": "d"},
                    key={"id": row_id},
                )
            )
            events.append(
                MockChangeEvent(
                    key=wrap_json_key({"id": row_id}),
                    value=None,
                    destination="inventory.orders",
                )
            )

        handler.handle_batch(events)
        self.assertEqual(len(handler.table_batches), 1)
        self.assertEqual(len(handler.table_batches[0]), 10)
        self.assertEqual(len(handler.tombstone_batches), 1)
        self.assertEqual(len(handler.tombstone_batches[0]["inventory.orders"]), 10)

    def test_handler_hook_failure_preserves_atomic_schema_state_for_retry(self) -> None:
        class FlakyCatalogHandler(SchemaAwareChangeHandler):
            def __init__(self) -> None:
                super().__init__(converter=JsonEventConverter())
                self.fail_create_once = True
                self.fail_evolve_once = True
                self.created: list[str] = []
                self.evolved: list[SchemaDiff] = []
                self.batches: list[list[dict[str, Any]]] = []

            def on_table_created(self, schema: TableSchema) -> None:
                if self.fail_create_once:
                    self.fail_create_once = False
                    raise RuntimeError("Transient catalog unavailability on create")
                self.created.append(schema.destination)

            def on_schema_evolved(
                self,
                new_schema: TableSchema,
                old_schema: TableSchema,
                diff: SchemaDiff,
            ) -> None:
                if self.fail_evolve_once:
                    self.fail_evolve_once = False
                    raise RuntimeError("Transient catalog unavailability on evolve")
                self.evolved.append(diff)

            def handle_table_batch(self, batch: TableBatch) -> None:
                self.batches.append(batch.rows)

        handler = FlakyCatalogHandler()
        v1_fields = [{"type": "int32", "optional": False, "field": "id"}]
        v2_fields = [
            {"type": "int64", "optional": False, "field": "id"},
            {"type": "string", "optional": True, "field": "email"},
        ]
        ev_v1 = make_flat_event("inventory.orders", v1_fields, {"id": 1}, key={"id": 1})
        ev_v2 = make_flat_event(
            "inventory.orders",
            v2_fields,
            {"id": 2, "email": "a@example.com"},
            key={"id": 2},
        )

        with self.assertRaisesRegex(RuntimeError, "on create"):
            handler.handle_batch([ev_v1])
        self.assertIsNone(handler.get_schema("inventory.orders"))
        self.assertEqual(handler.batches, [])

        handler.handle_batch([ev_v1])
        v1_schema = handler.get_schema("inventory.orders")
        assert v1_schema is not None
        self.assertEqual(handler.created, ["inventory.orders"])
        self.assertEqual(handler.batches, [[{"id": 1}]])

        with self.assertRaisesRegex(RuntimeError, "on evolve"):
            handler.handle_batch([ev_v2])
        self.assertEqual(handler.get_schema("inventory.orders"), v1_schema)
        self.assertEqual(handler.evolved, [])

        handler.handle_batch([ev_v2])
        self.assertEqual(len(handler.evolved), 1)
        self.assertTrue(handler.evolved[0].is_compatible)
        self.assertEqual(
            handler.batches, [[{"id": 1}], [{"id": 2, "email": "a@example.com"}]]
        )


if __name__ == "__main__":
    unittest.main()
