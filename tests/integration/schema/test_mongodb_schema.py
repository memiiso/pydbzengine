from __future__ import annotations

import json
import tempfile
import threading
import uuid
from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa
import pytest

from pydbzengine import DebeziumEngine, EventFormat
from pydbzengine.schema import (
    ChangeOp,
    ConnectEventConverter,
    EventKind,
    JsonEventConverter,
    SchemalessJsonEventConverter,
    StringTypeHandler,
    StructTypeHandler,
)
from tests.integration.schema.base import RecordingSchemaChangeHandler
from tests.support import BaseMongoDbTest


@pytest.mark.integration
class TestMongoDbSchemaIntegration(BaseMongoDbTest):
    """
    End-to-end MongoDB CDC schema integration tests against a real MongoDB
    replica-set container and SchemaAwareChangeHandler consumer across all
    three converter modes:
    1. JSON with schemas enabled (JsonEventConverter)
    2. Connect mode (ConnectEventConverter)
    3. Schemaless JSON mode (SchemalessJsonEventConverter)
    """

    def _prepare_collection(self, suffix: str) -> str:
        coll_name = f"orders_mongo_{suffix}"
        self.mongo_db.drop_collection(coll_name)
        self.mongo_db.insert_json(
            coll_name,
            json.dumps(
                {
                    "_id": 101,
                    "customer": "Alice",
                    "total": 250.75,
                    "active": True,
                    "items": ["book", "pen"],
                    "shipping": {"city": "Berlin", "zip": "10115"},
                }
            ),
        )
        self.addCleanup(self.mongo_db.drop_collection, coll_name)
        return coll_name

    def create_isolated_mongo_config(
        self,
        *,
        collection_name: str,
        custom_props: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        test_id = uuid.uuid4().hex[:8]
        offset_file = Path(tempfile.gettempdir()) / f"dbz_mongo_offset_{test_id}.dat"
        offset_file.unlink(missing_ok=True)
        self.addCleanup(offset_file.unlink, missing_ok=True)

        conf: dict[str, str] = {
            "name": f"mongo-schema-test-{test_id}",
            "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
            "mongodb.connection.string": self.mongo_db.get_connection_string(),
            "topic.prefix": f"dbz_mongo_{test_id}",
            "database.include.list": self.mongo_db.MONGO_DBNAME,
            "collection.include.list": f"{self.mongo_db.MONGO_DBNAME}.{collection_name}",
            "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
            "offset.storage.file.filename": str(offset_file),
            "converter.schemas.enable": "true",
            "value.converter.schemas.enable": "true",
            "key.converter.schemas.enable": "true",
            "snapshot.mode": "initial",
        }
        if custom_props:
            conf.update(custom_props)
        return conf

    def run_mongo_consumer(
        self,
        engine: DebeziumEngine,
        handler: RecordingSchemaChangeHandler,
        *,
        timeout_sec: float = 35.0,
    ) -> None:
        thread = threading.Thread(target=engine.run, daemon=True)
        thread.start()
        try:
            signaled = handler.event_received.wait(timeout=timeout_sec)
            if not signaled:
                raise TimeoutError(
                    f"Timed out after {timeout_sec}s waiting for {handler.expected_count} "
                    f"MongoDB records matching '{handler.target_table}'."
                )
        finally:
            engine.close()
            thread.join(timeout=10.0)

    def test_mongodb_json_schema_enabled_crud_and_arrow_materialization(self) -> None:
        coll_name = self._prepare_collection("json_schema")
        props = self.create_isolated_mongo_config(collection_name=coll_name)
        # 1 READ + 1 INSERT + 1 UPDATE + 1 DELETE + 1 TOMBSTONE = 5 records
        handler = RecordingSchemaChangeHandler(
            target_table=coll_name,
            expected_count=5,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format="json")
        self.assertIsInstance(handler.converter, JsonEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.JSON)
        self.assertTrue(handler.converter.schemas_enabled)

        def _mutate_mongo() -> None:
            handler.first_event_received.wait(timeout=15.0)
            self.mongo_db.insert_json(
                coll_name,
                json.dumps(
                    {
                        "_id": 102,
                        "customer": "Bob",
                        "total": 99.50,
                        "active": False,
                        "items": ["notebook"],
                    }
                ),
            )
            self.mongo_db.update_json(
                coll_name,
                '{"_id": 101}',
                '{"$set": {"customer": "Alice Updated", "total": 300.0}}',
            )
            self.mongo_db.delete_json(coll_name, '{"_id": 102}')

        threading.Thread(target=_mutate_mongo, daemon=True).start()
        self.run_mongo_consumer(engine, handler)

        # 1. Verify TableSchema & Handler Types
        self.assertEqual(len(handler.created_schemas), 1)
        table_schema = handler.created_schemas[0]
        envelope_schema = table_schema.arrow_schema
        self.assertEqual(envelope_schema.field("after").type, pa.string())
        self.assertTrue(
            pa.types.is_struct(envelope_schema.field("updateDescription").type)
        )
        self.assertTrue(pa.types.is_struct(envelope_schema.field("source").type))

        assert table_schema.value_handler is not None
        field_handlers = dict(
            (f.name, f.handler) for f in table_schema.value_handler.fields
        )
        self.assertIsInstance(field_handlers["after"], StringTypeHandler)
        self.assertIsInstance(field_handlers["updateDescription"], StructTypeHandler)

        # 2. Verify Operations and Tombstone
        ops = [r.op for r in handler.records if r.kind is EventKind.DATA]
        self.assertIn(ChangeOp.READ, ops)
        self.assertIn(ChangeOp.INSERT, ops)
        self.assertIn(ChangeOp.UPDATE, ops)
        self.assertIn(ChangeOp.DELETE, ops)

        tombstones = [r for r in handler.records if r.kind is EventKind.TOMBSTONE]
        self.assertGreaterEqual(len(tombstones), 1)
        self.assertIsNone(tombstones[0].row)
        self.assertIsNotNone(tombstones[0].key)

        # 3. Verify Parsed Document Content and Arrow Table Materialization
        read_rec = next(r for r in handler.records if r.op is ChangeOp.READ)
        assert read_rec.value is not None
        raw_after = read_rec.value["after"]
        self.assertIsInstance(raw_after, str)
        doc = json.loads(raw_after)
        self.assertEqual(doc["customer"], "Alice")
        self.assertEqual(doc["shipping"]["city"], "Berlin")

        arrow_tbl = handler.table_batches[0].to_arrow()
        self.assertGreaterEqual(arrow_tbl.num_rows, 1)

    def test_mongodb_connect_mode_crud_and_arrow_materialization(self) -> None:
        coll_name = self._prepare_collection("connect_mode")
        props = self.create_isolated_mongo_config(
            collection_name=coll_name,
            custom_props={"format": "connect"},
        )
        handler = RecordingSchemaChangeHandler(
            target_table=coll_name,
            expected_count=5,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format="connect")
        self.assertIsInstance(handler.converter, ConnectEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.CONNECT)
        self.assertTrue(handler.converter.schemas_enabled)

        def _mutate_mongo() -> None:
            handler.first_event_received.wait(timeout=15.0)
            self.mongo_db.insert_json(
                coll_name,
                json.dumps({"_id": 202, "customer": "Charlie", "total": 50.0}),
            )
            self.mongo_db.update_json(
                coll_name,
                '{"_id": 101}',
                '{"$set": {"customer": "Alice Connect Updated"}}',
            )
            self.mongo_db.delete_json(coll_name, '{"_id": 202}')

        threading.Thread(target=_mutate_mongo, daemon=True).start()
        self.run_mongo_consumer(engine, handler)

        self.assertEqual(len(handler.created_schemas), 1)
        ops = [r.op for r in handler.records if r.kind is EventKind.DATA]
        self.assertIn(ChangeOp.READ, ops)
        self.assertIn(ChangeOp.INSERT, ops)
        self.assertIn(ChangeOp.UPDATE, ops)
        self.assertIn(ChangeOp.DELETE, ops)

        tombstones = [r for r in handler.records if r.kind is EventKind.TOMBSTONE]
        self.assertGreaterEqual(len(tombstones), 1)

        arrow_tbl = handler.table_batches[0].to_arrow()
        self.assertGreaterEqual(arrow_tbl.num_rows, 1)

    def test_mongodb_schemaless_json_mode(self) -> None:
        coll_name = self._prepare_collection("schemaless_json")
        props = self.create_isolated_mongo_config(
            collection_name=coll_name,
            custom_props={
                "converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                "key.converter.schemas.enable": "false",
            },
        )
        handler = RecordingSchemaChangeHandler(
            target_table=coll_name,
            expected_count=2,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format="json")
        self.assertIsInstance(handler.converter, SchemalessJsonEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.JSON)
        self.assertFalse(handler.converter.schemas_enabled)

        def _mutate_mongo() -> None:
            handler.first_event_received.wait(timeout=15.0)
            self.mongo_db.insert_json(
                coll_name,
                json.dumps({"_id": 303, "customer": "Schemaless User", "total": 10.0}),
            )

        threading.Thread(target=_mutate_mongo, daemon=True).start()
        self.run_mongo_consumer(engine, handler)

        self.assertEqual(len(handler.created_schemas), 1)
        ops = [r.op for r in handler.records if r.kind is EventKind.DATA]
        self.assertIn(ChangeOp.READ, ops)
        self.assertIn(ChangeOp.INSERT, ops)

        arrow_tbl = handler.table_batches[0].to_arrow()
        self.assertGreaterEqual(arrow_tbl.num_rows, 1)

    def test_mongodb_example_inventory_customers_snapshot(self) -> None:
        """Verify snapshot of the pre-populated inventory.customers collection in quay.io/debezium/example-mongodb:3.4."""
        props = self.create_isolated_mongo_config(collection_name="customers")
        handler = RecordingSchemaChangeHandler(
            properties=props,
            target_table="customers",
            expected_count=4,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format="json")
        self.run_mongo_consumer(engine, handler)

        self.assertEqual(len(handler.created_schemas), 1)
        read_records = [r for r in handler.records if r.op is ChangeOp.READ]
        self.assertEqual(len(read_records), 4)
        emails = {
            json.loads(r.value["after"])["email"]
            for r in read_records
            if r.value is not None
        }
        self.assertEqual(
            emails,
            {
                "sally.thomas@acme.com",
                "gbailey@foobar.com",
                "ed@walker.com",
                "annek@noanswer.org",
            },
        )
        arrow_tbl = handler.table_batches[0].to_arrow()
        self.assertGreaterEqual(arrow_tbl.num_rows, 1)
