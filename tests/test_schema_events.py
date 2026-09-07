from __future__ import annotations

import base64
import json
import unittest
from dataclasses import FrozenInstanceError
from decimal import Decimal
from typing import Any

from mock_events import MockChangeEvent

from pydbzengine.schema.base import BaseSchemaReader
from pydbzengine.schema.events import CdcEvent
from pydbzengine.schema.models import (
    CanonicalField,
    CanonicalPrimitiveType,
    CanonicalSchema,
    CanonicalType,
)


class TestCdcEvent(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = CanonicalSchema(
            identifier="inventory.customers",
            fields=(
                CanonicalField(
                    name="id",
                    field_type=CanonicalType(primitive=CanonicalPrimitiveType.INT64),
                ),
                CanonicalField(
                    name="name",
                    field_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
                ),
            ),
            primary_keys=("id",),
        )

    def test_from_record_connect_create(self) -> None:
        price_bytes = (2499).to_bytes(4, byteorder="big", signed=True)
        b64_price = base64.b64encode(price_bytes).decode("ascii")

        val_json = {
            "schema": {
                "type": "struct",
                "name": "inventory.products.Envelope",
                "fields": [
                    {"type": "string", "field": "op", "optional": False},
                    {
                        "type": "struct",
                        "field": "after",
                        "fields": [
                            {"type": "int64", "field": "id", "optional": False},
                            {"type": "string", "field": "name", "optional": True},
                            {
                                "type": "bytes",
                                "name": "org.apache.kafka.connect.data.Decimal",
                                "field": "price",
                                "parameters": {
                                    "scale": "2",
                                    "connect.decimal.precision": "10",
                                },
                            },
                        ],
                    },
                ],
            },
            "payload": {
                "op": "c",
                "ts_ms": 1788782400000,
                "after": {
                    "id": 10,
                    "name": "Widget",
                    "price": b64_price,
                },
                "source": {
                    "connector": "postgresql",
                    "db": "inventory_db",
                    "schema": "inventory",
                    "table": "products",
                },
            },
        }

        rec = MockChangeEvent(
            key=json.dumps({"id": 10}),
            value=json.dumps(val_json),
            destination="inventory_db.inventory.products",
            partition=1,
        )

        event = CdcEvent.from_record(rec)
        self.assertEqual(event.destination, "inventory_db.inventory.products")
        self.assertEqual(event.partition, 1)
        self.assertEqual(event.op, "c")
        self.assertTrue(event.is_create)
        self.assertFalse(event.is_update)
        self.assertFalse(event.is_delete)
        self.assertFalse(event.is_tombstone)

        # Ergonomic property navigation
        self.assertEqual(event.table_name, "products")
        self.assertEqual(event.schema_name, "inventory")
        self.assertEqual(event.namespace, "inventory.products")
        self.assertEqual(event.key, {"id": 10})

        # Decoded row values
        self.assertEqual(event.row["id"], 10)
        self.assertEqual(event.row["name"], "Widget")
        self.assertEqual(event.row["price"], Decimal("24.99"))

        # Schema and fingerprint access
        self.assertIsNotNone(event.schema)
        self.assertIn("price", event.schema.field_names)  # type: ignore[union-attr]
        self.assertEqual(event.fingerprint, event.schema.fingerprint)  # type: ignore[union-attr]

        # Audit dict export
        d = event.to_dict(include_audit=True)
        self.assertEqual(d["name"], "Widget")
        self.assertEqual(d["_dbz_op"], "c")
        self.assertEqual(d["_dbz_ts_ms"], 1788782400000)

    def test_from_record_delete_returns_before_row(self) -> None:
        val_json = {
            "payload": {
                "op": "d",
                "before": {"id": 10, "name": "OldWidget"},
                "after": None,
            }
        }
        rec = MockChangeEvent(
            key=json.dumps({"id": 10}),
            value=json.dumps(val_json),
            destination="inventory.products",
        )
        event = CdcEvent.from_record(rec)
        self.assertTrue(event.is_delete)
        self.assertFalse(event.is_create)
        self.assertEqual(event.row, {"id": 10, "name": "OldWidget"})

    def test_from_record_tombstone(self) -> None:
        rec = MockChangeEvent(
            key=json.dumps({"id": 99}),
            value=None,
            destination="inventory.products",
        )
        event = CdcEvent.from_record(rec)
        self.assertTrue(event.is_tombstone)
        self.assertFalse(event.is_create)
        self.assertFalse(event.is_delete)
        self.assertEqual(event.row, {})
        self.assertEqual(event.key, {"id": 99})

    def test_from_record_heartbeat(self) -> None:
        rec = MockChangeEvent(
            key="",
            value=json.dumps({"payload": {"op": "h"}}),
            destination="__debezium-heartbeat.mydb",
        )
        event = CdcEvent.from_record(rec)
        self.assertTrue(event.is_heartbeat)

    def test_from_json_schemaless(self) -> None:
        val = {"op": "c", "after": {"username": "alice", "age": 30}}
        event = CdcEvent.from_json(val, key={"id": 1}, destination="app.users")
        self.assertTrue(event.is_create)
        self.assertEqual(event.row["username"], "alice")
        self.assertEqual(event.table_name, "users")
        self.assertIsNotNone(event.schema)
        self.assertIn("username", event.schema.field_names)  # type: ignore[union-attr]

    def test_from_record_flattened_event(self) -> None:
        flat_json = {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int64", "field": "id"},
                    {"type": "string", "field": "name"},
                    {"type": "string", "field": "__op", "optional": True},
                    {"type": "string", "field": "__deleted", "optional": True},
                ],
            },
            "payload": {
                "id": 42,
                "name": "FlatWidget",
                "__op": "c",
                "__deleted": "false",
            },
        }
        rec = MockChangeEvent(
            key=json.dumps({"id": 42}),
            value=json.dumps(flat_json),
            destination="inventory.products",
        )
        event = CdcEvent.from_record(rec)
        self.assertTrue(event.is_create)
        self.assertFalse(event.is_delete)
        self.assertEqual(
            event.row,
            {
                "id": 42,
                "name": "FlatWidget",
                "__op": "c",
                "__deleted": "false",
            },
        )
        self.assertEqual(event.value, event.row)
        self.assertEqual(event.row["__op"], "c")
        self.assertEqual(event.row["__deleted"], "false")

    def test_from_record_flattened_delete_event(self) -> None:
        flat_delete = {
            "payload": {
                "id": 42,
                "name": "DeletedWidget",
                "__deleted": "true",
            }
        }
        rec = MockChangeEvent(
            key=json.dumps({"id": 42}),
            value=json.dumps(flat_delete),
            destination="inventory.products",
        )
        event = CdcEvent.from_record(rec)
        self.assertTrue(event.is_delete)
        self.assertFalse(event.is_create)
        self.assertEqual(
            event.row,
            {
                "id": 42,
                "name": "DeletedWidget",
                "__deleted": "true",
            },
        )
        self.assertEqual(event.value, event.row)

    def test_immutability(self) -> None:
        event = CdcEvent(destination="test", op="c", after={"id": 1})
        with self.assertRaises(FrozenInstanceError):
            event.destination = "new_dest"  # type: ignore[misc]

    def test_infer_primitive_types_extended(self) -> None:
        import datetime
        import uuid

        raw_payload = {
            "val_decimal": Decimal("99.95"),
            "val_dt_aware": datetime.datetime(
                2026, 9, 7, 12, 0, tzinfo=datetime.timezone.utc
            ),
            "val_dt_naive": datetime.datetime(2026, 9, 7, 12, 0),
            "val_date": datetime.date(2026, 9, 7),
            "val_time": datetime.time(12, 0, 0),
            "val_uuid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
            "val_bin": memoryview(b"abc"),
        }
        rec = MockChangeEvent(
            key=None,
            value=raw_payload,
            destination="inferred.table",
        )
        event = CdcEvent.from_record(rec)
        schema = event.schema
        self.assertIsNotNone(schema)
        self.assertEqual(
            schema.find_field("val_decimal").field_type.primitive,
            CanonicalPrimitiveType.DECIMAL,
        )
        self.assertEqual(
            schema.find_field("val_dt_aware").field_type.primitive,
            CanonicalPrimitiveType.TIMESTAMPTZ,
        )
        self.assertEqual(
            schema.find_field("val_dt_naive").field_type.primitive,
            CanonicalPrimitiveType.TIMESTAMP,
        )
        self.assertEqual(
            schema.find_field("val_date").field_type.primitive,
            CanonicalPrimitiveType.DATE,
        )
        self.assertEqual(
            schema.find_field("val_time").field_type.primitive,
            CanonicalPrimitiveType.TIME,
        )
        self.assertEqual(
            schema.find_field("val_uuid").field_type.primitive,
            CanonicalPrimitiveType.UUID,
        )
        self.assertEqual(
            schema.find_field("val_bin").field_type.primitive,
            CanonicalPrimitiveType.BINARY,
        )

    def test_shape_agnostic_arbitrary_metadata(self) -> None:
        """Verifies that flat events with custom metadata or extra keys are preserved completely."""
        payload = {
            "id": 100,
            "device_name": "SensorA",
            "__op": "u",
            "__deleted": "false",
            "__custom_meta": 42,
            "__source": {"version": "2.5.0"},
        }
        event = CdcEvent.from_json(payload, destination="telemetry.sensors")
        self.assertTrue(event.is_update)
        self.assertFalse(event.is_delete)
        self.assertEqual(event.row["id"], 100)
        self.assertEqual(event.row["__custom_meta"], 42)
        self.assertEqual(event.row["__op"], "u")
        self.assertEqual(event.value, event.row)

    def test_envelope_vs_flat_consistency(self) -> None:
        """Verifies consistent row and value access between envelope and flat representations."""
        # Envelope event
        env_payload = {
            "op": "c",
            "after": {"id": 1, "name": "Widget"},
            "before": None,
            "ts_ms": 1000,
        }
        env_event = CdcEvent.from_json(env_payload, destination="inv.items")
        self.assertEqual(env_event.row, {"id": 1, "name": "Widget"})
        self.assertEqual(env_event.value, env_payload)

        # Flat event
        flat_payload = {"id": 1, "name": "Widget", "__op": "c"}
        flat_event = CdcEvent.from_json(flat_payload, destination="inv.items")
        self.assertEqual(flat_event.row["id"], 1)
        self.assertEqual(flat_event.row["name"], "Widget")
        self.assertEqual(flat_event.row["__op"], "c")
        self.assertEqual(flat_event.value, flat_event.row)

    def test_is_schema_change_detection(self) -> None:
        """Verifies that DDL and schema change events are detected without losing the payload."""
        ddl_payload = {
            "payload": {
                "source": {"server": "dbserver1"},
                "databaseName": "inventory",
                "ddl": "ALTER TABLE customers ADD COLUMN email VARCHAR(255);",
                "tableChanges": [{"type": "ALTER"}],
            }
        }
        event = CdcEvent.from_json(ddl_payload, destination="schema-changes.inventory")
        self.assertTrue(event.is_schema_change)
        self.assertFalse(event.is_create)
        self.assertFalse(event.is_delete)
        self.assertFalse(event.is_heartbeat)
        self.assertEqual(
            event.row["ddl"], "ALTER TABLE customers ADD COLUMN email VARCHAR(255);"
        )
        self.assertEqual(event.value, event.row)

    def test_custom_schema_reader_injection(self) -> None:
        """Verifies custom BaseSchemaReader can be injected into from_record and from_json."""
        custom_schema = CanonicalSchema(
            identifier="custom.table",
            fields=(
                CanonicalField(
                    name="custom_col",
                    field_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
                ),
            ),
            primary_keys=("custom_col",),
        )

        class CustomReader(BaseSchemaReader):
            def can_read(self, record: Any) -> bool:
                return True

            def extract_schema(
                self, record: Any, flattening_enabled: bool = True
            ) -> CanonicalSchema:
                return custom_schema

        reader = CustomReader()
        rec = MockChangeEvent(
            key=json.dumps({"custom_col": "abc"}),
            value=json.dumps({"custom_col": "abc"}),
            destination="custom.table",
        )

        # Injected into from_record
        event1 = CdcEvent.from_record(rec, schema_reader=reader)
        self.assertEqual(event1.schema, custom_schema)
        self.assertEqual(event1.row["custom_col"], "abc")

        # Injected into from_json
        event2 = CdcEvent.from_json(
            {"custom_col": "abc"},
            key={"custom_col": "abc"},
            destination="custom.table",
            schema_reader=reader,
        )
        self.assertEqual(event2.schema, custom_schema)


if __name__ == "__main__":
    unittest.main()
