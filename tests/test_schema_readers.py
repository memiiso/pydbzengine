import json
import unittest

from mock_events import MockChangeEvent

from pydbzengine.schema.models import CanonicalPrimitiveType
from pydbzengine.schema.readers import DebeziumSchemaReader


class TestDebeziumSchemaReader(unittest.TestCase):
    def setUp(self):
        self.reader = DebeziumSchemaReader()

    def test_extract_schema_from_debezium_envelope(self):
        event_payload = {
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "type": "struct",
                        "fields": [
                            {"type": "int32", "optional": False, "field": "id"},
                            {"type": "string", "optional": True, "field": "name"},
                            {
                                "type": "int32",
                                "optional": True,
                                "name": "io.debezium.time.Date",
                                "version": 1,
                                "field": "birth_date",
                            },
                            {
                                "type": "int64",
                                "optional": True,
                                "name": "io.debezium.time.MicroTimestamp",
                                "version": 1,
                                "field": "created_at",
                            },
                            {
                                "type": "bytes",
                                "optional": True,
                                "name": "org.apache.kafka.connect.data.Decimal",
                                "version": 1,
                                "parameters": {
                                    "scale": "2",
                                    "connect.decimal.precision": "10",
                                },
                                "field": "salary",
                            },
                            {
                                "type": "string",
                                "optional": True,
                                "name": "io.debezium.data.Uuid",
                                "version": 1,
                                "field": "account_id",
                            },
                        ],
                        "optional": True,
                        "name": "dbserver1.inventory.customers.Value",
                        "field": "after",
                    },
                    {"type": "string", "optional": False, "field": "op"},
                    {"type": "int64", "optional": True, "field": "ts_ms"},
                ],
                "optional": False,
                "name": "dbserver1.inventory.customers.Envelope",
            },
            "payload": {
                "after": {
                    "id": 1001,
                    "name": "Sally",
                    "birth_date": 18262,
                    "created_at": 1620000000000000,
                    "salary": "s/A=",
                    "account_id": "c9a646d3-9c61-4073-8946-f94d93d3b762",
                },
                "op": "c",
                "ts_ms": 1620000000000,
            },
        }

        key_payload = {
            "schema": {
                "type": "struct",
                "fields": [{"type": "int32", "optional": False, "field": "id"}],
                "optional": False,
                "name": "dbserver1.inventory.customers.Key",
            },
            "payload": {"id": 1001},
        }

        record = MockChangeEvent(
            key=json.dumps(key_payload),
            value=json.dumps(event_payload),
            destination="inventory.customers",
        )

        self.assertTrue(self.reader.can_read(record))
        schema = self.reader.extract_schema(record, flattening_enabled=True)

        self.assertEqual(schema.identifier, "inventory.customers")
        self.assertEqual(schema.primary_keys, ("id",))

        # Check column types
        f_id = schema.find_field("id")
        self.assertIsNotNone(f_id)
        self.assertEqual(f_id.field_type.primitive, CanonicalPrimitiveType.INT32)
        self.assertFalse(f_id.optional)

        f_date = schema.find_field("birth_date")
        self.assertIsNotNone(f_date)
        self.assertEqual(f_date.field_type.primitive, CanonicalPrimitiveType.DATE)

        f_ts = schema.find_field("created_at")
        self.assertIsNotNone(f_ts)
        self.assertEqual(f_ts.field_type.primitive, CanonicalPrimitiveType.TIMESTAMP)

        f_dec = schema.find_field("salary")
        self.assertIsNotNone(f_dec)
        self.assertEqual(f_dec.field_type.primitive, CanonicalPrimitiveType.DECIMAL)
        self.assertEqual(f_dec.field_type.scale, 2)
        self.assertEqual(f_dec.field_type.precision, 10)

        f_uuid = schema.find_field("account_id")
        self.assertIsNotNone(f_uuid)
        self.assertEqual(f_uuid.field_type.primitive, CanonicalPrimitiveType.UUID)

        # Check metadata fields added
        self.assertIn("_consumed_at", schema.field_names)
        self.assertIn("_dbz_op", schema.field_names)
        self.assertIn("_dbz_ts_ms", schema.field_names)
        self.assertIn("_dbz_event_key", schema.field_names)
        self.assertIn("_dbz_event_key_hash", schema.field_names)

    def test_strict_failure_when_schema_missing(self):
        # Schemaless event must fail fast
        schemaless = MockChangeEvent(
            key='{"id": 1}',
            value='{"id": 1, "name": "Bob"}',
            destination="test.table",
        )
        self.assertFalse(self.reader.can_read(schemaless))
        with self.assertRaisesRegex(
            ValueError, "lacks a Debezium Connect 'schema' block"
        ):
            self.reader.extract_schema(schemaless)

    def test_primary_keys_extracted_from_schemaless_key_dict(self):
        event_payload = {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int32", "optional": False, "field": "order_id"},
                    {"type": "string", "optional": True, "field": "item"},
                ],
            },
            "payload": {"order_id": 42, "item": "Widget"},
        }
        record = MockChangeEvent(
            key='{"order_id": 42}',
            value=json.dumps(event_payload),
            destination="orders",
        )
        schema = self.reader.extract_schema(record)
        self.assertEqual(schema.primary_keys, ("order_id",))

    def test_extract_schema_without_metadata(self):
        event_payload = {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int32", "optional": False, "field": "id"},
                    {"type": "string", "optional": True, "field": "name"},
                ],
            },
            "payload": {"id": 1, "name": "Alice"},
        }
        record = MockChangeEvent(
            key='{"id": 1}',
            value=json.dumps(event_payload),
            destination="users",
        )
        schema = self.reader.extract_schema(record, include_metadata=False)
        self.assertEqual(schema.field_names, ["id", "name"])
        self.assertNotIn("_consumed_at", schema.field_names)
        self.assertNotIn("_dbz_op", schema.field_names)

    def test_dict_payload_support(self) -> None:
        """Verifies that events with Python dict key and value are handled without failure."""
        event_dict = {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "int32", "optional": False, "field": "id"},
                    {"type": "string", "optional": True, "field": "email"},
                ],
            },
            "payload": {"id": 10, "email": "alice@example.com"},
        }
        key_dict = {"id": 10}

        rec = MockChangeEvent(
            key=key_dict,
            value=event_dict,
            destination="dict_table",
        )
        self.assertTrue(self.reader.can_read(rec))
        schema = self.reader.extract_schema(rec, include_metadata=False)
        self.assertEqual(schema.field_names, ["id", "email"])
        self.assertEqual(schema.primary_keys, ("id",))


if __name__ == "__main__":
    unittest.main()
