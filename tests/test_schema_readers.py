import json
import unittest
from typing import Any

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

        # Check no synthetic metadata fields are injected
        self.assertNotIn("_consumed_at", schema.field_names)
        self.assertNotIn("_dbz_op", schema.field_names)
        self.assertNotIn("_dbz_ts_ms", schema.field_names)
        self.assertNotIn("_dbz_event_key", schema.field_names)
        self.assertNotIn("_dbz_event_key_hash", schema.field_names)
        self.assertEqual(
            schema.field_names,
            ["id", "name", "birth_date", "created_at", "salary", "account_id"],
        )

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
        schema = self.reader.extract_schema(record)
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
        schema = self.reader.extract_schema(rec)
        self.assertEqual(schema.field_names, ["id", "email"])
        self.assertEqual(schema.primary_keys, ("id",))

    def test_extract_schema_all_debezium_logical_types(self) -> None:
        """Verifies mapping of all supported Debezium logical types and precisions."""
        event_payload = {
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "type": "struct",
                        "fields": [
                            {
                                "type": "string",
                                "name": "io.debezium.time.IsoTime",
                                "field": "f_isotime",
                            },
                            {
                                "type": "string",
                                "name": "io.debezium.time.ZonedTime",
                                "field": "f_zonedtime",
                            },
                            {
                                "type": "int64",
                                "name": "io.debezium.time.MicroTime",
                                "field": "f_microtime",
                            },
                            {
                                "type": "int64",
                                "name": "io.debezium.time.NanoTime",
                                "field": "f_nanotime",
                            },
                            {
                                "type": "int32",
                                "name": "io.debezium.time.Time",
                                "field": "f_time",
                            },
                            {
                                "type": "string",
                                "name": "io.debezium.time.IsoTimestamp",
                                "field": "f_isots",
                            },
                            {
                                "type": "string",
                                "name": "io.debezium.time.ZonedTimestamp",
                                "field": "f_zonedts",
                            },
                            {
                                "type": "int32",
                                "name": "io.debezium.time.Year",
                                "field": "f_year",
                            },
                            {
                                "type": "int64",
                                "name": "io.debezium.time.MicroDuration",
                                "field": "f_duration",
                            },
                            {
                                "type": "string",
                                "name": "io.debezium.time.Interval",
                                "field": "f_interval",
                            },
                            {
                                "type": "bytes",
                                "name": "io.debezium.data.Bits",
                                "field": "f_bits",
                            },
                            {
                                "type": "string",
                                "name": "io.debezium.data.Json",
                                "field": "f_json",
                            },
                            {
                                "type": "string",
                                "name": "io.debezium.data.Enum",
                                "field": "f_enum",
                            },
                            {
                                "type": "string",
                                "name": "io.debezium.data.EnumSet",
                                "field": "f_enumset",
                            },
                            {
                                "type": "struct",
                                "name": "io.debezium.data.VariableScaleDecimal",
                                "fields": [
                                    {"type": "int32", "field": "scale"},
                                    {"type": "bytes", "field": "value"},
                                ],
                                "field": "f_vardec",
                            },
                        ],
                        "field": "after",
                    }
                ],
            },
            "payload": {
                "after": {
                    "f_isotime": "04:05:11",
                    "f_zonedtime": "04:05:11+02:00",
                    "f_microtime": 14711000000,
                    "f_nanotime": 14711000000000,
                    "f_time": 14711000,
                    "f_isots": "2024-05-05T14:30:00",
                    "f_zonedts": "2024-05-05T14:30:00Z",
                    "f_year": 2024,
                    "f_duration": 86400000000,
                    "f_interval": "P1Y2M3D",
                    "f_bits": "AQ==",
                    "f_json": '{"k": 1}',
                    "f_enum": "ACTIVE",
                    "f_enumset": "A,B",
                    "f_vardec": {"scale": 2, "value": "s/A="},
                },
                "op": "c",
            },
        }
        record = MockChangeEvent(
            key='{"id": 1}',
            value=json.dumps(event_payload),
            destination="test.logical_types",
        )
        schema = self.reader.extract_schema(record)

        self.assertEqual(
            schema.find_field("f_isotime").field_type.primitive,
            CanonicalPrimitiveType.TIME,
        )
        self.assertEqual(
            schema.find_field("f_zonedtime").field_type.primitive,
            CanonicalPrimitiveType.TIME,
        )
        self.assertEqual(
            schema.find_field("f_microtime").field_type.primitive,
            CanonicalPrimitiveType.TIME,
        )
        self.assertEqual(schema.find_field("f_microtime").field_type.precision, 6)
        self.assertEqual(
            schema.find_field("f_nanotime").field_type.primitive,
            CanonicalPrimitiveType.TIME,
        )
        self.assertEqual(schema.find_field("f_nanotime").field_type.precision, 9)
        self.assertEqual(
            schema.find_field("f_time").field_type.primitive,
            CanonicalPrimitiveType.TIME,
        )
        self.assertEqual(schema.find_field("f_time").field_type.precision, 3)

        self.assertEqual(
            schema.find_field("f_isots").field_type.primitive,
            CanonicalPrimitiveType.TIMESTAMP,
        )
        self.assertEqual(
            schema.find_field("f_zonedts").field_type.primitive,
            CanonicalPrimitiveType.TIMESTAMPTZ,
        )

        self.assertEqual(
            schema.find_field("f_year").field_type.primitive,
            CanonicalPrimitiveType.INT32,
        )
        self.assertEqual(
            schema.find_field("f_duration").field_type.primitive,
            CanonicalPrimitiveType.INT64,
        )
        self.assertEqual(
            schema.find_field("f_interval").field_type.primitive,
            CanonicalPrimitiveType.STRING,
        )

        self.assertEqual(
            schema.find_field("f_bits").field_type.primitive,
            CanonicalPrimitiveType.BINARY,
        )
        self.assertEqual(
            schema.find_field("f_json").field_type.primitive,
            CanonicalPrimitiveType.STRING,
        )
        self.assertEqual(
            schema.find_field("f_enum").field_type.primitive,
            CanonicalPrimitiveType.STRING,
        )
        self.assertEqual(
            schema.find_field("f_enumset").field_type.primitive,
            CanonicalPrimitiveType.STRING,
        )
        self.assertEqual(
            schema.find_field("f_vardec").field_type.primitive,
            CanonicalPrimitiveType.DECIMAL,
        )

    def test_extract_schema_extended_primitives_and_aliases(self) -> None:
        """Verifies primitive type map extensions (float8, float16, uuid, binary, aliases)."""
        event_payload = {
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "type": "struct",
                        "fields": [
                            {"type": "float8", "field": "f_f8"},
                            {"type": "float16", "field": "f_f16"},
                            {"type": "uuid", "field": "f_uuid"},
                            {"type": "binary", "field": "f_bin"},
                            {"type": "fixed", "field": "f_fixed"},
                            {"type": "bool", "field": "f_bool"},
                            {"type": "byte", "field": "f_byte"},
                            {"type": "short", "field": "f_short"},
                            {"type": "integer", "field": "f_int"},
                            {"type": "long", "field": "f_long"},
                            {"type": "text", "field": "f_text"},
                        ],
                        "field": "after",
                    }
                ],
            },
            "payload": {"after": {}},
        }
        record = MockChangeEvent(
            key='{"id": 1}',
            value=json.dumps(event_payload),
            destination="test.primitives",
        )
        schema = self.reader.extract_schema(record)

        self.assertEqual(
            schema.find_field("f_f8").field_type.primitive, CanonicalPrimitiveType.FLOAT
        )
        self.assertEqual(
            schema.find_field("f_f16").field_type.primitive,
            CanonicalPrimitiveType.FLOAT,
        )
        self.assertEqual(
            schema.find_field("f_uuid").field_type.primitive,
            CanonicalPrimitiveType.UUID,
        )
        self.assertEqual(
            schema.find_field("f_bin").field_type.primitive,
            CanonicalPrimitiveType.BINARY,
        )
        self.assertEqual(
            schema.find_field("f_fixed").field_type.primitive,
            CanonicalPrimitiveType.BINARY,
        )
        self.assertEqual(
            schema.find_field("f_bool").field_type.primitive,
            CanonicalPrimitiveType.BOOLEAN,
        )
        self.assertEqual(
            schema.find_field("f_byte").field_type.primitive,
            CanonicalPrimitiveType.INT8,
        )
        self.assertEqual(
            schema.find_field("f_short").field_type.primitive,
            CanonicalPrimitiveType.INT16,
        )
        self.assertEqual(
            schema.find_field("f_int").field_type.primitive,
            CanonicalPrimitiveType.INT32,
        )
        self.assertEqual(
            schema.find_field("f_long").field_type.primitive,
            CanonicalPrimitiveType.INT64,
        )
        self.assertEqual(
            schema.find_field("f_text").field_type.primitive,
            CanonicalPrimitiveType.STRING,
        )

    def test_ts_ms_special_fields_mapped_to_timestamptz(self) -> None:
        """Verifies __ts_ms and __source_ts_ms are recognized as TIMESTAMPTZ."""
        event_payload = {
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "type": "struct",
                        "fields": [
                            {"type": "int64", "field": "__ts_ms"},
                            {"type": "int64", "field": "__source_ts_ms"},
                            {"type": "int64", "field": "normal_counter"},
                        ],
                        "field": "after",
                    }
                ],
            },
            "payload": {"after": {}},
        }
        record = MockChangeEvent(
            key='{"id": 1}',
            value=json.dumps(event_payload),
            destination="test.ts_ms",
        )
        schema = self.reader.extract_schema(record)

        f_ts = schema.find_field("__ts_ms")
        self.assertEqual(f_ts.field_type.primitive, CanonicalPrimitiveType.TIMESTAMPTZ)
        self.assertEqual(f_ts.field_type.precision, 3)

        f_src_ts = schema.find_field("__source_ts_ms")
        self.assertEqual(
            f_src_ts.field_type.primitive, CanonicalPrimitiveType.TIMESTAMPTZ
        )

        f_normal = schema.find_field("normal_counter")
        self.assertEqual(f_normal.field_type.primitive, CanonicalPrimitiveType.INT64)

    def test_primary_keys_extracted_from_java_key_schema(self) -> None:
        """Verifies PK extraction when record has a keySchema() object with fields."""

        class FieldObj:
            def __init__(self, name: str) -> None:
                self._name = name

            def name(self) -> str:
                return self._name

        class SchemaObj:
            def fields(self) -> list[FieldObj]:
                return [FieldObj("tenant_id"), FieldObj("user_id")]

        class RecordWithKeySchema(MockChangeEvent):
            def keySchema(self) -> SchemaObj:
                return SchemaObj()

        event_payload = {
            "schema": {
                "type": "struct",
                "fields": [{"type": "string", "field": "val"}],
            },
            "payload": {"val": "test"},
        }
        record = RecordWithKeySchema(
            key=None,
            value=json.dumps(event_payload),
            destination="test.keyschema",
        )
        schema = self.reader.extract_schema(record)
        self.assertEqual(schema.primary_keys, ("tenant_id", "user_id"))

    def test_extract_schema_from_java_value_schema(self) -> None:
        """Verifies schema extraction directly from a Kafka Connect valueSchema() object."""

        class MockConnectType:
            def __init__(self, name: str) -> None:
                self._name = name

            def name(self) -> str:
                return self._name

        class MockFieldSchema:
            def __init__(
                self,
                type_name: str,
                logical_name: str | None = None,
                params: dict | None = None,
            ) -> None:
                self._type = MockConnectType(type_name)
                self._logical_name = logical_name
                self._params = params or {}

            def type(self) -> MockConnectType:
                return self._type

            def name(self) -> str | None:
                return self._logical_name

            def isOptional(self) -> bool:
                return True

            def doc(self) -> str | None:
                return "sample doc"

            def parameters(self) -> dict:
                return self._params

        class MockField:
            def __init__(self, name: str, schema: MockFieldSchema) -> None:
                self._name = name
                self._schema = schema

            def name(self) -> str:
                return self._name

            def schema(self) -> MockFieldSchema:
                return self._schema

        class MockValueSchema:
            def fields(self) -> list[MockField]:
                return [
                    MockField("id", MockFieldSchema("INT64")),
                    MockField(
                        "amount",
                        MockFieldSchema(
                            "BYTES",
                            "org.apache.kafka.connect.data.Decimal",
                            {"scale": "2"},
                        ),
                    ),
                    MockField(
                        "created_at",
                        MockFieldSchema("INT64", "io.debezium.time.MicroTimestamp"),
                    ),
                ]

        class ConnectRecord(MockChangeEvent):
            def valueSchema(self) -> MockValueSchema:
                return MockValueSchema()

        rec = ConnectRecord(key=None, value=None, destination="connect.orders")
        self.assertTrue(self.reader.can_read(rec))
        schema = self.reader.extract_schema(rec)
        self.assertEqual(schema.identifier, "connect.orders")
        self.assertEqual(schema.field_names, ["id", "amount", "created_at"])
        f_id = schema.find_field("id")
        self.assertIsNotNone(f_id)
        self.assertEqual(f_id.field_type.primitive, CanonicalPrimitiveType.INT64)  # type: ignore[union-attr]
        f_amount = schema.find_field("amount")
        self.assertIsNotNone(f_amount)
        self.assertEqual(f_amount.field_type.primitive, CanonicalPrimitiveType.DECIMAL)  # type: ignore[union-attr]
        self.assertEqual(f_amount.field_type.scale, 2)  # type: ignore[union-attr]
        f_ts = schema.find_field("created_at")
        self.assertIsNotNone(f_ts)
        self.assertEqual(f_ts.field_type.primitive, CanonicalPrimitiveType.TIMESTAMP)  # type: ignore[union-attr]
        self.assertEqual(f_ts.field_type.precision, 6)  # type: ignore[union-attr]

    def test_extract_schema_from_java_complex_schemas(self) -> None:
        class MockType:
            def __init__(self, name: str) -> None:
                self._name = name

            def name(self) -> str:
                return self._name

        class MockFieldSchema:
            def __init__(
                self,
                type_name: str,
                logical_name: str | None = None,
                params: dict | None = None,
                subfields: list | None = None,
                value_schema: Any = None,
                key_schema: Any = None,
            ) -> None:
                self._type = MockType(type_name)
                self._logical_name = logical_name
                self._params = params or {}
                self._subfields = subfields
                self._val_schema = value_schema
                self._key_schema = key_schema

            def type(self) -> MockType:
                return self._type

            def name(self) -> str | None:
                return self._logical_name

            def isOptional(self) -> bool:
                return True

            def doc(self) -> str | None:
                return None

            def parameters(self) -> dict:
                return self._params

            def fields(self) -> list | None:
                return self._subfields

            def valueSchema(self) -> Any:
                return self._val_schema

            def keySchema(self) -> Any:
                return self._key_schema

        class MockField:
            def __init__(self, name: str, schema: MockFieldSchema) -> None:
                self._name = name
                self._schema = schema

            def name(self) -> str:
                return self._name

            def schema(self) -> MockFieldSchema:
                return self._schema

        nested_struct_schema = MockFieldSchema(
            "STRUCT",
            subfields=[
                MockField("street", MockFieldSchema("STRING")),
                MockField("zip", MockFieldSchema("INT32")),
            ],
        )
        array_schema = MockFieldSchema(
            "ARRAY",
            value_schema=MockFieldSchema("STRING"),
        )
        map_schema = MockFieldSchema(
            "MAP",
            key_schema=MockFieldSchema("STRING"),
            value_schema=MockFieldSchema("INT32"),
        )

        class MockComplexValueSchema:
            def fields(self) -> list[MockField]:
                return [
                    MockField("address", nested_struct_schema),
                    MockField("tags", array_schema),
                    MockField("metrics", map_schema),
                ]

        class ComplexConnectRecord(MockChangeEvent):
            def valueSchema(self) -> MockComplexValueSchema:
                return MockComplexValueSchema()

        rec = ComplexConnectRecord(key=None, value=None, destination="connect.complex")
        schema = self.reader.extract_schema(rec)
        self.assertEqual(schema.field_names, ["address", "tags", "metrics"])

        f_addr = schema.find_field("address")
        self.assertIsNotNone(f_addr)
        self.assertEqual(f_addr.field_type.primitive, CanonicalPrimitiveType.STRUCT)  # type: ignore[union-attr]
        self.assertIsNotNone(f_addr.field_type.fields)  # type: ignore[union-attr]
        self.assertEqual([f.name for f in f_addr.field_type.fields], ["street", "zip"])  # type: ignore[union-attr]

        f_tags = schema.find_field("tags")
        self.assertIsNotNone(f_tags)
        self.assertEqual(f_tags.field_type.primitive, CanonicalPrimitiveType.LIST)  # type: ignore[union-attr]
        self.assertIsNotNone(f_tags.field_type.element_type)  # type: ignore[union-attr]
        self.assertEqual(
            f_tags.field_type.element_type.primitive, CanonicalPrimitiveType.STRING
        )  # type: ignore[union-attr]

        f_metrics = schema.find_field("metrics")
        self.assertIsNotNone(f_metrics)
        self.assertEqual(f_metrics.field_type.primitive, CanonicalPrimitiveType.MAP)  # type: ignore[union-attr]
        self.assertIsNotNone(f_metrics.field_type.key_type)  # type: ignore[union-attr]
        self.assertEqual(
            f_metrics.field_type.key_type.primitive, CanonicalPrimitiveType.STRING
        )  # type: ignore[union-attr]
        self.assertEqual(
            f_metrics.field_type.value_type.primitive, CanonicalPrimitiveType.INT32
        )  # type: ignore[union-attr]


if __name__ == "__main__":
    unittest.main()
