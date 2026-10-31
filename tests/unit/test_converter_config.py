from __future__ import annotations

import unittest

from pydbzengine import EventFormat
from pydbzengine.engine import (
    EventFormat as EngineEventFormat,
)
from pydbzengine.schema import (
    ConnectEventConverter,
    EventConverter,
    JsonEventConverter,
    SchemaAwareChangeHandler,
    SchemalessJsonEventConverter,
)
from pydbzengine.schema import (
    EventFormat as SchemaEventFormat,
)


class DummySchemaHandler(SchemaAwareChangeHandler):
    pass


class TestEventConverterConfig(unittest.TestCase):
    def test_event_format_values_and_exports(self) -> None:
        self.assertEqual(EventFormat.JSON, "json")
        self.assertEqual(EventFormat.CONNECT, "connect")
        self.assertIs(EventFormat, EngineEventFormat)
        self.assertIs(EventFormat, SchemaEventFormat)

    def test_defaults(self) -> None:
        conv = EventConverter.from_properties()
        self.assertIsInstance(conv, JsonEventConverter)
        self.assertEqual(conv.format, EventFormat.JSON)
        self.assertTrue(conv.schemas_enabled)
        self.assertEqual(conv.heartbeat_prefix, "__debezium-heartbeat")
        self.assertEqual(conv.default_variable_scale, 10)

    def test_format_detection_from_properties(self) -> None:
        for key in ("format", "engine.format", "converter.format"):
            conv_conn = EventConverter.from_properties({key: "connect"})
            self.assertIsInstance(conv_conn, ConnectEventConverter)
            self.assertEqual(conv_conn.format, EventFormat.CONNECT)
            self.assertTrue(conv_conn.schemas_enabled)

            conv_json = EventConverter.from_properties({key: "JSON"})
            self.assertIsInstance(conv_json, JsonEventConverter)
            self.assertEqual(conv_json.format, EventFormat.JSON)

        conv_val_conv = EventConverter.from_properties(
            {"value.converter": "org.apache.kafka.connect.json.JsonConverter"}
        )
        self.assertIsInstance(conv_val_conv, JsonEventConverter)
        self.assertEqual(conv_val_conv.format, EventFormat.JSON)

        conv_conn_conv = EventConverter.from_properties(
            {"value.converter": "io.debezium.embedded.Connect"}
        )
        self.assertIsInstance(conv_conn_conv, ConnectEventConverter)
        self.assertEqual(conv_conn_conv.format, EventFormat.CONNECT)

    def test_schemas_enable_detection(self) -> None:
        for key in (
            "value.converter.schemas.enable",
            "converter.schemas.enable",
            "schemas.enable",
        ):
            conv_false = EventConverter.from_properties({key: "false"})
            self.assertIsInstance(conv_false, SchemalessJsonEventConverter)
            self.assertFalse(conv_false.schemas_enabled)

            conv_bool = EventConverter.from_properties({key: False})
            self.assertIsInstance(conv_bool, SchemalessJsonEventConverter)
            self.assertFalse(conv_bool.schemas_enabled)

            conv_true = EventConverter.from_properties({key: "true"})
            self.assertIsInstance(conv_true, JsonEventConverter)
            self.assertTrue(conv_true.schemas_enabled)

    def test_connect_format_forces_schemas_enabled(self) -> None:
        conv = EventConverter.from_properties(
            {"format": "connect", "value.converter.schemas.enable": "false"}
        )
        self.assertIsInstance(conv, ConnectEventConverter)
        self.assertEqual(conv.format, EventFormat.CONNECT)
        self.assertTrue(conv.schemas_enabled)

    def test_heartbeat_prefix_override(self) -> None:
        conv = JsonEventConverter(
            {"topic.heartbeat.prefix": "my_prefix"},
        )
        self.assertEqual(conv.heartbeat_prefix, "my_prefix")

    def test_event_converter_init_time_binding(self) -> None:
        conv_json = JsonEventConverter()
        self.assertEqual(conv_json.format, EventFormat.JSON)
        self.assertTrue(conv_json.schemas_enabled)

        conv_schemaless = SchemalessJsonEventConverter()
        self.assertEqual(conv_schemaless.format, EventFormat.JSON)
        self.assertFalse(conv_schemaless.schemas_enabled)

        conv_connect = ConnectEventConverter()
        self.assertEqual(conv_connect.format, EventFormat.CONNECT)
        self.assertTrue(conv_connect.schemas_enabled)

    def test_schema_aware_change_handler_owns_converter_instance(self) -> None:
        handler_default = DummySchemaHandler()
        self.assertIsInstance(handler_default.converter, JsonEventConverter)
        self.assertEqual(handler_default.format, EventFormat.JSON)
        self.assertTrue(handler_default.schemas_enabled)
        self.assertEqual(
            handler_default.converter.heartbeat_prefix,
            EventConverter.DEFAULT_HEARTBEAT_PREFIX,
        )

        handler_props = DummySchemaHandler(
            properties={
                "name": "test",
                "value.converter.schemas.enable": "false",
                "topic.heartbeat.prefix": "custom_hb",
            },
        )
        self.assertIsInstance(handler_props.converter, SchemalessJsonEventConverter)
        self.assertEqual(handler_props.format, EventFormat.JSON)
        self.assertFalse(handler_props.schemas_enabled)
        self.assertEqual(handler_props.converter.heartbeat_prefix, "custom_hb")

        handler_connect = DummySchemaHandler(
            properties={"name": "test", "format": EventFormat.CONNECT},
        )
        self.assertIsInstance(handler_connect.converter, ConnectEventConverter)
        self.assertEqual(handler_connect.format, EventFormat.CONNECT)
        self.assertTrue(handler_connect.schemas_enabled)

        custom_conv = ConnectEventConverter(
            {"topic.heartbeat.prefix": "explicit_hb"},
        )
        handler_explicit = DummySchemaHandler(converter=custom_conv)
        self.assertIs(handler_explicit.converter, custom_conv)
        self.assertEqual(handler_explicit.format, EventFormat.CONNECT)
        self.assertEqual(handler_explicit.converter.heartbeat_prefix, "explicit_hb")

    def test_invalid_config_raises(self) -> None:
        with self.assertRaises(ValueError):
            EventConverter.from_properties({"format": "invalid_format"})
        with self.assertRaises(ValueError):
            EventConverter.from_properties(
                {
                    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
                }
            )
        with self.assertRaises(ValueError):
            EventConverter.from_properties(
                {"value.converter": "some.unknown.JsonLikeConverter"}
            )
        with self.assertRaises(TypeError):
            EventConverter.from_properties({"format": 123})
        with self.assertRaises(ValueError):
            EventConverter.from_properties({"value.converter.schemas.enable": "maybe"})
        with self.assertRaises(ValueError):
            self.assertIsNotNone(
                JsonEventConverter({"topic.heartbeat.prefix": ""}).heartbeat_prefix
            )
        with self.assertRaises(ValueError):
            self.assertIsNotNone(
                JsonEventConverter({"topic.heartbeat.prefix": "   "}).heartbeat_prefix
            )
        with self.assertRaises(ValueError):
            self.assertIsNotNone(
                JsonEventConverter({"topic.heartbeat.prefix": None}).heartbeat_prefix
            )
        with self.assertRaises(ValueError):
            JsonEventConverter(default_variable_scale=40)
        with self.assertRaises(ValueError):
            JsonEventConverter(default_variable_scale=-1)
        with self.assertRaises(ValueError):
            JsonEventConverter(default_variable_scale=True)
        with self.assertRaises(TypeError):
            JsonEventConverter(properties="not_a_mapping")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            JsonEventConverter(registry="not_a_registry")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            DummySchemaHandler(converter="not_a_converter")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
