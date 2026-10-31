from __future__ import annotations

import unittest

from pydbzengine import BasePythonChangeHandler, EventFormat
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
        handler = DummySchemaHandler()
        self.assertIsInstance(handler.converter, JsonEventConverter)
        self.assertEqual(handler.format, EventFormat.JSON)
        self.assertTrue(handler.schemas_enabled)
        self.assertEqual(handler.heartbeat_prefix, "__debezium-heartbeat")
        self.assertEqual(handler.default_variable_scale, 10)
        self.assertEqual(
            BasePythonChangeHandler.DEFAULT_HEARTBEAT_PREFIX,
            "__debezium-heartbeat",
        )
        self.assertEqual(
            handler.converter.heartbeat_prefix,
            BasePythonChangeHandler.DEFAULT_HEARTBEAT_PREFIX,
        )
        self.assertEqual(handler.converter.default_variable_scale, 10)

    def test_format_detection_from_properties(self) -> None:
        for key in ("format", "engine.format", "converter.format", "converter.class"):
            h_conn = DummySchemaHandler({key: "connect"})
            self.assertIsInstance(h_conn.converter, ConnectEventConverter)
            self.assertEqual(h_conn.format, EventFormat.CONNECT)
            self.assertTrue(h_conn.schemas_enabled)

            h_json = DummySchemaHandler({key: "JSON"})
            self.assertIsInstance(h_json.converter, JsonEventConverter)
            self.assertEqual(h_json.format, EventFormat.JSON)

        h_val_conv = DummySchemaHandler(
            {"value.converter": "org.apache.kafka.connect.json.JsonConverter"}
        )
        self.assertIsInstance(h_val_conv.converter, JsonEventConverter)
        self.assertEqual(h_val_conv.format, EventFormat.JSON)

        h_conn_conv = DummySchemaHandler(
            {"value.converter": "io.debezium.embedded.Connect"}
        )
        self.assertIsInstance(h_conn_conv.converter, ConnectEventConverter)
        self.assertEqual(h_conn_conv.format, EventFormat.CONNECT)

        h_both_json = DummySchemaHandler(
            {
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            }
        )
        self.assertIsInstance(h_both_json.converter, JsonEventConverter)
        self.assertEqual(h_both_json.format, EventFormat.JSON)

        h_both_connect = DummySchemaHandler(
            {
                "value.converter": "org.apache.kafka.connect.storage.ConnectConverter",
                "key.converter": "org.apache.kafka.connect.storage.ConnectConverter",
            }
        )
        self.assertIsInstance(h_both_connect.converter, ConnectEventConverter)
        self.assertEqual(h_both_connect.format, EventFormat.CONNECT)

    def test_schemas_enable_detection(self) -> None:
        for key in (
            "value.converter.schemas.enable",
            "key.converter.schemas.enable",
            "converter.schemas.enable",
            "schemas.enable",
        ):
            h_false = DummySchemaHandler({key: "false"})
            self.assertIsInstance(h_false.converter, SchemalessJsonEventConverter)
            self.assertFalse(h_false.schemas_enabled)

            h_bool = DummySchemaHandler({key: False})
            self.assertIsInstance(h_bool.converter, SchemalessJsonEventConverter)
            self.assertFalse(h_bool.schemas_enabled)

            h_true = DummySchemaHandler({key: "true"})
            self.assertIsInstance(h_true.converter, JsonEventConverter)
            self.assertTrue(h_true.schemas_enabled)

        h_both_false = DummySchemaHandler(
            {
                "value.converter.schemas.enable": "false",
                "key.converter.schemas.enable": "false",
            }
        )
        self.assertIsInstance(h_both_false.converter, SchemalessJsonEventConverter)
        self.assertFalse(h_both_false.schemas_enabled)

    def test_connect_format_forces_schemas_enabled(self) -> None:
        handler = DummySchemaHandler(
            {"format": "connect", "value.converter.schemas.enable": "false"}
        )
        self.assertIsInstance(handler.converter, ConnectEventConverter)
        self.assertEqual(handler.format, EventFormat.CONNECT)
        self.assertTrue(handler.schemas_enabled)

    def test_heartbeat_prefix_override(self) -> None:
        conv = JsonEventConverter(heartbeat_prefix="my_prefix")
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
        self.assertEqual(handler_props.heartbeat_prefix, "custom_hb")
        self.assertEqual(handler_props.converter.heartbeat_prefix, "custom_hb")

        handler_connect = DummySchemaHandler(
            properties={"name": "test", "format": EventFormat.CONNECT},
        )
        self.assertIsInstance(handler_connect.converter, ConnectEventConverter)
        self.assertEqual(handler_connect.format, EventFormat.CONNECT)
        self.assertTrue(handler_connect.schemas_enabled)

        custom_conv = ConnectEventConverter(heartbeat_prefix="explicit_hb")
        handler_explicit = DummySchemaHandler(converter=custom_conv)
        self.assertIs(handler_explicit.converter, custom_conv)
        self.assertEqual(handler_explicit.format, EventFormat.CONNECT)
        self.assertEqual(handler_explicit.heartbeat_prefix, "explicit_hb")
        self.assertEqual(handler_explicit.converter.heartbeat_prefix, "explicit_hb")

    def test_invalid_config_raises(self) -> None:
        with self.assertRaises(ValueError):
            DummySchemaHandler({"format": "invalid_format"})
        with self.assertRaises(ValueError):
            DummySchemaHandler(
                {
                    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
                }
            )
        with self.assertRaises(ValueError):
            DummySchemaHandler(
                {"key.converter": "org.apache.kafka.connect.storage.StringConverter"}
            )
        with self.assertRaisesRegex(
            ValueError,
            "Having two different formats for event .* and key .* is not supported",
        ):
            DummySchemaHandler(
                {
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter": "org.apache.kafka.connect.storage.ConnectConverter",
                }
            )
        with self.assertRaisesRegex(
            ValueError,
            "Having two different formats for event .* and key .* is not supported",
        ):
            DummySchemaHandler(
                {
                    "format": "json",
                    "key.converter": "io.debezium.embedded.Connect",
                }
            )
        with self.assertRaisesRegex(
            ValueError,
            "Having two different formats for event .* and key .* is not supported",
        ):
            DummySchemaHandler(
                {
                    "format": "connect",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                }
            )
        with self.assertRaisesRegex(
            ValueError,
            "Conflicting format configuration in properties",
        ):
            DummySchemaHandler(
                {
                    "format": "json",
                    "value.converter": "io.debezium.embedded.Connect",
                    "key.converter": "io.debezium.embedded.Connect",
                }
            )
        with self.assertRaisesRegex(
            ValueError,
            "Having two different schema configurations for event .* and key .* is not supported",
        ):
            DummySchemaHandler(
                {
                    "value.converter.schemas.enable": "true",
                    "key.converter.schemas.enable": "false",
                }
            )
        with self.assertRaisesRegex(
            ValueError,
            "Having two different schema configurations for event .* and key .* is not supported",
        ):
            DummySchemaHandler(
                {
                    "converter.schemas.enable": "true",
                    "key.converter.schemas.enable": "false",
                }
            )
        with self.assertRaises(ValueError):
            DummySchemaHandler({"value.converter": "some.unknown.JsonLikeConverter"})
        with self.assertRaises(TypeError):
            DummySchemaHandler({"format": 123})
        with self.assertRaises(TypeError):
            DummySchemaHandler({"key.converter": 123})
        with self.assertRaises(ValueError):
            DummySchemaHandler({"value.converter.schemas.enable": "maybe"})
        with self.assertRaises(ValueError):
            DummySchemaHandler({"key.converter.schemas.enable": "maybe"})
        with self.assertRaises(ValueError):
            DummySchemaHandler({"topic.heartbeat.prefix": ""})
        with self.assertRaises(ValueError):
            DummySchemaHandler({"topic.heartbeat.prefix": "   "})
        with self.assertRaises(ValueError):
            DummySchemaHandler({"topic.heartbeat.prefix": None})
        with self.assertRaises(ValueError):
            JsonEventConverter(heartbeat_prefix="")
        with self.assertRaises(ValueError):
            JsonEventConverter(heartbeat_prefix="   ")
        with self.assertRaises(ValueError):
            JsonEventConverter(heartbeat_prefix=None)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            JsonEventConverter(default_variable_scale=40)
        with self.assertRaises(ValueError):
            JsonEventConverter(default_variable_scale=-1)
        with self.assertRaises(ValueError):
            JsonEventConverter(default_variable_scale=True)
        with self.assertRaises(TypeError):
            DummySchemaHandler(properties="not_a_mapping")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            DummySchemaHandler(converter="not_a_converter")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
