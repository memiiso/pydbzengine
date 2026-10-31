from __future__ import annotations

import unittest

from pydbzengine import (
    DebeziumConfig,
    DebeziumEngine,
    EventFormat,
)
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

    def test_unconfigured_initial_state_and_fail_fast(self) -> None:
        handler = DummySchemaHandler()
        self.assertIsNone(handler.converter)
        self.assertEqual(handler.default_variable_scale, 10)
        self.assertEqual(
            DebeziumConfig.DEFAULT_HEARTBEAT_PREFIX,
            "__debezium-heartbeat",
        )
        with self.assertRaisesRegex(RuntimeError, "is not configured"):
            handler.handle_batch([])

    def test_configure_defaults(self) -> None:
        handler = DummySchemaHandler()
        handler.configure(DebeziumConfig({}))
        self.assertIsInstance(handler.converter, JsonEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.JSON)
        self.assertTrue(handler.converter.schemas_enabled)
        self.assertEqual(
            handler.converter.heartbeat_prefix,
            DebeziumConfig.DEFAULT_HEARTBEAT_PREFIX,
        )
        self.assertEqual(handler.converter.default_variable_scale, 10)

    def test_schemas_enable_detection(self) -> None:
        for key in (
            "value.converter.schemas.enable",
            "key.converter.schemas.enable",
            "converter.schemas.enable",
            "schemas.enable",
        ):
            c_false = EventConverter.from_config(DebeziumConfig({key: "false"}))
            self.assertIsInstance(c_false, SchemalessJsonEventConverter)
            self.assertFalse(c_false.schemas_enabled)

            c_bool = EventConverter.from_config(DebeziumConfig({key: False}))
            self.assertIsInstance(c_bool, SchemalessJsonEventConverter)
            self.assertFalse(c_bool.schemas_enabled)

            c_true = EventConverter.from_config(DebeziumConfig({key: "true"}))
            self.assertIsInstance(c_true, JsonEventConverter)
            self.assertTrue(c_true.schemas_enabled)

        c_both_false = EventConverter.from_config(
            DebeziumConfig(
                {
                    "value.converter.schemas.enable": "false",
                    "key.converter.schemas.enable": "false",
                }
            )
        )
        self.assertIsInstance(c_both_false, SchemalessJsonEventConverter)
        self.assertFalse(c_both_false.schemas_enabled)

    def test_connect_format_forces_schemas_enabled(self) -> None:
        conv = EventConverter.from_config(
            DebeziumConfig(
                {"value.converter.schemas.enable": "false"},
                format="connect",
            )
        )
        self.assertIsInstance(conv, ConnectEventConverter)
        self.assertEqual(conv.format, EventFormat.CONNECT)
        self.assertTrue(conv.schemas_enabled)

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

    def test_schema_aware_change_handler_configure_builds_converter(self) -> None:
        handler_default = DummySchemaHandler()
        handler_default.configure(DebeziumConfig({}))
        self.assertIsInstance(handler_default.converter, JsonEventConverter)
        assert handler_default.converter is not None
        self.assertEqual(handler_default.converter.format, EventFormat.JSON)
        self.assertTrue(handler_default.converter.schemas_enabled)
        self.assertEqual(
            handler_default.converter.heartbeat_prefix,
            EventConverter.DEFAULT_HEARTBEAT_PREFIX,
        )

        handler_props = DummySchemaHandler()
        handler_props.configure(
            DebeziumConfig(
                {
                    "name": "test",
                    "value.converter.schemas.enable": "false",
                    "topic.heartbeat.prefix": "custom_hb",
                }
            )
        )
        self.assertIsInstance(handler_props.converter, SchemalessJsonEventConverter)
        assert handler_props.converter is not None
        self.assertEqual(handler_props.converter.format, EventFormat.JSON)
        self.assertFalse(handler_props.converter.schemas_enabled)
        self.assertEqual(handler_props.converter.heartbeat_prefix, "custom_hb")

        handler_connect = DummySchemaHandler()
        handler_connect.configure(
            DebeziumConfig({"name": "test"}, format=EventFormat.CONNECT)
        )
        self.assertIsInstance(handler_connect.converter, ConnectEventConverter)
        assert handler_connect.converter is not None
        self.assertEqual(handler_connect.converter.format, EventFormat.CONNECT)
        self.assertTrue(handler_connect.converter.schemas_enabled)

    def test_invalid_config_raises(self) -> None:
        with self.assertRaises(ValueError):
            DebeziumConfig({}, format="invalid_format")
        with self.assertRaises(TypeError):
            DebeziumConfig({}, format=123)  # type: ignore[arg-type]
        with self.assertRaisesRegex(
            ValueError,
            "Having two different schema configurations for event .* and key .* is not supported",
        ):
            DebeziumConfig(
                {
                    "value.converter.schemas.enable": "true",
                    "key.converter.schemas.enable": "false",
                }
            )
        with self.assertRaisesRegex(
            ValueError,
            "Having two different schema configurations for event .* and key .* is not supported",
        ):
            DebeziumConfig(
                {
                    "converter.schemas.enable": "true",
                    "key.converter.schemas.enable": "false",
                }
            )
        with self.assertRaises(ValueError):
            DebeziumConfig({"value.converter.schemas.enable": "maybe"})
        with self.assertRaises(ValueError):
            DebeziumConfig({"key.converter.schemas.enable": "maybe"})
        with self.assertRaises(ValueError):
            DebeziumConfig({"topic.heartbeat.prefix": ""})
        with self.assertRaises(ValueError):
            DebeziumConfig({"topic.heartbeat.prefix": "   "})
        with self.assertRaises(ValueError):
            DebeziumConfig({"topic.heartbeat.prefix": None})
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
            DebeziumConfig("not_a_mapping")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            EventConverter.from_config("not_a_config")  # type: ignore[arg-type]

    def test_schema_handler_configured_by_debezium_engine_schemaless(self) -> None:
        handler = DummySchemaHandler()
        self.assertIsNone(handler.converter)

        _ = DebeziumEngine(
            properties={"name": "test", "converter.schemas.enable": "false"},
            handler=handler,
        )
        self.assertIsInstance(handler.converter, SchemalessJsonEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.JSON)
        self.assertFalse(handler.converter.schemas_enabled)

    def test_schema_handler_configured_by_debezium_engine_connect_format_param(
        self,
    ) -> None:
        handler = DummySchemaHandler()
        _ = DebeziumEngine(
            properties={"name": "test"},
            handler=handler,
            format="connect",
        )
        self.assertIsInstance(handler.converter, ConnectEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.CONNECT)
        self.assertTrue(handler.converter.schemas_enabled)

    def test_schema_handler_configured_by_debezium_engine_heartbeat_prefix(
        self,
    ) -> None:
        handler = DummySchemaHandler()
        _ = DebeziumEngine(
            properties={"name": "test", "topic.heartbeat.prefix": "custom_hb"},
            handler=handler,
        )
        assert handler.converter is not None
        self.assertEqual(handler.converter.heartbeat_prefix, "custom_hb")

    def test_event_format_resolve_from(self) -> None:
        self.assertEqual(EventFormat.resolve_from("json"), EventFormat.JSON)
        self.assertEqual(EventFormat.resolve_from("JSON"), EventFormat.JSON)
        self.assertEqual(EventFormat.resolve_from("connect"), EventFormat.CONNECT)
        self.assertEqual(
            EventFormat.resolve_from("io.debezium.embedded.Connect"),
            EventFormat.CONNECT,
        )
        self.assertEqual(
            EventFormat.resolve_from(EventFormat.CONNECT),
            EventFormat.CONNECT,
        )
        with self.assertRaisesRegex(ValueError, "Unsupported format"):
            EventFormat.resolve_from("invalid_format")
        with self.assertRaisesRegex(TypeError, "Expected EventFormat or str"):
            EventFormat.resolve_from(123)  # type: ignore[arg-type]

    def test_debezium_config_format_resolution_and_conflicts(self) -> None:
        self.assertEqual(DebeziumConfig({}).format, EventFormat.JSON)
        self.assertEqual(
            DebeziumConfig({"engine.format": "io.debezium.embedded.Connect"}).format,
            EventFormat.CONNECT,
        )
        self.assertEqual(
            DebeziumConfig(
                {"value.converter": "org.apache.kafka.connect.storage.ConnectConverter"}
            ).format,
            EventFormat.CONNECT,
        )
        with self.assertRaisesRegex(
            ValueError,
            "Having two different formats for event .* and key .* is not supported",
        ):
            DebeziumConfig(
                {
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter": "org.apache.kafka.connect.storage.ConnectConverter",
                }
            )
        with self.assertRaisesRegex(
            ValueError,
            "Conflicting format configuration in properties",
        ):
            DebeziumConfig(
                {
                    "format": "json",
                    "value.converter": "org.apache.kafka.connect.storage.ConnectConverter",
                    "key.converter": "org.apache.kafka.connect.storage.ConnectConverter",
                }
            )
        with self.assertRaisesRegex(
            ValueError,
            "Conflicting format configuration: explicit format .* does not match property format",
        ):
            DebeziumConfig(
                {"format": "connect"},
                format="json",
            )


if __name__ == "__main__":
    unittest.main()
