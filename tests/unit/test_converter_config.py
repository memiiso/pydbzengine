from __future__ import annotations

import unittest
from types import MappingProxyType

import pydbzengine.schema as schema_pkg
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
)
from pydbzengine.schema import (
    EventFormat as SchemaEventFormat,
)


class DummySchemaHandler(SchemaAwareChangeHandler):
    pass


class TestEventConverterConfig(unittest.TestCase):
    def test_event_format_values_and_narrow_schema_exports(self) -> None:
        self.assertEqual(EventFormat.JSON, "json")
        self.assertEqual(EventFormat.CONNECT, "connect")
        self.assertIs(EventFormat, EngineEventFormat)
        self.assertIs(EventFormat, SchemaEventFormat)
        self.assertEqual(
            set(schema_pkg.__all__),
            {
                "ChangeBatch",
                "ChangeRecord",
                "ConnectEventConverter",
                "EventConverter",
                "EventFormat",
                "EventKind",
                "JsonEventConverter",
                "SchemaAwareChangeHandler",
                "SchemaDiff",
                "TableBatch",
                "TableSchema",
            },
        )
        for internal in (
            "FieldSpec",
            "SchemaField",
            "DataTypeHandler",
            "TypeRegistry",
            "SchemalessJsonEventConverter",
        ):
            self.assertFalse(hasattr(schema_pkg, internal))

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
        assert isinstance(handler.converter, JsonEventConverter)
        self.assertEqual(handler.converter.format, EventFormat.JSON)
        self.assertTrue(handler.converter.schemas_enabled)
        self.assertEqual(
            handler.converter.heartbeat_prefix,
            DebeziumConfig.DEFAULT_HEARTBEAT_PREFIX,
        )
        self.assertEqual(handler.converter.default_variable_scale, 10)

    def test_schemas_enable_detection_and_schemaless_rejection(self) -> None:
        for key in (
            "value.converter.schemas.enable",
            "key.converter.schemas.enable",
            "converter.schemas.enable",
            "schemas.enable",
        ):
            cfg_str_false = DebeziumConfig({key: "false"})
            self.assertFalse(cfg_str_false.schemas_enabled)
            with self.assertRaisesRegex(ValueError, "BasePythonChangeHandler"):
                EventConverter.from_config(cfg_str_false)

            cfg_bool_false = DebeziumConfig({key: False})
            self.assertFalse(cfg_bool_false.schemas_enabled)
            with self.assertRaisesRegex(ValueError, "BasePythonChangeHandler"):
                EventConverter.from_config(cfg_bool_false)

            c_true = EventConverter.from_config(DebeziumConfig({key: "true"}))
            self.assertIsInstance(c_true, JsonEventConverter)
            self.assertTrue(c_true.schemas_enabled)

        cfg_both_false = DebeziumConfig(
            {
                "value.converter.schemas.enable": "false",
                "key.converter.schemas.enable": "false",
            }
        )
        self.assertFalse(cfg_both_false.schemas_enabled)
        with self.assertRaisesRegex(ValueError, "BasePythonChangeHandler"):
            EventConverter.from_config(cfg_both_false)

    def test_connect_format_schemas_always_enabled(self) -> None:
        conv = EventConverter.from_config(
            DebeziumConfig(
                {"converter.schemas.enable": "false"},
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
                    "value.converter.schemas.enable": "true",
                    "topic.heartbeat.prefix": "custom_hb",
                }
            )
        )
        self.assertIsInstance(handler_props.converter, JsonEventConverter)
        assert handler_props.converter is not None
        self.assertEqual(handler_props.converter.format, EventFormat.JSON)
        self.assertTrue(handler_props.converter.schemas_enabled)
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
            EventConverter.from_config(
                DebeziumConfig(
                    {
                        "value.converter.schemas.enable": "true",
                        "key.converter.schemas.enable": "false",
                    }
                )
            )
        with self.assertRaisesRegex(
            ValueError,
            "Having two different schema configurations for event .* and key .* is not supported",
        ):
            EventConverter.from_config(
                DebeziumConfig(
                    {
                        "converter.schemas.enable": "true",
                        "key.converter.schemas.enable": "false",
                    }
                )
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

    def test_schema_handler_rejected_by_debezium_engine_when_schemaless(self) -> None:
        handler = DummySchemaHandler()
        with self.assertRaisesRegex(ValueError, "BasePythonChangeHandler"):
            DebeziumEngine(
                properties={"name": "test", "converter.schemas.enable": "false"},
                handler=handler,
            )

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
        self.assertEqual(
            EventFormat.resolve_from("io.debezium.engine.format.Json"),
            EventFormat.JSON,
        )
        self.assertEqual(
            EventFormat.resolve_from("org.apache.kafka.connect.json.JsonConverter"),
            EventFormat.JSON,
        )
        self.assertEqual(EventFormat.resolve_from("connect"), EventFormat.CONNECT)
        self.assertEqual(
            EventFormat.resolve_from("io.debezium.embedded.Connect"),
            EventFormat.CONNECT,
        )
        self.assertEqual(
            EventFormat.resolve_from(
                "org.apache.kafka.connect.storage.ConnectConverter"
            ),
            EventFormat.CONNECT,
        )
        self.assertEqual(
            EventFormat.resolve_from(EventFormat.CONNECT),
            EventFormat.CONNECT,
        )
        for unsupported in (
            "invalid_format",
            "org.apache.kafka.connect.storage.StringConverter",
        ):
            with self.assertRaisesRegex(ValueError, "Unsupported format"):
                EventFormat.resolve_from(unsupported)
        with self.assertRaisesRegex(TypeError, "Expected EventFormat or str"):
            EventFormat.resolve_from(123)  # type: ignore[arg-type]

    def test_debezium_config_format_resolution_and_conflicts(self) -> None:
        self.assertEqual(DebeziumConfig({}).format, EventFormat.JSON)
        self.assertEqual(
            DebeziumConfig(
                {"value.converter": "org.apache.kafka.connect.json.JsonConverter"}
            ).format,
            EventFormat.JSON,
        )
        self.assertEqual(
            DebeziumConfig({"converter.class": "io.debezium.embedded.Connect"}).format,
            EventFormat.CONNECT,
        )
        self.assertEqual(
            DebeziumConfig(
                {"key.converter": "org.apache.kafka.connect.storage.ConnectConverter"}
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
                    "converter.class": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter": "org.apache.kafka.connect.storage.ConnectConverter",
                }
            )
        with self.assertRaisesRegex(
            ValueError,
            "Conflicting format configuration: explicit format .* does not match property format",
        ):
            DebeziumConfig(
                {"converter.class": "io.debezium.embedded.Connect"},
                format="json",
            )
        with self.assertRaisesRegex(ValueError, "Unsupported format"):
            EventConverter.from_config(
                DebeziumConfig(
                    {
                        "key.converter": "org.apache.kafka.connect.storage.StringConverter"
                    }
                )
            )
        with self.assertRaisesRegex(TypeError, "Expected EventFormat or str"):
            DebeziumConfig({"value.converter": None})
        with self.assertRaisesRegex(TypeError, "Expected EventFormat or str"):
            DebeziumConfig({"key.converter": None})

    def test_debezium_config_mapping_jproperties_and_immutability(self) -> None:
        from pydbzengine.engine.jvm import ensure_jvm_started

        ensure_jvm_started()
        from pydbzengine.engine._jvm import JProperties

        raw_dict = {"name": "pg-conn", "schemas.enable": "false"}
        cfg_from_dict = DebeziumConfig(raw_dict)
        raw_dict["schemas.enable"] = "true"
        raw_dict["injected"] = "ignored"
        self.assertFalse(cfg_from_dict.schemas_enabled)
        self.assertNotIn("injected", cfg_from_dict.properties)
        with self.assertRaises(TypeError):
            cfg_from_dict.properties["name"] = "mutated"  # type: ignore[index]

        cfg_from_proxy = DebeziumConfig(
            MappingProxyType({"name": "proxy-conn", "topic.heartbeat.prefix": "hb"})
        )
        self.assertEqual(cfg_from_proxy.heartbeat_prefix, "hb")

        j_props = JProperties()
        j_props.setProperty("name", "java-conn")
        j_props.setProperty("converter.schemas.enable", "true")
        j_props.setProperty("topic.heartbeat.prefix", "java_hb")

        handler = DummySchemaHandler()
        engine = DebeziumEngine(properties=j_props, handler=handler)
        self.assertTrue(engine.config.schemas_enabled)
        self.assertEqual(engine.config.heartbeat_prefix, "java_hb")
        self.assertIsInstance(handler.converter, JsonEventConverter)

    def test_debezium_config_kwargs_conflicts_and_to_engine_properties(self) -> None:
        cfg_kw = DebeziumConfig(
            {"name": "c1"},
            schemas_enabled=False,
            heartbeat_prefix="  custom_hb  ",
        )
        self.assertFalse(cfg_kw.schemas_enabled)
        self.assertEqual(cfg_kw.heartbeat_prefix, "custom_hb")
        engine_props = cfg_kw.to_engine_properties()
        self.assertEqual(engine_props["converter.schemas.enable"], "false")
        self.assertEqual(engine_props["topic.heartbeat.prefix"], "custom_hb")

        # Top-level schemas.enable=false is canonicalized to converter.schemas.enable=false for Java ConverterBuilder
        cfg_top_level = DebeziumConfig({"name": "c2", "schemas.enable": False})
        self.assertEqual(
            cfg_top_level.to_engine_properties()["converter.schemas.enable"],
            "false",
        )

        # Standalone empty DebeziumConfig() works without passing {}
        cfg_standalone = DebeziumConfig(schemas_enabled=False)
        self.assertFalse(cfg_standalone.schemas_enabled)
        self.assertEqual(cfg_standalone.format, EventFormat.JSON)

        # DebeziumEngine accepts a pre-built DebeziumConfig directly
        cfg_enabled = DebeziumConfig({"name": "c3"}, heartbeat_prefix="custom_hb")
        handler = DummySchemaHandler()
        engine = DebeziumEngine(properties=cfg_enabled, handler=handler)
        self.assertIs(engine.config, cfg_enabled)
        self.assertIsInstance(handler.converter, JsonEventConverter)

        # Conflict validations
        with self.assertRaisesRegex(ValueError, "CONNECT always carries schemas"):
            DebeziumConfig(format="connect", schemas_enabled=False)
        with self.assertRaisesRegex(ValueError, "Conflicting schema configuration"):
            DebeziumConfig({"schemas.enable": "false"}, schemas_enabled=True)
        with self.assertRaisesRegex(ValueError, "Conflicting schema configuration"):
            DebeziumConfig(
                {"converter.schemas.enable": "true", "schemas.enable": "false"}
            )
        with self.assertRaisesRegex(
            ValueError, "Conflicting heartbeat_prefix configuration"
        ):
            DebeziumConfig({"topic.heartbeat.prefix": "hb_a"}, heartbeat_prefix="hb_b")
        with self.assertRaises(ValueError):
            DebeziumConfig(heartbeat_prefix="   ")
        with self.assertRaises(TypeError):
            DebeziumConfig(schemas_enabled="false")  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "Conflicting format configuration"):
            DebeziumEngine(
                properties=DebeziumConfig({"name": "c"}, format="connect"),
                handler=DummySchemaHandler(),
                format="json",
            )

    def test_schema_aware_change_handler_standalone_init_and_validation(self) -> None:
        h_cfg = DummySchemaHandler(config=DebeziumConfig(format=EventFormat.CONNECT))
        self.assertIsInstance(h_cfg.converter, ConnectEventConverter)
        self.assertEqual(h_cfg.handle_batch([]), 0)

        with self.assertRaisesRegex(ValueError, "BasePythonChangeHandler"):
            DummySchemaHandler(config=DebeziumConfig(schemas_enabled=False))

        custom_conv = JsonEventConverter(heartbeat_prefix="standalone_hb")
        h_conv = DummySchemaHandler(converter=custom_conv)
        self.assertIs(h_conv.converter, custom_conv)
        self.assertEqual(h_conv.handle_batch([]), 0)

        # Attaching handler with explicit converter to DebeziumEngine preserves the converter
        _ = DebeziumEngine(properties={"name": "c_keep"}, handler=h_conv, format="json")
        self.assertIs(h_conv.converter, custom_conv)
        self.assertEqual(h_conv.converter.heartbeat_prefix, "standalone_hb")

        # Mismatched engine format or schemaless config fails fast without overwriting converter
        with self.assertRaisesRegex(ValueError, "does not match explicitly provided"):
            DebeziumEngine(
                properties={"name": "c_bad_fmt"}, handler=h_conv, format="connect"
            )
        self.assertIs(h_conv.converter, custom_conv)
        with self.assertRaisesRegex(ValueError, "BasePythonChangeHandler"):
            DebeziumEngine(
                properties={
                    "name": "c_schemaless",
                    "converter.schemas.enable": "false",
                },
                handler=h_conv,
            )
        self.assertIs(h_conv.converter, custom_conv)

        # Setting only value.converter.schemas.enable does not inject converter.schemas.enable
        val_only_cfg = DebeziumConfig(
            {"name": "c_val_only", "value.converter.schemas.enable": "false"}
        )
        self.assertFalse(val_only_cfg.value_schemas_enabled)
        self.assertTrue(val_only_cfg.key_schemas_enabled)
        self.assertNotIn(
            "converter.schemas.enable", val_only_cfg.to_engine_properties()
        )

        with self.assertRaisesRegex(ValueError, "not both"):
            DummySchemaHandler(
                converter=custom_conv,
                config=DebeziumConfig(),
            )
        with self.assertRaises(TypeError):
            DummySchemaHandler(converter="not_a_converter")  # type: ignore[arg-type]
        for bad_scale in (-1, 39, True):
            with self.assertRaises(ValueError):
                DummySchemaHandler(default_variable_scale=bad_scale)


if __name__ == "__main__":
    unittest.main()
