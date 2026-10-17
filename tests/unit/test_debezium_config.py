from __future__ import annotations

import json
import unittest

from pydbzengine.schema import (
    ConverterConfig,
    DebeziumConfig,
    EventFormat,
    SchemaConverter,
)
from tests.support import MockChangeEvent


class TestConverterConfig(unittest.TestCase):
    def test_alias_and_event_format_import(self) -> None:
        self.assertIs(DebeziumConfig, ConverterConfig)
        self.assertEqual(EventFormat.JSON, "json")
        self.assertEqual(EventFormat.CONNECT, "connect")

    def test_default_config(self) -> None:
        cfg = ConverterConfig()
        self.assertEqual(cfg.format, EventFormat.JSON)
        self.assertTrue(cfg.schemas_enabled)
        self.assertTrue(cfg.is_json)
        self.assertFalse(cfg.is_connect)
        self.assertEqual(cfg.default_variable_scale, 10)

    def test_from_properties_defaults(self) -> None:
        cfg = ConverterConfig.from_properties({})
        self.assertEqual(cfg.format, EventFormat.JSON)
        self.assertTrue(cfg.schemas_enabled)
        self.assertEqual(cfg.default_variable_scale, 10)

    def test_from_dict_schemas_enable_detection(self) -> None:
        for key in (
            "value.converter.schemas.enable",
            "converter.schemas.enable",
            "schemas.enable",
        ):
            cfg_false = ConverterConfig.from_properties({key: "false"})
            self.assertFalse(cfg_false.schemas_enabled)

            cfg_bool = ConverterConfig.from_properties({key: False})
            self.assertFalse(cfg_bool.schemas_enabled)

            cfg_true = ConverterConfig.from_properties({key: "true"})
            self.assertTrue(cfg_true.schemas_enabled)

    def test_from_dict_format_and_manual_overrides(self) -> None:
        cfg = ConverterConfig.from_properties(
            {"value.converter.schemas.enable": "true"},
            format="connect",
            schemas_enabled=False,
            default_variable_scale=4,
        )
        self.assertEqual(cfg.format, EventFormat.CONNECT)
        self.assertTrue(cfg.is_connect)
        self.assertFalse(cfg.is_json)
        self.assertFalse(cfg.schemas_enabled)
        self.assertEqual(cfg.default_variable_scale, 4)

    def test_resolve_and_converter_instantiation(self) -> None:
        cfg = ConverterConfig(schemas_enabled=True, default_variable_scale=6)
        converter = SchemaConverter(cfg)
        self.assertIsInstance(converter, SchemaConverter)
        self.assertEqual(converter.default_variable_scale, 6)
        self.assertTrue(converter.config.is_json)

        overridden = ConverterConfig.from_properties(
            cfg, format="connect", default_variable_scale=8
        )
        self.assertTrue(overridden.is_connect)
        self.assertEqual(overridden.default_variable_scale, 8)

    def test_column_name_collision_immunity_in_pure_conversion(self) -> None:
        payload = {
            "id": 42,
            "before": "historical_state",
            "after": "current_state",
            "__op": "u",
            "__deleted": "false",
        }
        rec = MockChangeEvent(
            key=json.dumps({"id": 42}),
            value=json.dumps(payload),
            destination="audit.changelog",
        )

        converter = SchemaConverter(schemas_enabled=False)
        tbl = converter.to_arrow([rec])
        self.assertIsNotNone(tbl)
        assert tbl is not None
        row = tbl.to_pylist()[0]
        self.assertEqual(row["id"], 42)
        self.assertEqual(row["before"], "historical_state")
        self.assertEqual(row["after"], "current_state")

    def test_invalid_config_raises(self) -> None:
        with self.assertRaises(ValueError):
            ConverterConfig.from_properties({}, format="invalid_format")
        with self.assertRaises(ValueError):
            ConverterConfig.from_properties({"value.converter.schemas.enable": "maybe"})
        with self.assertRaises(ValueError):
            DebeziumConfig.from_dict({}, format="invalid_format")
        with self.assertRaises(ValueError):
            DebeziumConfig.from_dict({"value.converter.schemas.enable": "maybe"})
