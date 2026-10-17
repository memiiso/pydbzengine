from __future__ import annotations

import json
import unittest

from pydbzengine.schema import (
    DebeziumConfig,
    EventFormat,
    EventStructure,
    SchemaConverter,
)
from tests.support import MockChangeEvent


class TestDebeziumConfig(unittest.TestCase):
    def test_default_config(self) -> None:
        cfg = DebeziumConfig()
        self.assertEqual(cfg.format, EventFormat.JSON)
        self.assertEqual(cfg.structure, EventStructure.ENVELOPE)
        self.assertTrue(cfg.schemas_enabled)
        self.assertTrue(cfg.is_json)
        self.assertFalse(cfg.is_connect)
        self.assertTrue(cfg.is_envelope)
        self.assertFalse(cfg.is_flattened)
        self.assertEqual(cfg.delete_handling_mode, "drop")
        self.assertEqual(cfg.default_variable_scale, 10)
        self.assertEqual(cfg.connector_config, {})

    def test_from_dict_defaults(self) -> None:
        cfg = DebeziumConfig.from_dict({})
        self.assertEqual(cfg.format, EventFormat.JSON)
        self.assertEqual(cfg.structure, EventStructure.ENVELOPE)
        self.assertTrue(cfg.schemas_enabled)
        self.assertFalse(cfg.is_flattened)
        self.assertEqual(cfg.delete_handling_mode, "drop")

    def test_from_dict_smt_unwrap_detection(self) -> None:
        raw_config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "topic.prefix": "inventory",
            "transforms": "unwrap,reroute",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
        }
        cfg = DebeziumConfig.from_dict(raw_config)
        self.assertTrue(cfg.is_flattened)
        self.assertFalse(cfg.is_envelope)
        self.assertEqual(cfg.structure, EventStructure.FLATTENED)
        self.assertEqual(cfg.delete_handling_mode, "rewrite")
        self.assertEqual(cfg.topic_prefix, "inventory")
        self.assertEqual(cfg.connector_type, "postgresql")
        self.assertEqual(cfg.connector_config["topic.prefix"], "inventory")

        # Debezium 2.x delete.tombstone.handling.mode and MongoDB ExtractNewDocumentState
        mongo_cfg = DebeziumConfig.from_dict(
            {
                "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
                "transforms.unwrap.delete.tombstone.handling.mode": "rewrite-with-tombstone",
            }
        )
        self.assertTrue(mongo_cfg.is_flattened)
        self.assertEqual(mongo_cfg.delete_handling_mode, "rewrite")
        self.assertEqual(mongo_cfg.connector_type, "mongodb")

    def test_from_dict_legacy_database_server_name(self) -> None:
        raw_config = {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.server.name": "mysql_prod",
            "value.converter.schemas.enable": "false",
        }
        cfg = DebeziumConfig.from_dict(raw_config)
        self.assertEqual(cfg.topic_prefix, "mysql_prod")
        self.assertEqual(cfg.connector_type, "mysql")
        self.assertFalse(cfg.schemas_enabled)

    def test_from_dict_connector_dialects(self) -> None:
        dialects = [
            ("io.debezium.connector.oracle.OracleConnector", "oracle"),
            ("io.debezium.connector.sqlserver.SqlServerConnector", "sqlserver"),
            ("io.debezium.connector.mongodb.MongoDbConnector", "mongodb"),
            ("io.debezium.connector.db2.Db2Connector", "db2"),
        ]
        for conn_class, expected in dialects:
            cfg = DebeziumConfig.from_dict({"connector.class": conn_class})
            self.assertEqual(cfg.connector_type, expected)

    def test_from_dict_format_and_manual_overrides(self) -> None:
        cfg = DebeziumConfig.from_dict(
            {"value.converter.schemas.enable": "true"},
            format="connect",
            structure="flattened",
            schemas_enabled=False,
            default_variable_scale=4,
        )
        self.assertEqual(cfg.format, EventFormat.CONNECT)
        self.assertTrue(cfg.is_connect)
        self.assertFalse(cfg.is_json)
        self.assertTrue(cfg.is_flattened)
        self.assertFalse(cfg.schemas_enabled)
        self.assertEqual(cfg.default_variable_scale, 4)

    def test_create_converter_factory(self) -> None:
        cfg = DebeziumConfig(schemas_enabled=True, default_variable_scale=6)
        converter = cfg.create_converter()
        self.assertIsInstance(converter, SchemaConverter)
        self.assertEqual(converter.default_variable_scale, 6)

    def test_column_name_collision_immunity_with_flattened_config(self) -> None:
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

        cfg_flat = DebeziumConfig(structure=EventStructure.FLATTENED)
        converter = cfg_flat.create_converter()
        row = converter.extract_row(rec)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["id"], 42)
        self.assertEqual(row["before"], "historical_state")
        self.assertEqual(row["after"], "current_state")

    def test_invalid_config_raises(self) -> None:
        with self.assertRaises(ValueError):
            DebeziumConfig.from_dict({}, format="invalid_format")
        with self.assertRaises(ValueError):
            DebeziumConfig.from_dict({}, structure="invalid_structure")
        with self.assertRaises(ValueError):
            DebeziumConfig.from_dict({"value.converter.schemas.enable": "maybe"})
