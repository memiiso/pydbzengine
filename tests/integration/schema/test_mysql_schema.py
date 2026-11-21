from __future__ import annotations

import datetime
import random
import tempfile
import threading
import uuid
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from pydbzengine import DebeziumEngine, EventFormat
from pydbzengine.schema import (
    ConnectEventConverter,
    EventKind,
    JsonEventConverter,
)
from tests.integration.schema.base import RecordingSchemaChangeHandler
from tests.support import BaseMySqlTest


@pytest.mark.integration
class TestMySqlSchemaIntegration(BaseMySqlTest):
    """
    End-to-end MySQL CDC schema integration tests against a real MySQL database
    and SchemaAwareChangeHandler consumer in both schema-enabled converter modes:
    1. JSON with schemas enabled (JsonEventConverter)
    2. Connect mode (ConnectEventConverter)

    Covers MySQL-specific data types (YEAR, SET, ENUM, TINYINT, MEDIUMINT, BIT(1),
    BIT(16), DECIMAL, DATETIME, TIMESTAMP, POINT, GEOMETRY, JSON, BLOB/VARBINARY),
    keyless tables, MySQL DDL EventKind.SCHEMA_CHANGE events, and live ALTER TABLE evolution.
    """

    TABLE_NAME = "test_mysql_all_types"

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._setup_mysql_all_types_table()

    @classmethod
    def _setup_mysql_all_types_table(cls) -> None:
        ddl = f"""
        DROP TABLE IF EXISTS inventory.{cls.TABLE_NAME};

        CREATE TABLE inventory.{cls.TABLE_NAME} (
            col_id INT AUTO_INCREMENT PRIMARY KEY,
            col_tinyint TINYINT,
            col_smallint SMALLINT,
            col_mediumint MEDIUMINT,
            col_int INT,
            col_bigint BIGINT,
            col_float FLOAT,
            col_double DOUBLE,
            col_decimal DECIMAL(12, 4),
            col_boolean BOOLEAN,
            col_bit1 BIT(1),
            col_bit16 BIT(16),
            col_year YEAR,
            col_date DATE,
            col_time TIME(6),
            col_datetime_ms DATETIME(3),
            col_datetime_us DATETIME(6),
            col_timestamp TIMESTAMP(6),
            col_char CHAR(5),
            col_varchar VARCHAR(50),
            col_text TEXT,
            col_binary BINARY(4),
            col_varbinary VARBINARY(16),
            col_blob BLOB,
            col_enum ENUM('small', 'medium', 'large'),
            col_set SET('read', 'write', 'admin'),
            col_json JSON,
            col_point POINT,
            col_geometry GEOMETRY
        );

        INSERT INTO inventory.{cls.TABLE_NAME} (
            col_tinyint, col_smallint, col_mediumint, col_int, col_bigint,
            col_float, col_double, col_decimal,
            col_boolean, col_bit1, col_bit16,
            col_year, col_date, col_time,
            col_datetime_ms, col_datetime_us, col_timestamp,
            col_char, col_varchar, col_text,
            col_binary, col_varbinary, col_blob,
            col_enum, col_set, col_json,
            col_point, col_geometry
        ) VALUES (
            127, 32767, 8388607, 2147483647, 9223372036854775807,
            3.14, 2.7182818284, 12345678.1234,
            TRUE, b'1', b'1010101011001100',
            2024, '2024-03-15', '14:30:00.123456',
            '2024-03-15 14:30:00.123', '2024-03-15 14:30:00.123456', '2024-03-15 12:30:00.123456',
            'ABCDE', 'Hello MySQL', 'Long MySQL text value',
            X'DEADBEEF', X'CAFEBABE', X'01020304',
            'medium', 'read,write', '{{"db": "mysql", "version": 8}}',
            ST_PointFromText('POINT(1.5 2.5)'),
            ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')
        );
        """
        cls.mysql_db.execute_sql(ddl)

    def create_isolated_mysql_config(
        self,
        *,
        table_name: str | None = None,
        custom_props: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        target = table_name or self.TABLE_NAME
        test_id = uuid.uuid4().hex[:8]
        offset_file = Path(tempfile.gettempdir()) / f"dbz_mysql_offset_{test_id}.dat"
        history_file = Path(tempfile.gettempdir()) / f"dbz_mysql_history_{test_id}.dat"
        offset_file.unlink(missing_ok=True)
        history_file.unlink(missing_ok=True)
        self.addCleanup(offset_file.unlink, missing_ok=True)
        self.addCleanup(history_file.unlink, missing_ok=True)

        port = str(
            self.mysql_db.source_mysql_db.get_exposed_port(
                self.mysql_db.MYSQL_PORT_DEFAULT
            )
        )
        host = self.mysql_db.source_mysql_db.get_container_host_ip()

        conf: dict[str, str] = {
            "name": f"mysql-schema-test-{test_id}",
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": host,
            "database.port": port,
            "database.user": self.mysql_db.MYSQL_USER,
            "database.password": self.mysql_db.MYSQL_PASSWORD,
            "database.server.id": str(random.randint(100000, 999999)),
            "topic.prefix": f"dbz_mysql_{test_id}",
            "database.include.list": "inventory",
            "table.include.list": f"inventory.{target}",
            "include.schema.changes": "true",
            "schema.history.internal": "io.debezium.storage.file.history.FileSchemaHistory",
            "schema.history.internal.file.filename": str(history_file),
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

    def run_mysql_consumer(
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
                    f"MySQL records matching '{handler.target_table}'."
                )
        finally:
            engine.close()
            thread.join(timeout=10.0)

    def test_mysql_json_all_types_and_ddl_schema_changes(self) -> None:
        """
        Verifies JsonEventConverter and SchemaAwareChangeHandler on MySQL snapshot
        across all MySQL data types (including YEAR, SET, ENUM, TINYINT, MEDIUMINT,
        BIT, POINT, GEOMETRY) plus MySQL DDL EventKind.SCHEMA_CHANGE events.
        """
        props = self.create_isolated_mysql_config()
        handler = RecordingSchemaChangeHandler(
            target_table=self.TABLE_NAME,
            expected_count=1,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format="json")
        self.assertIsInstance(handler.converter, JsonEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.JSON)
        self.assertTrue(handler.converter.schemas_enabled)
        self.run_mysql_consumer(engine, handler)

        # 1. Verify MySQL DDL Schema Change events were classified as EventKind.SCHEMA_CHANGE
        self.assertGreaterEqual(len(handler.schema_change_records), 1)
        self.assertEqual(handler.schema_change_records[0].kind, EventKind.SCHEMA_CHANGE)

        # 2. Verify TableSchema & PyArrow Types for MySQL columns
        self.assertEqual(len(handler.created_schemas), 1)
        table_schema = handler.created_schemas[0]
        self.assertEqual(table_schema.key_fields, ("col_id",))

        after_struct = table_schema.arrow_schema.field("after").type
        self.assertTrue(pa.types.is_struct(after_struct))
        self.assertEqual(after_struct.field("col_id").type, pa.int32())
        self.assertEqual(after_struct.field("col_tinyint").type, pa.int16())
        self.assertEqual(after_struct.field("col_smallint").type, pa.int16())
        self.assertEqual(after_struct.field("col_mediumint").type, pa.int32())
        self.assertEqual(after_struct.field("col_int").type, pa.int32())
        self.assertEqual(after_struct.field("col_bigint").type, pa.int64())
        self.assertEqual(after_struct.field("col_float").type, pa.float32())
        self.assertEqual(after_struct.field("col_double").type, pa.float64())
        self.assertEqual(after_struct.field("col_decimal").type, pa.decimal128(12, 4))
        self.assertEqual(after_struct.field("col_bit1").type, pa.bool_())
        self.assertEqual(after_struct.field("col_bit16").type, pa.binary())
        self.assertEqual(after_struct.field("col_year").type, pa.int32())
        self.assertEqual(after_struct.field("col_date").type, pa.date32())
        self.assertEqual(after_struct.field("col_time").type, pa.time64("us"))
        self.assertEqual(after_struct.field("col_datetime_ms").type, pa.timestamp("ms"))
        self.assertEqual(after_struct.field("col_datetime_us").type, pa.timestamp("us"))
        self.assertEqual(
            after_struct.field("col_timestamp").type, pa.timestamp("us", tz="UTC")
        )
        self.assertEqual(after_struct.field("col_char").type, pa.string())
        self.assertEqual(after_struct.field("col_varchar").type, pa.string())
        self.assertEqual(after_struct.field("col_text").type, pa.string())
        self.assertEqual(after_struct.field("col_binary").type, pa.binary())
        self.assertEqual(after_struct.field("col_varbinary").type, pa.binary())
        self.assertEqual(after_struct.field("col_blob").type, pa.binary())
        self.assertEqual(after_struct.field("col_enum").type, pa.string())
        self.assertEqual(after_struct.field("col_set").type, pa.string())
        self.assertEqual(after_struct.field("col_json").type, pa.string())
        self.assertTrue(pa.types.is_struct(after_struct.field("col_point").type))
        self.assertTrue(pa.types.is_struct(after_struct.field("col_geometry").type))

        # Verify Debezium logical_type metadata preserved on pass-through logical types
        for col_name, expected_logical in (
            ("col_year", b"io.debezium.time.Year"),
            ("col_enum", b"io.debezium.data.Enum"),
            ("col_set", b"io.debezium.data.EnumSet"),
        ):
            meta = after_struct.field(col_name).metadata
            assert meta is not None
            self.assertEqual(meta[b"debezium.logical_type"], expected_logical)

        # 3. Verify Coerced Values and Materialized Arrow Table
        record = handler.records[0]
        self.assertEqual(record.kind, EventKind.DATA)
        assert record.value is not None
        self.assertEqual(record.value["op"], "r")
        row = record.value["after"]
        self.assertEqual(row["col_tinyint"], 127)
        self.assertEqual(row["col_smallint"], 32767)
        self.assertEqual(row["col_mediumint"], 8388607)
        self.assertEqual(row["col_int"], 2147483647)
        self.assertEqual(row["col_bigint"], 9223372036854775807)
        self.assertAlmostEqual(row["col_float"], 3.14, places=2)
        self.assertAlmostEqual(row["col_double"], 2.7182818284, places=6)
        self.assertEqual(row["col_decimal"], Decimal("12345678.1234"))
        self.assertEqual(row["col_bit1"], True)
        self.assertIsInstance(row["col_bit16"], bytes)
        self.assertEqual(row["col_year"], 2024)
        self.assertEqual(row["col_char"], "ABCDE")
        self.assertEqual(row["col_varchar"], "Hello MySQL")
        self.assertEqual(row["col_binary"], b"\xde\xad\xbe\xef")
        self.assertEqual(row["col_varbinary"], b"\xca\xfe\xba\xbe")
        self.assertEqual(row["col_blob"], b"\x01\x02\x03\x04")
        self.assertEqual(row["col_enum"], "medium")
        self.assertEqual(row["col_set"], "read,write")
        self.assertIn("mysql", row["col_json"])
        self.assertEqual(row["col_point"]["x"], 1.5)
        self.assertEqual(row["col_point"]["y"], 2.5)
        self.assertIsInstance(row["col_geometry"]["wkb"], bytes)

        arrow_tbl = handler.table_batches[0].to_arrow()
        self.assertEqual(arrow_tbl.num_rows, 1)
        arrow_row = arrow_tbl.to_pylist()[0]["after"]
        self.assertEqual(arrow_row["col_date"], datetime.date(2024, 3, 15))
        self.assertEqual(arrow_row["col_year"], 2024)

    def test_mysql_connect_all_types_and_live_schema_evolution(self) -> None:
        """
        Verifies ConnectEventConverter and SchemaAwareChangeHandler on MySQL across
        MySQL types, followed by live UPDATE, ALTER TABLE ADD COLUMN, INSERT, and DELETE.
        """
        table_name = "test_mysql_connect_crud"
        self.execute_on_source_db(
            f"""
            DROP TABLE IF EXISTS inventory.{table_name};
            CREATE TABLE inventory.{table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                release_year YEAR,
                flags SET('a', 'b', 'c'),
                price DECIMAL(10, 2),
                location POINT
            );
            INSERT INTO inventory.{table_name} (name, release_year, flags, price, location)
            VALUES ('item_1', 2024, 'a,c', 19.99, ST_PointFromText('POINT(10.0 20.0)'));
            """
        )
        self.addCleanup(
            self.execute_on_source_db, f"DROP TABLE IF EXISTS inventory.{table_name};"
        )

        props = self.create_isolated_mysql_config(table_name=table_name)
        # 1 READ + 1 UPDATE + 1 INSERT (after ALTER TABLE) + 1 DELETE + 1 TOMBSTONE = 5 records
        handler = RecordingSchemaChangeHandler(
            target_table=table_name,
            expected_count=5,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format="connect")
        self.assertIsInstance(handler.converter, ConnectEventConverter)
        assert handler.converter is not None
        self.assertEqual(handler.converter.format, EventFormat.CONNECT)
        self.assertTrue(handler.converter.schemas_enabled)

        def _mutate_mysql() -> None:
            handler.first_event_received.wait(timeout=15.0)
            self.execute_on_source_db(
                f"""
                UPDATE inventory.{table_name} SET name = 'item_1_updated' WHERE id = 1;
                ALTER TABLE inventory.{table_name} ADD COLUMN category ENUM('tech', 'books') DEFAULT 'tech';
                INSERT INTO inventory.{table_name} (name, release_year, flags, price, location, category)
                VALUES ('item_2', 2025, 'b', 49.95, ST_PointFromText('POINT(30.0 40.0)'), 'books');
                DELETE FROM inventory.{table_name} WHERE id = 1;
                """
            )

        threading.Thread(target=_mutate_mysql, daemon=True).start()
        self.run_mysql_consumer(engine, handler, timeout_sec=35.0)

        self.assertEqual(len(handler.created_schemas), 1)
        self.assertEqual(handler.created_schemas[0].key_fields, ("id",))
        self.assertGreaterEqual(len(handler.evolutions), 1)
        new_schema, old_schema, diff = handler.evolutions[-1]
        self.assertTrue(diff.is_additive)
        self.assertTrue(diff.is_compatible)
        self.assertFalse(diff.key_fields_changed)
        self.assertFalse(diff.key_schema_changed)
        self.assertEqual(diff.removed_fields, ())
        self.assertEqual(diff.added_fields, ())
        old_after = old_schema.arrow_schema.field("after").type
        new_after = new_schema.arrow_schema.field("after").type
        self.assertTrue(pa.types.is_struct(old_after))
        self.assertTrue(pa.types.is_struct(new_after))
        self.assertNotIn("category", [f.name for f in old_after])
        self.assertIn("category", [f.name for f in new_after])

        ops = [
            r.value["op"]
            for r in handler.records
            if r.kind is EventKind.DATA and r.value is not None
        ]
        self.assertIn("r", ops)
        self.assertIn("u", ops)
        self.assertIn("c", ops)
        self.assertIn("d", ops)

        insert_rec = next(
            r
            for r in handler.records
            if r.kind is EventKind.DATA
            and r.value is not None
            and r.value.get("op") == "c"
        )
        assert insert_rec.value is not None
        self.assertEqual(insert_rec.value["after"]["name"], "item_2")
        self.assertEqual(insert_rec.value["after"]["category"], "books")

        tombstones = [r for r in handler.records if r.kind is EventKind.TOMBSTONE]
        self.assertGreaterEqual(len(tombstones), 1)
        self.assertIsNone(tombstones[0].value)
        self.assertEqual(tombstones[0].key, {"id": 1})

        self.assertGreaterEqual(len(handler.table_batches), 2)
        self.assertTrue(
            any(
                "category" in [f.name for f in t.schema.field("after").type]
                for t in handler.arrow_tables
            )
        )
        aligned_initial = handler.table_batches[0].to_arrow(
            target_schema=new_schema.arrow_schema
        )
        self.assertEqual(aligned_initial.schema, new_schema.arrow_schema)
        self.assertIsNone(aligned_initial.to_pylist()[0]["after"]["category"])

    def _run_mysql_keyless_table_test(
        self,
        *,
        fmt: str,
        expected_converter: type[ConnectEventConverter | JsonEventConverter],
    ) -> None:
        table_name = f"test_mysql_keyless_{fmt}"
        self.execute_on_source_db(
            f"""
            DROP TABLE IF EXISTS inventory.{table_name};
            CREATE TABLE inventory.{table_name} (
                label VARCHAR(50) NOT NULL,
                score INT
            );
            INSERT INTO inventory.{table_name} (label, score) VALUES ('no_pk_{fmt}', 77);
            """
        )
        self.addCleanup(
            self.execute_on_source_db, f"DROP TABLE IF EXISTS inventory.{table_name};"
        )

        props = self.create_isolated_mysql_config(table_name=table_name)
        handler = RecordingSchemaChangeHandler(
            target_table=table_name,
            expected_count=1,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format=fmt)
        self.assertIsInstance(handler.converter, expected_converter)
        self.run_mysql_consumer(engine, handler, timeout_sec=35.0)

        self.assertEqual(len(handler.created_schemas), 1)
        table_schema = handler.created_schemas[0]
        self.assertEqual(table_schema.key_fields, ())
        self.assertIsNone(table_schema.key_schema)
        rec = handler.records[0]
        self.assertIsNone(rec.key)
        assert rec.value is not None
        self.assertEqual(rec.value["after"]["label"], f"no_pk_{fmt}")
        self.assertEqual(rec.value["after"]["score"], 77)
        self.assertEqual(handler.table_batches[0].to_arrow().num_rows, 1)

    def test_mysql_connect_keyless_table_empty_key_schema(self) -> None:
        self._run_mysql_keyless_table_test(
            fmt="connect", expected_converter=ConnectEventConverter
        )

    def test_mysql_json_keyless_table_empty_key_schema(self) -> None:
        self._run_mysql_keyless_table_test(
            fmt="json", expected_converter=JsonEventConverter
        )

    def test_mysql_example_inventory_customers_snapshot(self) -> None:
        """Verify snapshot of the pre-populated inventory.customers table in debezium/example-mysql."""
        props = self.create_isolated_mysql_config(table_name="customers")
        handler = RecordingSchemaChangeHandler(
            target_table="customers",
            expected_count=4,
        )
        engine = DebeziumEngine(properties=props, handler=handler, format="json")
        self.run_mysql_consumer(engine, handler, timeout_sec=35.0)

        self.assertEqual(len(handler.created_schemas), 1)
        read_records = [
            r
            for r in handler.records
            if r.kind is EventKind.DATA
            and r.value is not None
            and r.value.get("op") == "r"
        ]
        self.assertEqual(len(read_records), 4)
        emails = {
            r.value["after"]["email"]
            for r in read_records
            if r.value is not None and r.value["after"] is not None
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
        after_struct = arrow_tbl.schema.field("after").type
        self.assertEqual(
            [f.name for f in after_struct], ["id", "first_name", "last_name", "email"]
        )
