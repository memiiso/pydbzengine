from __future__ import annotations

import tempfile
import threading
import uuid
from collections.abc import Mapping
from pathlib import Path

from pydbzengine import (
    BasePythonChangeHandler,
    ChangeEvent,
    DebeziumEngine,
)
from tests.support import BasePostgresqlTest


class CollectingHandler(BasePythonChangeHandler):
    """Thread-safe test handler that gathers ChangeEvent records matching a table destination."""

    def __init__(
        self,
        target_table: str = "test_all_types",
        expected_count: int = 1,
    ) -> None:
        self.target_table = target_table
        self.expected_count = expected_count
        self.records: list[ChangeEvent] = []
        self.first_event_received = threading.Event()
        self.event_received = threading.Event()

    def handle_batch(self, records: list[ChangeEvent]) -> int | None:
        for r in records:
            dest = str(r.destination())
            if self.target_table in dest:
                self.records.append(r)
                self.first_event_received.set()
                if len(self.records) >= self.expected_count:
                    self.event_received.set()
        return len(records)


class BaseSchemaIntegrationTest(BasePostgresqlTest):
    """
    Unified base test class for Debezium PostgreSQL CDC schema integration tests.
    Sets up a comprehensive PostgreSQL table with 38+ data types, builds isolated
    connector configurations per test, and provides fast reactive execution helpers.
    """

    TABLE_NAME = "test_all_types"

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._setup_source_database()

    @classmethod
    def _setup_source_database(cls) -> None:
        """Creates required PostgreSQL extensions, types, and the comprehensive test table."""
        ddl = f"""
        CREATE EXTENSION IF NOT EXISTS hstore;
        CREATE EXTENSION IF NOT EXISTS ltree;

        DO $$ BEGIN
            CREATE TYPE user_status_enum AS ENUM ('active', 'inactive', 'pending');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;

        CREATE TABLE IF NOT EXISTS public.{cls.TABLE_NAME} (
            col_id SERIAL PRIMARY KEY,
            col_smallint SMALLINT,
            col_int INTEGER,
            col_bigint BIGINT,
            col_smallserial SMALLSERIAL,
            col_bigserial BIGSERIAL,
            col_real REAL,
            col_double DOUBLE PRECISION,
            col_numeric_fixed NUMERIC(12, 4),
            col_numeric_var NUMERIC,
            col_money MONEY,
            col_boolean BOOLEAN,
            col_char CHAR(5),
            col_varchar VARCHAR(50),
            col_text TEXT,
            col_bytea BYTEA,
            col_bit BIT(8),
            col_bit_varying BIT VARYING(16),
            col_date DATE,
            col_time_micros TIME(6),
            col_time_millis TIME(3),
            col_timetz TIMETZ,
            col_timestamp_micros TIMESTAMP(6),
            col_timestamp_millis TIMESTAMP(3),
            col_timestamptz TIMESTAMPTZ,
            col_interval INTERVAL,
            col_uuid UUID,
            col_json JSON,
            col_jsonb JSONB,
            col_xml XML,
            col_enum user_status_enum,
            col_int_array INTEGER[],
            col_text_array TEXT[],
            col_bool_array BOOLEAN[],
            col_point POINT,
            col_inet INET,
            col_cidr CIDR,
            col_macaddr MACADDR,
            col_hstore HSTORE,
            col_ltree LTREE
        );

        TRUNCATE TABLE public.{cls.TABLE_NAME};

        INSERT INTO public.{cls.TABLE_NAME} (
            col_smallint, col_int, col_bigint,
            col_real, col_double, col_numeric_fixed, col_numeric_var, col_money,
            col_boolean, col_char, col_varchar, col_text,
            col_bytea, col_bit, col_bit_varying,
            col_date, col_time_micros, col_time_millis, col_timetz,
            col_timestamp_micros, col_timestamp_millis, col_timestamptz, col_interval,
            col_uuid, col_json, col_jsonb, col_xml, col_enum,
            col_int_array, col_text_array, col_bool_array,
            col_point, col_inet, col_cidr, col_macaddr,
            col_hstore, col_ltree
        ) VALUES (
            32767, 2147483647, 9223372036854775807,
            3.14, 2.7182818284, 12345678.1234, 999999.99999, '123.45',
            true, 'ABCDE', 'Hello Debezium', 'Long text value goes here',
            E'\\\\xDEADBEEF', B'10101010', B'1100110011',
            '2024-03-15', '14:30:00.123456', '14:30:00.123', '14:30:00.123456+02:00',
            '2024-03-15 14:30:00.123456', '2024-03-15 14:30:00.123', '2024-03-15 12:30:00.123456+00', '1 year 2 months 3 days 04:05:06',
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{{"key": "value"}}', '{{"nested": {{"field": 123}}}}', '<root><item>test</item></root>', 'active',
            ARRAY[1, 2, 3], ARRAY['apple', 'banana'], ARRAY[true, false],
            POINT(1.5, 2.5), '192.168.1.1', '10.0.0.0/24', '08:00:2b:01:02:03',
            '"k1"=>"v1", "k2"=>"v2"', 'Top.Science.Astronomy'
        );
        """
        cls.postgres_db.execute_sql(ddl)

    def create_isolated_connector_config(
        self,
        *,
        custom_props: Mapping[str, str] | None = None,
        unwrap_messages: bool = False,
    ) -> tuple[dict[str, str], Path]:
        """
        Builds a strictly isolated Debezium PostgreSQL connector configuration
        with unique slot, publication, and automatically cleaned temp offset file.
        """
        test_id = uuid.uuid4().hex[:8]
        offset_file = Path(tempfile.gettempdir()) / f"dbz_offset_{test_id}.dat"
        offset_file.unlink(missing_ok=True)
        self.addCleanup(offset_file.unlink, missing_ok=True)

        port = str(
            self.postgres_db.source_pg_db.get_exposed_port(
                self.postgres_db.POSTGRES_PORT_DEFAULT
            )
        )
        host = self.postgres_db.source_pg_db.get_container_host_ip()

        conf: dict[str, str] = {
            "name": f"schema-test-engine-{test_id}",
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": host,
            "database.port": port,
            "database.user": self.postgres_db.POSTGRES_USER,
            "database.password": self.postgres_db.POSTGRES_PASSWORD,
            "database.dbname": self.postgres_db.POSTGRES_DBNAME,
            "topic.prefix": f"dbz_{test_id}",
            "schema.include.list": "public",
            "table.include.list": f"public.{self.TABLE_NAME}",
            "plugin.name": "pgoutput",
            "slot.name": f"dbz_slot_{test_id}",
            "publication.name": f"dbz_pub_{test_id}",
            "slot.drop.on.stop": "true",
            "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
            "offset.storage.file.filename": str(offset_file),
            "converter.schemas.enable": "true",
            "value.converter.schemas.enable": "true",
            "key.converter.schemas.enable": "true",
            "snapshot.mode": "always",
        }

        if unwrap_messages:
            conf["transforms"] = "unwrap"
            conf["transforms.unwrap.type"] = (
                "io.debezium.transforms.ExtractNewRecordState"
            )
            conf["transforms.unwrap.add.fields"] = (
                "op,table,source.ts_ms,sourcedb,ts_ms"
            )
            conf["transforms.unwrap.delete.handling.mode"] = "rewrite"

        if custom_props:
            conf.update(custom_props)

        return conf, offset_file

    def run_engine_and_collect(
        self,
        engine: DebeziumEngine,
        handler: CollectingHandler,
        *,
        timeout_sec: float = 30.0,
    ) -> list[ChangeEvent]:
        """
        Runs the Debezium engine asynchronously and blocks until expected records
        are captured or timeout expires, then immediately shuts down the engine.
        """
        thread = threading.Thread(target=engine.run, daemon=True)
        thread.start()

        try:
            signaled = handler.event_received.wait(timeout=timeout_sec)
            if not signaled:
                raise TimeoutError(
                    f"Timed out after {timeout_sec}s waiting for {handler.expected_count} "
                    f"records matching '{handler.target_table}'."
                )
            return list(handler.records)
        finally:
            engine.close()
            thread.join(timeout=10.0)
