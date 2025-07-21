import os
import unittest
from pathlib import Path

from db_postgresql import DbPostgresql
from pydbzengine import Properties


class BasePostgresqlTest(unittest.TestCase):
    CURRENT_DIR = Path(__file__).parent
    OFFSET_FILE = CURRENT_DIR.joinpath('postgresql-offsets.dat')
    SOURCEPGDB = DbPostgresql()

    def debezium_engine_props(self, unwrap_messages=True):
        current_dir = Path(__file__).parent
        offset_file_path = current_dir.joinpath('postgresql-offsets.dat')

        props = Properties()
        props.setProperty("name", "engine")
        props.setProperty("snapshot.mode", "always")
        props.setProperty("database.hostname", self.SOURCEPGDB.CONTAINER.get_container_host_ip())
        props.setProperty("database.port", str(self.SOURCEPGDB.CONTAINER.get_exposed_port(self.SOURCEPGDB.POSTGRES_PORT_DEFAULT)))
        props.setProperty("database.user", self.SOURCEPGDB.POSTGRES_USER)
        props.setProperty("database.password", self.SOURCEPGDB.POSTGRES_PASSWORD)
        props.setProperty("database.dbname", self.SOURCEPGDB.POSTGRES_DBNAME)
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        props.setProperty("offset.storage.file.filename", offset_file_path.as_posix())
        props.setProperty("poll.interval.ms", "10000")
        props.setProperty("converter.schemas.enable", "false")
        props.setProperty("offset.flush.interval.ms", "1000")
        props.setProperty("topic.prefix", "testc")
        props.setProperty("schema.whitelist", "inventory")
        props.setProperty("database.whitelist", "inventory")
        props.setProperty("table.whitelist", "inventory.products")
        props.setProperty("replica.identity.autoset.values", "inventory.*:FULL")

        if unwrap_messages:
            props.setProperty("transforms", "unwrap")
            props.setProperty("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
            props.setProperty("transforms.unwrap.add.fields", "op,table,source.ts_ms,sourcedb,ts_ms")
            props.setProperty("transforms.unwrap.delete.handling.mode", "rewrite")
        return props

    def clean_offset_file(self):
        if self.OFFSET_FILE.exists():
            os.remove(self.OFFSET_FILE)

    def setUp(self):
        self.clean_offset_file()
        self.SOURCEPGDB.start()

    def tearDown(self):
        self.SOURCEPGDB.stop()
        self.clean_offset_file()

    def execute_on_source_db(self, sql:str):
        self.SOURCEPGDB.execute_sql(sql=sql)