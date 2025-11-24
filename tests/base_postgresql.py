import os
import unittest
from pathlib import Path

from db_postgresql import DbPostgresql
from pydbzengine import Properties


class BasePostgresqlTest(unittest.TestCase):
    CURRENT_DIR = Path(__file__).parent
    OFFSET_FILE = CURRENT_DIR.joinpath('postgresql-offsets.dat')

    def __init__(self):
        super().__init__()
        self.SOURCEPGDB = DbPostgresql()

    def debezium_engine_props_dict(self, unwrap_messages=True) -> dict:
        current_dir = Path(__file__).parent
        offset_file_path = current_dir.joinpath('postgresql-offsets.dat')

        conf: dict = {}
        conf.setdefault("name", "engine")
        conf.setdefault("snapshot.mode", "always")
        conf.setdefault("database.hostname", self.SOURCEPGDB.sourcePgDb.get_container_host_ip())
        conf.setdefault("database.port",
                        str(self.SOURCEPGDB.sourcePgDb.get_exposed_port(self.SOURCEPGDB.POSTGRES_PORT_DEFAULT)))
        conf.setdefault("database.user", self.SOURCEPGDB.POSTGRES_USER)
        conf.setdefault("database.password", self.SOURCEPGDB.POSTGRES_PASSWORD)
        conf.setdefault("database.dbname", self.SOURCEPGDB.POSTGRES_DBNAME)
        conf.setdefault("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        conf.setdefault("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        conf.setdefault("offset.storage.file.filename", offset_file_path.as_posix())
        conf.setdefault("poll.interval.ms", "10000")
        conf.setdefault("converter.schemas.enable", "false")
        conf.setdefault("offset.flush.interval.ms", "1000")
        conf.setdefault("topic.prefix", "testc")
        conf.setdefault("schema.whitelist", "inventory")
        conf.setdefault("database.whitelist", "inventory")
        conf.setdefault("table.whitelist", "inventory.products")
        conf.setdefault("replica.identity.autoset.values", "inventory.*:FULL")

        if unwrap_messages:
            conf.setdefault("transforms", "unwrap")
            conf.setdefault("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
            conf.setdefault("transforms.unwrap.add.fields", "op,table,source.ts_ms,sourcedb,ts_ms")
            conf.setdefault("transforms.unwrap.delete.handling.mode", "rewrite")

        return conf

    def debezium_engine_props(self, unwrap_messages=True):
        props = Properties()
        conf = self.debezium_engine_props_dict(unwrap_messages=unwrap_messages)
        for k, v in conf.items():
            props.setProperty(k, v)
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

    def execute_on_source_db(self, sql: str):
        self.SOURCEPGDB.execute_sql(sql=sql)
