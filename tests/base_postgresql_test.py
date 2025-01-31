import os
import unittest
from pathlib import Path

from DbPostgresql import DbPostgresql
from pydbzengine import Properties


class BasePostgresqlTest(unittest.TestCase):
    CURRENT_DIR = Path(__file__).parent
    OFFSET_FILE = CURRENT_DIR.joinpath('postgresql-offsets.dat')
    SOURCEDB = DbPostgresql()

    def debezium_engine_props(self):
        props = Properties()
        props.setProperty("name", "engine")
        props.setProperty("snapshot.mode", "always")
        props.setProperty("database.hostname", self.SOURCEDB.CONTAINER.get_container_host_ip())
        props.setProperty("database.port",
                          self.SOURCEDB.CONTAINER.get_exposed_port(self.SOURCEDB.POSTGRES_PORT_DEFAULT))
        props.setProperty("database.user", self.SOURCEDB.POSTGRES_USER)
        props.setProperty("database.password", self.SOURCEDB.POSTGRES_PASSWORD)
        props.setProperty("database.dbname", self.SOURCEDB.POSTGRES_DBNAME)
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        props.setProperty("offset.storage.file.filename", self.OFFSET_FILE.as_posix())
        props.setProperty("poll.interval.ms", "10000")
        props.setProperty("converter.schemas.enable", "false")
        props.setProperty("offset.flush.interval.ms", "1000")
        props.setProperty("topic.prefix", "testc")
        props.setProperty("schema.whitelist", "inventory")
        props.setProperty("database.whitelist", "inventory")
        props.setProperty("table.whitelist", "inventory.*")
        props.setProperty("replica.identity.autoset.values", "inventory.*:FULL")
        # // debezium unwrap message
        props.setProperty("transforms", "unwrap")
        props.setProperty("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
        props.setProperty("transforms.unwrap.add.fields", "op,table,source.ts_ms,sourcedb,ts_ms")
        props.setProperty("transforms.unwrap.delete.handling.mode", "rewrite")
        # props.setProperty("debezium.transforms.unwrap.drop.tombstones", "true")
        return props

    def setUp(self):
        print("setUp")
        if self.OFFSET_FILE.exists():
            os.remove(self.OFFSET_FILE)
        self.SOURCEDB.start()

    def tearDown(self):
        self.tearDownClass()

    @classmethod
    def tearDownClass(cls):
        print("tearDown")
        if cls.OFFSET_FILE.exists():
            os.remove(cls.OFFSET_FILE)
        try:
            cls.SOURCEDB.stop()
        except:
            pass
