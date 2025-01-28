import logging
import os
import unittest
from pathlib import Path
from typing import List

from DbPostgresql import DbPostgresql
from TestUtil import TestUtil
from pydbzengine import ChangeEvent, RecordCommitter, BasePythonChangeHandler
from pydbzengine import Properties, DebeziumJsonEngine


class TestChangeHandler(BasePythonChangeHandler):
    """
    An example implementation of a handler class, where we need to process the data received from java.
    Used for testing only.
    """
    LOGGER_NAME = "printHandler"

    def handleJsonBatch(self, records: List[ChangeEvent], committer: RecordCommitter):
        logging.getLogger(self.LOGGER_NAME).info(f"Received {len(records)} records")
        print(f"Received {len(records)} records")
        print(f"Record 1 table: {records[0].destination()}")
        print(f"Record 1 key: {records[0].key()}")
        print(f"Record 1 value: {records[0].value()}")
        print("--------------------------------------")
        for r in records:
            committer.markProcessed(r)
        committer.markBatchFinished()


class TestDebeziumJsonEngine(unittest.TestCase):
    OFFSET_FILE = Path(__file__).parent.joinpath('postgresql-offsets.dat')
    SOURCEDB = DbPostgresql()

    def get_props(self):
        if self.OFFSET_FILE.exists():
            os.remove(self.OFFSET_FILE)
        props = Properties()
        props.setProperty("name", "engine")
        props.setProperty("snapshot.mode", "initial_only")
        props.setProperty("database.hostname", self.SOURCEDB.CONTAINER.get_container_host_ip())
        props.setProperty("database.port",
                          self.SOURCEDB.CONTAINER.get_exposed_port(self.SOURCEDB.POSTGRES_PORT_DEFAULT))
        props.setProperty("database.user", self.SOURCEDB.POSTGRES_USER)
        props.setProperty("database.password", self.SOURCEDB.POSTGRES_PASSWORD)
        props.setProperty("database.dbname", self.SOURCEDB.POSTGRES_DBNAME)
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        props.setProperty("offset.storage.file.filename", self.OFFSET_FILE.as_posix())
        props.setProperty("max.batch.size", "2")
        props.setProperty("poll.interval.ms", "10000")
        props.setProperty("converter.schemas.enable", "false")
        props.setProperty("offset.flush.interval.ms", "1000")
        props.setProperty("database.server.name", "testc")
        props.setProperty("database.server.id", "1234")
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

    def test_consuming(self):
        props = self.get_props()
        print(os.getcwd())

        with self.assertLogs(TestChangeHandler.LOGGER_NAME, level='INFO') as cm:
            # run async then interrupt after timeout!
            engine = DebeziumJsonEngine(properties=props, handler=TestChangeHandler())
            TestUtil.run_engine_async(engine=engine)

        print(str(cm.output))
        self.assertRegex(text=str(cm.output), expected_regex='.*Received.*records.*')
