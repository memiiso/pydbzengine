import logging
from typing import List

from base_postgresql_test import BasePostgresqlTest
from pydbzengine import ChangeEvent, BasePythonChangeHandler
from pydbzengine import DebeziumJsonEngine

from pydbzengine.helper import Utils


class TestChangeHandler(BasePythonChangeHandler):
    """
    An example implementation of a handler class, where we need to process the data received from java.
    Used for testing only.
    """
    LOGGER_NAME = "TestChangeHandler"

    def handleJsonBatch(self, records: List[ChangeEvent]):
        logging.getLogger(self.LOGGER_NAME).info(f"Received {len(records)} records")
        print(f"Received {len(records)} records")
        # for record in records:
        #     print(f"Event table: {record.destination()}")
        #     print(f"Event key: {record.key()}")
        #     print(f"Event value: {record.value()}")
        # print("--------------------------------------")


class TestBasePythonChangeHandler(BasePostgresqlTest):
    def test_consuming_with_handler(self):
        props = self.debezium_engine_props()
        props.setProperty("database.server.name", "testc")
        props.setProperty("database.server.id", "1234")
        props.setProperty("max.batch.size", "5")

        with self.assertLogs(TestChangeHandler.LOGGER_NAME, level='INFO') as cm:
            # run async then interrupt after timeout!
            engine = DebeziumJsonEngine(properties=props, handler=TestChangeHandler())
            Utils.run_engine_async(engine=engine)

        self.assertRegex(text=str(cm.output), expected_regex='.*Received.*records.*')
