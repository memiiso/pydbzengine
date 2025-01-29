import logging
from typing import List

from base_postgresql_test import BasePostgresqlTest
from pydbzengine import ChangeEvent, RecordCommitter, BasePythonChangeHandler
from pydbzengine import DebeziumJsonEngine
from testing_utils import TestingUtils


class TestChangeHandler(BasePythonChangeHandler):
    """
    An example implementation of a handler class, where we need to process the data received from java.
    Used for testing only.
    """
    LOGGER_NAME = "TestChangeHandler"

    def handleJsonBatch(self, records: List[ChangeEvent], committer: RecordCommitter):
        logging.getLogger(self.LOGGER_NAME).info(f"Received {len(records)} records")
        print(f"Received {len(records)} records")
        for record in records:
            print(f"Event table: {record.destination()}")
            print(f"Event key: {record.key()}")
            print(f"Event value: {record.value()}")
        print("--------------------------------------")
        for r in records:
            committer.markProcessed(r)
        committer.markBatchFinished()


class TestBasePythonChangeHandler(BasePostgresqlTest):
    def test_consuming_with_handler(self):
        props = self.get_props()

        with self.assertLogs(TestChangeHandler.LOGGER_NAME, level='INFO') as cm:
            # run async then interrupt after timeout!
            engine = DebeziumJsonEngine(properties=props, handler=TestChangeHandler())
            TestingUtils.run_engine_async(engine=engine)

        self.assertRegex(text=str(cm.output), expected_regex='.*Received.*records.*')
