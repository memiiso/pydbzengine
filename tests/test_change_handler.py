import logging

from base_postgresql import BasePostgresqlTest

from pydbzengine import (
    BasePythonChangeHandler,
    ChangeEvent,
    DebeziumEngine,
)
from pydbzengine.helper import Utils


class DummyChangeHandler(BasePythonChangeHandler):
    """
    An example implementation of a handler class, where we need to process the data received from java.
    Used for testing only.
    """

    LOGGER_NAME = "DummyChangeHandler"

    def handleJsonBatch(self, records: list[ChangeEvent]):
        logging.getLogger(self.LOGGER_NAME).info(f"Received {len(records)} records")
        print(f"Received {len(records)} records")


class TestBasePythonChangeHandler(BasePostgresqlTest):
    def test_consuming_with_handler(self):
        props = self.debezium_engine_props()
        props.setProperty("database.server.name", "testc")
        props.setProperty("database.server.id", "1234")
        props.setProperty("max.batch.size", "5")

        with self.assertLogs(DummyChangeHandler.LOGGER_NAME, level="INFO") as cm:
            # run async then interrupt after timeout!
            engine = DebeziumEngine(properties=props, handler=DummyChangeHandler())
            Utils.run_engine_async(engine=engine)

        self.assertRegex(text=str(cm.output), expected_regex=".*Received.*records.*")
