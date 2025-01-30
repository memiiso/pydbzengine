import logging
import os
from typing import List

import dlt
import duckdb

from base_postgresql_test import BasePostgresqlTest
from pydbzengine import ChangeEvent, BasePythonChangeHandler
from pydbzengine import DebeziumJsonEngine
from pydbzengine.debeziumdlt import DltChangeHandler
from pydbzengine.helper import Utils


class TestChangeHandler(BasePythonChangeHandler):
    """
    An example implementation of a handler class, where we need to process the data received from java.
    Used for testing only.
    """
    LOGGER_NAME = "TestChangeHandler"

    def handleJsonBatch(self, records: List[ChangeEvent]):
        logging.getLogger(self.LOGGER_NAME).info(f"Received {len(records)} records")
        # print(f"Received {len(records)} records")
        # for record in records:
        #     print(f"Event table: {record.destination()}")
        #     print(f"Event key: {record.key()}")
        #     print(f"Event value: {record.value()}")
        # print("--------------------------------------")


class TestBasePythonChangeHandler(BasePostgresqlTest):
    DUCK_DB = BasePostgresqlTest.CURRENT_DIR.joinpath("dbz_cdc_events.duckdb")

    def tearDown(self):
        if self.DUCK_DB.is_file() and self.DUCK_DB.exists():
            os.remove(self.DUCK_DB)
        super().tearDown()

    def test_consuming_with_handler(self):
        props = self.debezium_engine_props()
        props.setProperty("max.batch.size", "5")

        with self.assertLogs(TestChangeHandler.LOGGER_NAME, level='INFO') as cm:
            # run async then interrupt after timeout!
            engine = DebeziumJsonEngine(properties=props, handler=TestChangeHandler())
            Utils.run_engine_async(engine=engine)

        self.assertRegex(text=str(cm.output), expected_regex='.*Received.*records.*')

    def test_dlt_consuming(self):
        # get debezium engine configuration Properties
        props = self.debezium_engine_props()
        # create dlt pipeline to consume events to duckdb
        dlt_pipeline = dlt.pipeline(
            pipeline_name="dbz_cdc_events",
            destination="duckdb",
            dataset_name="dbz_data"
        )
        # create handler class, which will process generated debezium events wih dlt
        handler = DltChangeHandler(dlt_pipeline=dlt_pipeline)
        with self.assertLogs(DltChangeHandler.LOGGER_NAME, level='INFO') as cm:
            # give the config and the handler class to the DebeziumJsonEngine
            engine = DebeziumJsonEngine(properties=props, handler=handler)
            # run async then interrupt after timeout time to test the result!
            Utils.run_engine_async(engine=engine, timeout_sec=120)

        self.assertRegex(text=str(cm.output), expected_regex='.*Consumed.*records.*')
        # print the data ===========================
        con = duckdb.connect(self.DUCK_DB.as_posix())
        result = con.sql("SHOW ALL TABLES").fetchall()
        for r in result:
            database, schema, table = r[:3]
            if schema == "dbz_data":
                con.sql(f"select * from {database}.{schema}.{table}").show()
