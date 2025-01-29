import json
import logging
from typing import List, Dict

import dlt

from pydbzengine import ChangeEvent, BasePythonChangeHandler


@dlt.source
def debezium_source_events(records: List[ChangeEvent]):
    # group the events per table
    table_events: Dict[str, List[str]] = {}
    for e in records:
        table = e.destination().replace(".", "_")
        val = json.loads(e.value())
        if table in table_events:
            table_events[table].append(val)
        else:
            table_events[table] = [val]

    for table_name, events in table_events.items():
        yield dlt.resource(events, name=table_name)


class DltChangeHandler(BasePythonChangeHandler):
    """
    An implementation of a handler class, which consumes data using dlt
    """
    LOGGER_NAME = "debeziumdlt.DltChangeHandler"

    def __init__(self, dlt_pipeline):
        self.dlt_pipeline = dlt_pipeline
        self.log = logging.getLogger(self.LOGGER_NAME)

    def handleJsonBatch(self, records: List[ChangeEvent]):
        self.log.info(f"Received {len(records)} records")
        self.dlt_pipeline.run(debezium_source_events(records))
        self.log.info(f"Consumed {len(records)} records")
