import json
import logging
from typing import List, Dict

import dlt

from pydbzengine import ChangeEvent, BasePythonChangeHandler


@dlt.source
def debezium_source_events(records: List[ChangeEvent]):
    """
    A DLT source that processes Debezium change events.

    This function takes a list of Debezium ChangeEvent objects, groups them by table,
    and yields them as DLT resources.  This allows DLT to load the data into separate tables.

    Args:
        records: A list of Debezium ChangeEvent objects.  These represent changes captured
                 from a database (e.g., inserts, updates, deletes).

    Yields:
        dlt.Resource: A DLT resource for each table, containing the corresponding change events.
    """
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
    A change handler that uses the dlt library to process Debezium change events.

    This class receives batches of Debezium ChangeEvent objects and uses a dlt pipeline
    to load the data into a destination (e.g., a data warehouse).
    """
    LOGGER_NAME = "debeziumdlt.DltChangeHandler"

    def __init__(self, dlt_pipeline):
        """
        Initializes the DltChangeHandler.

        Args:
            dlt_pipeline: The dlt pipeline instance to use for loading data.
        """
        self.dlt_pipeline = dlt_pipeline
        self.log = logging.getLogger(self.LOGGER_NAME)

    def handleJsonBatch(self, records: List[ChangeEvent]):
        """
        Handles a batch of Debezium ChangeEvent records.

        This method is called by the Debezium connector when a batch of change events
        is received.  It runs the dlt pipeline
        to process the records.

        Args:
            records: A list of Debezium ChangeEvent objects representing database changes.
        """
        self.log.info(f"Received {len(records)} records")
        self.dlt_pipeline.run(debezium_source_events(records))
        self.log.info(f"Consumed {len(records)} records")
