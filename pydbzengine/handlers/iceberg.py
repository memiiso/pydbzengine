import json
import logging
import uuid
import datetime
from typing import List, Dict

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    StringType,
    NestedField,
    StructType,
    LongType,
    UUIDType,
    TimestampType,
)

from pydbzengine import ChangeEvent, BasePythonChangeHandler

DEBEZIUM_EVENT_SCHEMA = Schema(
    NestedField(field_id=1, name="op", field_type=StringType(), required=True, doc="The operation type: c, u, d, r"),
    NestedField(field_id=2, name="ts_ms", field_type=LongType(), required=False,
                doc="Timestamp of the event in milliseconds"),
    NestedField(field_id=3, name="ts_us", field_type=LongType(), required=False,
                doc="Timestamp of the event in microseconds"),
    NestedField(field_id=4, name="ts_ns", field_type=LongType(), required=False,
                doc="Timestamp of the event in nanoseconds"),
    NestedField(
        field_id=5,
        name="source",
        field_type=StringType(),
        required=True,
        doc="Debezium source metadata",
    ),
    NestedField(
        field_id=6,
        name="before",
        field_type=StringType(),
        required=False,
        doc="JSON string of the row state before the change",
    ),
    NestedField(
        field_id=7,
        name="after",
        field_type=StringType(),
        required=False,
        doc="JSON string of the row state after the change",
    ),
    NestedField(
        field_id=8,
        name="_dbz_event_key",
        field_type=StringType(),
        required=False,
        doc="JSON string of the Debezium event key",
    ),
    NestedField(
        field_id=9,
        name="_dbz_event_key_hash",
        field_type=UUIDType(),
        required=False,
        doc="UUID hash of the Debezium event key",
    ),
    NestedField(
        field_id=10,
        name="_consumed_at",
        field_type=TimestampType(),
        required=False,
        doc="Timestamp of when the event was consumed",
    ),
)

DEBEZIUM_EVENT_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=10, field_id=1000, name="_consumed_at_day", transform=DayTransform())
)


class IcebergChangeHandler(BasePythonChangeHandler):
    """
    A change handler that uses Apache Iceberg to process Debezium change events.
    This class receives batches of Debezium ChangeEvent objects and applies the changes
    to the corresponding Iceberg tables.
    """
    LOGGER_NAME = "pydbzengine.iceberg.IcebergChangeHandler"

    def __init__(self, catalog: "Catalog", destination_namespace: tuple):
        """
        Initializes the IcebergChangeHandler.
        """
        self.log = logging.getLogger(self.LOGGER_NAME)
        self.destination_namespace: tuple = destination_namespace
        self.catalog = catalog

    def handleJsonBatch(self, records: List[ChangeEvent]):
        """
        Handles a batch of Debezium ChangeEvent records.
        This method is called by the Debezium connector when a batch of change events
        is received. It groups the records by destination table, and then for each
        table, it applies the changes (inserts, updates, deletes) to the corresponding
        Iceberg table.
        Args:
            records: A list of Debezium ChangeEvent objects representing database changes.
        """
        self.log.info(f"Received {len(records)} records")
        table_events: Dict[str, list] = {}
        for record in records:
            destination = record.destination()
            if destination not in table_events:
                table_events[destination] = []
            table_events[destination].append(record)

        for destination, event_records in table_events.items():
            self._handle_table_changes(destination, event_records)

        self.log.info(f"Consumed {len(records)} records")

    def _handle_table_changes(self, destination: str, records: List[ChangeEvent]):
        """
        Handles changes for a specific table.
        Args:
            destination: The name of the table to apply the changes to.
            records: A list of ChangeEvent objects for the specified table.
        """
        table = self.get_table(destination)

        consumed_at = datetime.datetime.now(datetime.timezone.utc)
        arrow_data = []
        for record in records:
            # Parse the JSON payload
            payload = json.loads(record.value())

            # Extract relevant fields based on DEBEZIUM_EVENT_SCHEMA
            op = payload.get("op")
            ts_ms = payload.get("ts_ms")
            ts_us = payload.get("ts_us")
            ts_ns = payload.get("ts_ns")
            source = payload.get("source")
            before = payload.get("before")
            after = payload.get("after")
            dbz_event_key = record.key()  # its string by default
            dbz_event_key_hash = uuid.uuid5(uuid.NAMESPACE_DNS, dbz_event_key) if dbz_event_key else None

            # Create a dictionary matching the DEBEZIUM_EVENT_SCHEMA
            arrow_data.append(
                {
                    "op": op,
                    "ts_ms": ts_ms,
                    "ts_us": ts_us,
                    "ts_ns": ts_ns,
                    "source": json.dumps(source) if source is not None else None,
                    "before": json.dumps(before) if before is not None else None,
                    "after": json.dumps(after) if after is not None else None,
                    "_dbz_event_key": dbz_event_key,
                    "_dbz_event_key_hash": dbz_event_key_hash.bytes,
                    "_consumed_at": consumed_at,
                }
            )

        if arrow_data:
            pa_table = pa.Table.from_pylist(mapping=arrow_data, schema=DEBEZIUM_EVENT_SCHEMA.as_arrow())
            table.append(pa_table)
            self.log.info(f"Appended {len(arrow_data)} records to table {'.'.join(table.name())}")

    def get_table(self, destination: str) -> "Table":
        # TODO keep table object in map to avoid calling catalog
        iceberg_table: tuple = self.map_to_iceberg_table(destination)
        return self.load_table(iceberg_table=iceberg_table)

    def load_table(self, iceberg_table):
        try:
            return self.catalog.load_table(identifier=iceberg_table)
        except NoSuchTableError:
            self.log.warning(f"Iceberg table {'.'.join(iceberg_table)} not found, creating it.")
            table = self.catalog.create_table(identifier=iceberg_table,
                                              schema=DEBEZIUM_EVENT_SCHEMA,
                                              partition_spec=DEBEZIUM_EVENT_PARTITION_SPEC)
            self.log.info(f"Created iceberg table {'.'.join(iceberg_table)} with daily partitioning on _consumed_at.")
            return table

    def map_to_iceberg_table(self, destination: str) -> tuple:
        table_name = destination.replace('.', '_').replace(' ', '_').replace('-', '_')
        return self.destination_namespace + (table_name,)
