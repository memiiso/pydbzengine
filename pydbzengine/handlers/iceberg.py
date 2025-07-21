import datetime
import io
import json
import logging
import uuid
from abc import abstractmethod
from typing import List, Dict

import pyarrow as pa
from pyarrow import json as pa_json
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    StringType,
    NestedField,
    LongType,
    UUIDType,
    TimestampType,
)

from pydbzengine import ChangeEvent, BasePythonChangeHandler


class BaseIcebergChangeHandler(BasePythonChangeHandler):
    DEBEZIUM_TABLE_PARTITION_SPEC = PartitionSpec(
        PartitionField(source_id=10, field_id=1000, name="_consumed_at_day", transform=DayTransform())
    )
    LOGGER_NAME = "pydbzengine.iceberg.IcebergChangeHandler"

    def __init__(self, catalog: "Catalog", destination_namespace: tuple, supports_variant: bool = False):
        """
        Initializes the IcebergChangeHandler.
        """
        self.log = logging.getLogger(self.LOGGER_NAME)
        self.destination_namespace: tuple = destination_namespace
        self.catalog = catalog
        self.supports_variant = supports_variant

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

    @abstractmethod
    def _handle_table_changes(self, destination: str, records: List[ChangeEvent]):
        raise NotImplementedError

    def get_table(self, destination: str) -> "Table":
        # TODO keep table object in map to avoid calling catalog
        table_identifier: tuple = self.destination_to_table_identifier(destination)
        return self.load_table(table_identifier=table_identifier)

    def load_table(self, table_identifier):
        return self.catalog.load_table(identifier=table_identifier)

    def destination_to_table_identifier(self, destination: str) -> tuple:
        table_name = destination.replace('.', '_').replace(' ', '_').replace('-', '_')
        return self.destination_namespace + (table_name,)


class IcebergChangeHandler(BaseIcebergChangeHandler):
    """
    A change handler that uses Apache Iceberg to process Debezium change events.
    This class receives batches of Debezium ChangeEvent objects and applies the changes
    to the corresponding Iceberg tables.
    """

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
            # Create a dictionary matching the schema
            avro_record = self._transform_event_to_row_dict(record=record, consumed_at=consumed_at)
            arrow_data.append(avro_record)

        if arrow_data:
            pa_table = pa.Table.from_pylist(mapping=arrow_data, schema=self._target_schema.as_arrow())
            table.append(pa_table)
            self.log.info(f"Appended {len(arrow_data)} records to table {'.'.join(table.name())}")

    def _transform_event_to_row_dict(self, record: ChangeEvent, consumed_at: datetime) -> dict:
        # Parse the JSON payload
        payload = json.loads(record.value())

        # Extract relevant fields based on schema
        op = payload.get("op")
        ts_ms = payload.get("ts_ms")
        ts_us = payload.get("ts_us")
        ts_ns = payload.get("ts_ns")
        source = payload.get("source")
        before = payload.get("before")
        after = payload.get("after")
        dbz_event_key = record.key()  # its string by default
        dbz_event_key_hash = uuid.uuid5(uuid.NAMESPACE_DNS, dbz_event_key) if dbz_event_key else None

        return {
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

    def load_table(self, table_identifier):
        try:
            return super().load_table(table_identifier=table_identifier)
        except NoSuchTableError:
            self.log.warning(f"Iceberg table {'.'.join(table_identifier)} not found, creating it.")
            table = self.catalog.create_table(identifier=table_identifier,
                                              schema=self._target_schema,
                                              partition_spec=self.DEBEZIUM_TABLE_PARTITION_SPEC)
            self.log.info(
                f"Created iceberg table {'.'.join(table_identifier)} with daily partitioning on _consumed_at.")
            return table

    @property
    def _target_schema(self) -> Schema:
        # @TODO according to self.supports_variant we can return different schemas!
        return Schema(
            NestedField(field_id=1, name="op", field_type=StringType(), required=True,
                        doc="The operation type: c, u, d, r"),
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


class IcebergChangeHandlerV2(BaseIcebergChangeHandler):
    """
    A change handler that uses Apache Iceberg to process Debezium change events.
    This class receives batches of Debezium ChangeEvent objects and applies the changes
    to the corresponding Iceberg tables.
    """

    def __init__(self, catalog: "Catalog", destination_namespace: tuple, supports_variant: bool = False,
                 event_flattening_enabled=False):
        super().__init__(catalog, destination_namespace, supports_variant)
        self.event_flattening_enabled = event_flattening_enabled

    def _handle_table_changes(self, destination: str, records: List[ChangeEvent]):
        """
        Handles changes for a specific table.
        Args:
            destination: The name of the table to apply the changes to.
            records: A list of ChangeEvent objects for the specified table.
        """

        table = self.get_table(destination)
        if table is None:
            table_identifier: tuple = self.destination_to_table_identifier(destination=destination)
            table = self._infer_and_create_table(records=records, table_identifier=table_identifier)
        #
        arrow_data = self._read_to_arrow_table(records=records, schema=table.schema())

        # Populate all metadata columns (_consumed_at, _dbz_event_key, etc.)
        enriched_arrow_data = self._enrich_arrow_table_with_metadata(
            arrow_table=arrow_data,
            records=records
        )

        self._handle_schema_changes(table=table, arrow_schema=enriched_arrow_data.schema)
        table.append(enriched_arrow_data)
        self.log.info(f"Appended {len(enriched_arrow_data)} records to table {'.'.join(table.name())}")

    def _enrich_arrow_table_with_metadata(self, arrow_table: pa.Table, records: List[ChangeEvent]) -> pa.Table:
        num_records = len(arrow_table)
        dbz_event_keys = []
        dbz_event_key_hashes = []

        for record in records:
            key = record.key()
            dbz_event_keys.append(key)
            key_hash = str(uuid.uuid5(uuid.NAMESPACE_DNS, key)) if key else None
            dbz_event_key_hashes.append(key_hash)

        # Create PyArrow arrays for each metadata column
        consumed_at_array = pa.array([datetime.datetime.now(datetime.timezone.utc)] * num_records,
                                     type=pa.timestamp('us', tz='UTC'))
        dbz_event_key_array = pa.array(dbz_event_keys, type=pa.string())
        dbz_event_key_hash_array = pa.array(dbz_event_key_hashes, type=pa.string())

        # Replace the null columns in the Arrow table with the populated arrays.
        # This uses set_column, which is efficient for replacing entire columns.
        enriched_table = arrow_table.set_column(
            arrow_table.schema.get_field_index("_consumed_at"),
            "_consumed_at",
            consumed_at_array
        )
        enriched_table = enriched_table.set_column(
            enriched_table.schema.get_field_index("_dbz_event_key"),
            "_dbz_event_key",
            dbz_event_key_array
        )
        enriched_table = enriched_table.set_column(
            enriched_table.schema.get_field_index("_dbz_event_key_hash"),
            "_dbz_event_key_hash",
            dbz_event_key_hash_array
        )

        return enriched_table

    def _read_to_arrow_table(self, records, schema=None):
        json_lines_buffer = io.BytesIO()
        for record in records:
            json_lines_buffer.write((record.value() + '\n').encode('utf-8'))
        json_lines_buffer.seek(0)

        parse_options = None
        if schema:
            # If an Iceberg schema is provided, convert it to a PyArrow schema and use it for parsing.
            parse_options = pa_json.ParseOptions(explicit_schema=schema.as_arrow(), unexpected_field_behavior="infer")
        return pa_json.read_json(json_lines_buffer, parse_options=parse_options)

    def get_table(self, destination: str) -> "Table":
        table_identifier: tuple = self.destination_to_table_identifier(destination=destination)
        return self.load_table(table_identifier=table_identifier)

    def load_table(self, table_identifier):
        try:
            return self.catalog.load_table(identifier=table_identifier)
        except NoSuchTableError:
            return None

    def _infer_and_create_table(self, records: List[ChangeEvent], table_identifier: tuple) -> Table:
        """
        Infers a schema from a batch of records, creates a new Iceberg table with that schema,
        and sets up daily partitioning on the _consumed_at field.
        """
        arrow_table = self._read_to_arrow_table(records)
        sanitized_fields = self._sanitize_schema_fields(data_schema=arrow_table.schema)

        # Add metadata fields to the list of pyarrow fields
        sanitized_fields.extend([
            pa.field("_consumed_at", pa.timestamp('us', tz='UTC')),
            pa.field("_dbz_event_key", pa.string()),
            pa.field("_dbz_event_key_hash", pa.string())  # For UUIDType
        ])

        # Create a pyarrow schema first
        sanitized_arrow_schema = pa.schema(sanitized_fields)

        # Create the table
        table = self.catalog.create_table(
            identifier=table_identifier,
            schema=sanitized_arrow_schema
        )
        # add partitioning
        with table.update_spec() as update_spec:
            update_spec.add_field(source_column_name="_consumed_at", transform=DayTransform(),
                                  partition_field_name="_consumed_at_day")

        if self.event_flattening_enabled and False:
            # @TODO fix future. https://github.com/apache/iceberg-python/issues/1728
            identifier_fields = self._get_identifier_fields(sample_event=records[0],
                                                            table_identifier=table_identifier
                                                            )
            if identifier_fields:
                with table.update_schema(allow_incompatible_changes=True) as update_schema:
                    for field in identifier_fields:
                        update_schema._set_column_requirement(path=field, required=True)
                    update_schema.set_identifier_fields(*identifier_fields)
        self.log.info(f"Created iceberg table {'.'.join(table_identifier)} with daily partitioning on _consumed_at.")
        return table

    def _sanitize_schema_fields(self, data_schema: pa.Schema) -> list:
        """
        Recursively traverses a PyArrow schema and replaces null types with a string fallback.
        This is useful when a schema is inferred from JSON where some fields are always null.
        """
        new_fields = []
        for field in data_schema:
            field_type = field.type
            if pa.types.is_null(field_type):
                # Found a null type, replace it with a string as a fallback.
                new_fields.append(field.with_type(pa.string()))
            elif pa.types.is_struct(field_type):
                # Found a struct, so we recurse on its fields to sanitize them.
                # We can treat the struct's fields as a schema for the recursive call.
                nested_schema = pa.schema(field_type)
                sanitized_nested_schema = self._sanitize_schema_fields(nested_schema)
                # Recreate the field with the new, sanitized struct type.
                new_fields.append(field.with_type(pa.struct(sanitized_nested_schema)))
            else:
                # Not a null or struct, so we keep the field as is.
                new_fields.append(field)
        return new_fields

    def _get_identifier_fields(self, sample_event: ChangeEvent, table_identifier: tuple) -> list:
        """
        Parses the Debezium event key to extract primary key field names.

        This method uses a series of guard clauses to validate the key and returns
        an empty list if any validation step fails.

        Args:
            sample_event: A sample change event to inspect for the key.
            table_identifier: The identifier of the table, used for logging.

        Returns:
            A list of key field names, or an empty list if the key cannot be determined.
        """
        key_json_str = sample_event.key()
        table_name_str = '.'.join(table_identifier)

        if not key_json_str:
            self.log.warning(f"Cannot determine identifier fields for {table_name_str}: event key is empty.")
            return []

        try:
            key_data = json.loads(key_json_str)
        except json.JSONDecodeError:
            self.log.error(f"Failed to parse Debezium event key as JSON for table {table_name_str}: {key_json_str}")
            return []

        if not isinstance(key_data, dict):
            self.log.warning(
                f"Event key for {table_name_str} is not a JSON object, cannot infer primary key. Key: {key_json_str}")
            return []

        key_field_names = list(key_data.keys())
        if not key_field_names:
            self.log.warning(f"Event key for {table_name_str} is an empty JSON object, cannot infer primary key.")
            return []

        self.log.info(f"Found potential primary key fields {key_field_names} for table {table_name_str}")
        return key_field_names

    def _handle_schema_changes(self, table: "Table", arrow_schema: "pa.Schema"):
        with table.update_schema() as update:
            update.union_by_name(new_schema=arrow_schema)
        self.log.info(f"Schema for table {'.'.join(table.name())} has been updated.")
