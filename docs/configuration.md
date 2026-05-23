# Configuration Guide

`pydbzengine` utilizes a standard Python dictionary to configure the embedded Debezium Engine. These properties map directly to Debezium's Java configuration keys.

Below is a reference guide to the essential properties required to set up an embedded Debezium CDC pipeline.

---

## Core Engine Properties

Every `DebeziumJsonEngine` deployment requires a set of basic parameters to identify the instance and specify offset storage.

| Property | Description | Example |
| :--- | :--- | :--- |
| `name` | Unique logical name of the engine instance. | `"engine"` |
| `connector.class` | The class path of the Debezium connector. | `"io.debezium.connector.postgresql.PostgresConnector"` |
| `offset.storage` | Storage class to persist database engine offsets. | `"org.apache.kafka.connect.storage.FileOffsetBackingStore"` |
| `offset.storage.file.filename` | Local filepath to write offsets to disk. | `"/path/to/offsets.dat"` |
| `offset.flush.interval.ms` | Interval at which engine states are committed. | `"1000"` |

---

## Schema History Properties
Connectors like **MySQL, SQL Server, and Oracle** require database schema histories to track DDL structural changes over time.

| Property | Description | Example |
| :--- | :--- | :--- |
| `schema.history.internal` | Class used to store database schema definitions. | `"io.debezium.storage.file.history.FileSchemaHistory"` |
| `schema.history.internal.file.filename` | Local file path for storing the database schema history. | `"/path/to/schema_history.dat"` |

---

## Database Connection Properties
These settings define connection parameters to replicate from the source database. Note that PostgreSQL requires `plugin.name` to specify the logical decoding plugin (defaults to `pgoutput` on PostgreSQL 10+).

```python
dbz_properties = {
    # Hostname and Credentials
    "database.hostname": "localhost",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    
    # Prefix appended to all published target names/tables
    "topic.prefix": "cdc_prod",
    
    # Specific database / table filters
    "table.include.list": "inventory.customers,inventory.orders",
}
```

---

## Event Unwrapping (ExtractNewRecordState)

By default, Debezium envelopes change events in a complex structure containing `before`, `after`, `op`, `source`, and timestamp metadata. If you want the event's `value()` to return only the latest flat row state (e.g. for `IcebergChangeHandlerV2` or `DltChangeHandler`), you must enable Debezium's **`ExtractNewRecordState`** transform.

### Example Configuration

Add these lines to your properties dictionary to unwrap database updates:

```python
dbz_properties.update({
    # Register the unwrap transform
    "transforms": "unwrap",
    
    # Specify the unwrap class
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    
    # Optionally append metadata to the unwrapped event
    "transforms.unwrap.add.fields": "op,table,source.ts_ms,sourcedb,ts_ms",
    
    # How to handle deleted rows (tombstones)
    "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
})
```

*   **`delete.tombstone.handling.mode`**: Set to `"rewrite"` to ensure delete events are cleanly represented in your Python handlers instead of triggering empty tombstone exceptions.

---

## Official Documentation Reference
For the complete list of connector-specific and advanced options, please refer to the [Official Debezium Connector Configuration Guide](https://debezium.io/documentation/reference/stable/connectors/index.html).
