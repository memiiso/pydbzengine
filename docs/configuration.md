# Configuration Guide

`pydbzengine` accepts a Python `dict[str, Any]` whose keys map directly to Debezium's Java configuration properties.

---

## Core Engine Properties

Every `DebeziumEngine` instance requires basic identification and offset storage properties:

| Property | Description | Example |
| :--- | :--- | :--- |
| `name` | Unique logical name of the engine instance. | `"engine"` |
| `connector.class` | Java class path of the Debezium connector. | `"io.debezium.connector.postgresql.PostgresConnector"` |
| `offset.storage` | Storage class used to persist source offsets. | `"org.apache.kafka.connect.storage.FileOffsetBackingStore"` |
| `offset.storage.file.filename` | Local file path where offsets are written. | `"/path/to/offsets.dat"` |
| `offset.flush.interval.ms` | Interval at which offsets are flushed to storage. | `"1000"` |

---

## Engine Wire Formats

`DebeziumEngine` supports two record wire formats:
- **`"json"`** (*default*): Events are serialized as JSON strings.
- **`"connect"`**: Events are passed as in-memory Java Kafka Connect `SourceRecord` objects.

```python
from pydbzengine import DebeziumEngine

# JSON mode (default)
engine = DebeziumEngine(properties=dbz_properties, handler=handler)

# Kafka Connect mode
engine = DebeziumEngine(properties=dbz_properties, handler=handler, format="connect")
```

---

## Schema History Properties

Connectors that track DDL history (**MySQL, MariaDB, SQL Server, Oracle, Db2**) require an internal schema history store:

| Property | Description | Example |
| :--- | :--- | :--- |
| `schema.history.internal` | Storage class for database schema history. | `"io.debezium.storage.file.history.FileSchemaHistory"` |
| `schema.history.internal.file.filename` | Local file path for storing schema history. | `"/path/to/schema_history.dat"` |

---

## Database Connection Properties

```python
dbz_properties = {
    "database.hostname": "localhost",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    "topic.prefix": "cdc_prod",
    "table.include.list": "inventory.customers,inventory.orders",
}
```

---

## Event Unwrapping (`ExtractNewRecordState`)

By default, Debezium wraps change events in an envelope structure containing `before`, `after`, `op`, `source`, and timestamp metadata.

- When using [`SchemaAwareChangeHandler`](schema/consumer.md), `schema.row_schema` and `batch.to_arrow(extract_rows=True)` extract the unpacked table row directly from standard Debezium envelopes without requiring an SMT.
- When using `IcebergChangeHandlerV2` or `DltChangeHandler` with flat payloads, configure Debezium's `ExtractNewRecordState` transform:

```python
dbz_properties.update(
    {
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.add.fields": "op,table,source.ts_ms,sourcedb,ts_ms",
        "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
    }
)
```

---

## External Reference

For connector-specific properties, see the [Debezium Connector Configuration Reference](https://debezium.io/documentation/reference/stable/connectors/index.html).
