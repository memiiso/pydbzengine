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
- **`"json"`** (*default*): Events are serialized as JSON strings. Schemas are enabled by default (`converter.schemas.enable=true`), which is required by [`SchemaAwareChangeHandler`](schema/consumer.md). Set `converter.schemas.enable=false` only when consuming raw schemaless JSON with [`BasePythonChangeHandler`](handlers/custom.md) (or built-in handlers such as `DltChangeHandler`).
- **`"connect"`**: Events are passed as in-memory Java Kafka Connect `SourceRecord` objects (always schema-enabled).

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

- [`SchemaAwareChangeHandler`](schema/consumer.md) compiles whatever top-level struct Debezium emits: without an SMT, `schema.arrow_schema` and `batch.to_arrow()` mirror the full envelope (`before`/`after` as nested structs).
- When you want flat table columns directly (for [`SchemaAwareChangeHandler`](schema/consumer.md) warehouse sinks, `IcebergChangeHandlerV2`, or `DltChangeHandler`), configure Debezium's `ExtractNewRecordState` transform:

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
