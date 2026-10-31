# Handlers Overview

`pydbzengine` provides built-in handlers for common destinations and base classes for custom consumers.

## Available Handlers

- **[Schema-Aware Consumer (`SchemaAwareChangeHandler`)](../schema/consumer.md)**: Base class for typed CDC consumers. Compiles Debezium schemas into PyArrow `TableSchema` descriptors, computes `SchemaDiff` on schema evolution, and slices events into homogeneous `TableBatch` runs.
- **[Apache Iceberg](iceberg.md)**: Appends CDC events into Apache Iceberg tables (`IcebergChangeHandlerV2` and `IcebergChangeHandler`).
- **[dlt (data load tool)](dlt.md)**: Loads CDC events into DuckDB, BigQuery, Snowflake, and other `dlt` destinations via `DltChangeHandler`.
- **[Custom Handlers (`BasePythonChangeHandler`)](custom.md)**: Low-level handler interface for processing raw `ChangeEvent` batches directly.

## Running a Handler

Pass the configured handler instance to `DebeziumEngine`:

```python
from pydbzengine import DebeziumEngine
from pydbzengine.handlers.iceberg import IcebergChangeHandlerV2

props = {
    "name": "engine",
    # ... connector properties ...
}
handler = IcebergChangeHandlerV2(
    catalog=catalog,
    destination_namespace=("iceberg", "debezium_cdc_data"),
)
engine = DebeziumEngine(properties=props, handler=handler)
engine.run()
```
