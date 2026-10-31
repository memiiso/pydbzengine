# Custom Handlers

`pydbzengine` provides two base classes for implementing custom consumers:

1. **[`SchemaAwareChangeHandler`](../schema/consumer.md)**: Converts raw JSON or Connect events into typed Python dictionaries and `pyarrow.Table` batches, tracks active schemas per destination, and dispatches `on_table_created`, `on_schema_evolved`, and `handle_table_batch`. See [Schema-Aware Consumer & Examples](../schema/consumer.md) for PyArrow and Snowflake implementations.
2. **`BasePythonChangeHandler`**: Low-level callback class that receives the raw `list[ChangeEvent]` batch directly from the JPype bridge without schema compilation or type conversion.

## Implementing `BasePythonChangeHandler`

Subclass `BasePythonChangeHandler` and implement `handle_batch` (or `handleJsonBatch`):

```python
from pydbzengine import BasePythonChangeHandler, ChangeEvent, DebeziumEngine


class CustomLogHandler(BasePythonChangeHandler):
    def handle_batch(self, records: list[ChangeEvent]) -> int:
        self.log.info("Received batch of %d records", len(records))
        for record in records:
            dest = record.destination()
            key = record.key()
            value = record.value()
            self.log.info("Processing event for %s (key=%s)", dest, key)
        return len(records)


if __name__ == "__main__":
    engine = DebeziumEngine(properties=dbz_properties, handler=CustomLogHandler())
    engine.run()
```

## `ChangeEvent` Protocol

Each `ChangeEvent` object exposes four methods mirroring the underlying Debezium `ConnectRecord` interface:

*   **`destination() -> str`**: Destination topic or table name (e.g., `"inventory.customers"`).
*   **`key() -> Any`**: Serialized JSON string (in `"json"` mode) or Java `SourceRecord` key (in `"connect"` mode), or `None` for keyless tables.
*   **`value() -> Any`**: Serialized JSON string (in `"json"` mode) or Java `SourceRecord` (in `"connect"` mode), or `None` for delete tombstones.
*   **`partition() -> int`**: Partition number for the record.
