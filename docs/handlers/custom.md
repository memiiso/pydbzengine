# Custom Handlers

Extend `BasePythonChangeHandler` to process raw `ChangeEvent` batches directly (including schemaless JSON payloads).

!!! tip "Need typed Arrow tables or schema evolution tracking?"
    If you consume schema-enabled events and want compiled `pyarrow.Schema` / `pyarrow.Table` batches with `SchemaDiff` tracking, subclass [`SchemaAwareChangeHandler`](../schema/consumer.md) instead.

## Creating a Handler

Implement `handle_batch(self, records: list[ChangeEvent])` (legacy `handleJsonBatch` is still supported for backward compatibility):

```python
from pydbzengine import BasePythonChangeHandler, ChangeEvent, DebeziumEngine


class MyCustomHandler(BasePythonChangeHandler):
    def handle_batch(self, records: list[ChangeEvent]) -> int:
        for dest, dest_records in self.group_by_destination(records).items():
            self.logger.info("Processing %d event(s) for %s", len(dest_records), dest)
            for record in dest_records:
                key = record.key()
                value = record.value()
                # Process event (e.g., forward to queue, API, or database)
        return len(records)


if __name__ == "__main__":
    engine = DebeziumEngine(properties=dbz_properties, handler=MyCustomHandler())
    engine.run()
```

`BasePythonChangeHandler` also provides:

*   **`configure(self, config: DebeziumConfig) -> None`**: Lifecycle hook called automatically by `DebeziumEngine` at initialization.
*   **`group_by_destination(self, records: Sequence[ChangeEvent]) -> dict[str, list[ChangeEvent]]`**: Groups events by destination while preserving arrival order.
*   **`self.logger`**: Hierarchical Python `logging.Logger` instance (from `LoggingMixin`).

## The `ChangeEvent` Object

`ChangeEvent` mirrors Debezium's `ChangeEvent` / Kafka `ConnectRecord` interface:

*   **`destination() -> str`**: Destination topic or table name (e.g., `inventory.customers`).
*   **`key() -> Any`**: Record key — JSON string in `"json"` mode, Java Connect object in `"connect"` mode, or `None` for keyless tables.
*   **`value() -> Any`**: Record payload — JSON string in `"json"` mode, Java Connect `SourceRecord` in `"connect"` mode, or `None` for delete tombstones.
*   **`partition() -> int`**: Source partition number.
