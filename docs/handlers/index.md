# Handlers Overview

`pydbzengine` comes with several built-in handlers to stream CDC events directly into common data destinations. You can also easily create your own by extending the `BasePythonChangeHandler`.

## Available Handlers

- **[Apache Iceberg](iceberg.md)**: Stream change events directly into Apache Iceberg tables. Supports automatic schema inference and partitioning (via `IcebergChangeHandlerV2`).
- **[dlt (data load tool)](dlt.md)**: Integrate with `dlt` to load data into DuckDB, BigQuery, Snowflake, and more.
- **[Custom Handlers](custom.md)**: Implement your own logic for any destination or processing requirement.

## Using a Handler

To use a handler, simply pass an instance of it to the `DebeziumJsonEngine`:

```python
from pydbzengine import DebeziumJsonEngine
from pydbzengine.handlers.iceberg import IcebergChangeHandlerV2

# ... setup catalog and props ...
props = {
    "name": "engine",
    # ...
}
handler = IcebergChangeHandlerV2(catalog=catalog, ...)
engine = DebeziumJsonEngine(properties=props, handler=handler)
engine.run()
```
