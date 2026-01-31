# Apache Iceberg Handler

The primary Iceberg handler is `IcebergChangeHandlerV2`. It is designed to stream CDC events directly into Apache Iceberg tables, automatically inferring schemas and managing table structures.

## IcebergChangeHandlerV2 (Recommended)

This is the most advanced handler, which automatically infers the schema from the Debezium events and creates well-structured Iceberg tables.

- **Automatic Schema Inference**: Inspects the first batch of records and infers the schema using PyArrow, preserving native data types (e.g., `LongType`, `TimestampType`).
- **Use Case**: Ideal for scenarios where you want the pipeline to automatically create tables with native data types that mirror the source.
- **Robustness**: If a field's type cannot be inferred (e.g., all `null`), it safely falls back to `StringType`.
- **Metadata**: Adds `_consumed_at`, `_dbz_event_key`, and `_dbz_event_key_hash` columns for traceability.

## Example Usage

```python
from pyiceberg.catalog import load_catalog
from pydbzengine.handlers.iceberg import IcebergChangeHandlerV2

conf = {
    "uri": "http://localhost:8181",
    "warehouse": "warehouse",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
}
catalog = load_catalog(name="rest", **conf)
handler = IcebergChangeHandlerV2(
    catalog=catalog, 
    destination_namespace=("iceberg", "debezium_cdc_data")
)
```

## IcebergChangeHandler (V1)

A simpler handler that appends change data to a source-equivalent Iceberg table using a fixed schema.

- **Structure**: The `before` and `after` payloads are stored as complete JSON strings.
- **Use Case**: Best for creating a "bronze" layer where you want to capture the raw Debezium event exactly as it comes, absorbing all source system schema changes automatically.
