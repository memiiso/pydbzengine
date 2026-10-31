# Apache Iceberg Handler

`pydbzengine.handlers.iceberg` provides two handlers for writing CDC events to Apache Iceberg tables.

## `IcebergChangeHandlerV2`

Infers a PyArrow schema from incoming JSON records, creates partitioned Iceberg tables if they do not exist, and appends batches:

- **Schema Inference**: Reads the JSON batch via `pyarrow.json`, replacing all-null columns with `pa.string()`.
- **Schema Evolution**: Unions newly observed fields into the existing Iceberg table schema via `table.update_schema().union_by_name(...)`.
- **Metadata & Partitioning**: Appends `_consumed_at`, `_dbz_event_key`, and `_dbz_event_key_hash` columns and partitions the table by `DayTransform()` on `_consumed_at`.

### Example Usage

```python
from pyiceberg.catalog import load_catalog

from pydbzengine import DebeziumEngine
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
    destination_namespace=("iceberg", "debezium_cdc_data"),
)

engine = DebeziumEngine(properties=dbz_properties, handler=handler)
engine.run()
```

## `IcebergChangeHandler` (V1)

Appends raw Debezium JSON envelopes to an Iceberg table with a fixed schema:

- **Fixed Schema**: Stores `op`, `ts_ms`, `ts_us`, `ts_ns`, serialized JSON strings for `source`, `before`, and `after`, plus `_dbz_event_key`, `_dbz_event_key_hash`, and `_consumed_at`.
- **Use Case**: Raw bronze-layer ingestion where `before` and `after` states are kept as unparsed JSON strings.
