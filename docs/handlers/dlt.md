# dlt (data load tool) Handler

`DltChangeHandler` loads Debezium JSON change events into any destination supported by [dlt](https://dlthub.com/) (DuckDB, BigQuery, Snowflake, Redshift, Postgres, and others).

## Behavior

- Groups non-null JSON records by `record.destination()` (replacing `.` with `_` for table naming).
- Parses each JSON payload and yields a `dlt.resource` per destination table into `dlt_pipeline.run(...)`.

## Example Usage

```python
import dlt

from pydbzengine import DebeziumEngine
from pydbzengine.handlers.dlt import DltChangeHandler

dlt_pipeline = dlt.pipeline(
    pipeline_name="dbz_cdc_events",
    destination="duckdb",
    dataset_name="dbz_data",
)

handler = DltChangeHandler(dlt_pipeline=dlt_pipeline)
engine = DebeziumEngine(properties=dbz_properties, handler=handler)
engine.run()
```

See [dlt_consuming.py](https://github.com/memiiso/pydbzengine/blob/main/pydbzengine/examples/dlt_consuming.py) for a runnable script.
