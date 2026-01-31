# dlt (data load tool) Handler

The `DltChangeHandler` integrates seamlessly with the [dlt](https://dlthub.com/) library, allowing you to load CDC events into any destination `dlt` supports.

## Features

- **Schema Evolution**: leverages `dlt`'s powerful schema inference and evolution capabilities.
- **Multi-Destination**: Supports DuckDB, BigQuery, Snowflake, Redshift, and more.
- **Normalization**: Automatically normalizes nested data according to your `dlt` configuration.

## Example Usage

```python
import dlt
from pydbzengine.handlers.dlt import DltChangeHandler

# Create a dlt pipeline
dlt_pipeline = dlt.pipeline(
    pipeline_name="dbz_cdc_events",
    destination="duckdb",
    dataset_name="dbz_data"
)

handler = DltChangeHandler(dlt_pipeline=dlt_pipeline)
```

For a full working example, see [dlt_consuming.py](https://github.com/memiiso/pydbzengine/blob/main/pydbzengine/examples/dlt_consuming.py).
