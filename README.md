[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![PyPI version](https://badge.fury.io/py/pydbzengine.svg)](https://badge.fury.io/py/pydbzengine)
[![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/memiiso/pydbzengine/graphs/contributors)
[![Create Pypi Release](https://github.com/memiiso/pydbzengine/actions/workflows/release.yml/badge.svg)](https://github.com/memiiso/pydbzengine/actions/workflows/release.yml)

# pydbzengine

A Pythonic interface for the [Debezium Engine](https://debezium.io/documentation/reference/stable/development/engine.html), allowing you to consume database Change Data Capture (CDC) events directly in your Python applications.

**Full Documentation**: [https://memiiso.github.io/pydbzengine](https://memiiso.github.io/pydbzengine)

## Features

*   **Python Interface**: Configure and run the embedded Debezium Engine using Python dicts (or `DebeziumConfig`) and handler classes.
*   **Multi-Format Support**: Consume events in `"json"` or Kafka `"connect"` (`SourceRecord`) wire formats.
*   **Schema-Aware Consumer (`pydbzengine.schema`)**: Compile Debezium schemas into Apache Arrow (`pyarrow.Schema` and `pyarrow.Table`) with automatic schema tracking, `SchemaDiff`, and Arrow PyCapsule interop (see [Schema docs](https://memiiso.github.io/pydbzengine/schema/)).
*   **Pluggable Event Handlers**: Build custom handlers on `BasePythonChangeHandler` or `SchemaAwareChangeHandler`.
*   **Built-in Iceberg Handler**: Stream change events into Apache Iceberg tables.
*   **dlt Integration**: Load CDC events into DuckDB, BigQuery, Snowflake, and other [dlt](https://dlthub.com/) destinations.
*   **Apache Airflow Operator**: Run Debezium engines inside Airflow DAGs via `DebeziumEngineOperator` (see [Airflow docs](docs/airflow.md)).
*   **Execution Helpers**: Run engines with timeouts or stop automatically after initial snapshots complete via `Utils` (see [Helper docs](docs/helpers.md)).
*   **Debezium Connectors**: Bundled with standard Debezium connectors (PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, MongoDB, Db2).

## How it Works

This library bridges Python and the Java-based Debezium Engine via [JPype](https://jpype.readthedocs.io/en/stable/), managing the embedded JVM and forwarding Debezium change batches into Python handlers.

## Pre-available Data Handling Classes

`pydbzengine` includes built-in handlers and base classes. For detailed configuration and usage, see the [Handlers Documentation](https://memiiso.github.io/pydbzengine/handlers/).

### Schema-Aware Consumer (`pydbzengine[arrow]`)
*   **`SchemaAwareChangeHandler`**: Compiles schema-enabled `"json"` and `"connect"` events into PyArrow schemas and `TableBatch` objects with `on_table_created`, `on_schema_evolved` (`SchemaDiff`), and `handle_table_batch` lifecycle hooks. See [Schema Subsystem Docs](https://memiiso.github.io/pydbzengine/schema/).

### Apache Iceberg Handler
Stream CDC events into Apache Iceberg tables.
*   **`IcebergChangeHandlerV2` (Recommended)**: Infers schemas and manages table structures with native data types.
*   **`IcebergChangeHandler`**: Appends raw change data (JSON) to source-equivalent tables using a fixed schema.

### dlt (data load tool) Handler
*   **`DltChangeHandler`**: Integrates with `dlt` to load data into any supported destination (DuckDB, BigQuery, Snowflake, etc.).

### Custom Handlers
*   **`BasePythonChangeHandler`**: Extend this class to process raw `ChangeEvent` batches directly in Python (including schemaless JSON).

## Installation

### Prerequisites
You must have a **Java Development Kit (JDK) version 17 or newer** installed and available in your system's `PATH`.

### Install from PyPI

```shell
# Core engine
pip install pydbzengine

# With optional extras
pip install "pydbzengine[arrow]"    # Required for pydbzengine.schema
pip install "pydbzengine[iceberg]"
pip install "pydbzengine[dlt]"
pip install "pydbzengine[airflow]"
```

### Install from GitHub

> [!NOTE]
> If a recent release is missing from PyPI (for example, due to package size limits on bundled `.jar` artifacts) or you need unreleased changes from `main`, install directly from GitHub.

```shell
# Core engine (latest main)
pip install "git+https://github.com/memiiso/pydbzengine.git"

# With optional extras
pip install "pydbzengine[arrow] @ git+https://github.com/memiiso/pydbzengine.git"    # Required for pydbzengine.schema
pip install "pydbzengine[iceberg] @ git+https://github.com/memiiso/pydbzengine.git"
pip install "pydbzengine[dlt] @ git+https://github.com/memiiso/pydbzengine.git"
pip install "pydbzengine[airflow] @ git+https://github.com/memiiso/pydbzengine.git"
pip install "pydbzengine[dev] @ git+https://github.com/memiiso/pydbzengine.git"

# Specific release tag (e.g., 3.6.3.0):
pip install "pydbzengine @ git+https://github.com/memiiso/pydbzengine.git@3.6.3.0"
```

## How to Use

### Schema-Aware Consumer (Apache Arrow)

Install with `pip install "pydbzengine[arrow] @ git+https://github.com/memiiso/pydbzengine.git"` and subclass `SchemaAwareChangeHandler` to receive compiled `TableSchema`, `SchemaDiff`, and `TableBatch` (`pyarrow.Table`) objects:

```python
from pydbzengine import DebeziumEngine
from pydbzengine.schema import (
    SchemaAwareChangeHandler,
    SchemaDiff,
    TableBatch,
    TableSchema,
)


class ArrowConsumer(SchemaAwareChangeHandler):
    def on_table_created(self, schema: TableSchema) -> None:
        self.logger.info(
            "Table %s created: %s", schema.destination, schema.arrow_schema.names
        )

    def on_schema_evolved(
        self, new_schema: TableSchema, old_schema: TableSchema, diff: SchemaDiff
    ) -> None:
        self.logger.info(
            "Schema evolved on %s (compatible=%s, added=%s)",
            new_schema.destination,
            diff.is_compatible,
            [f.name for f in diff.added_fields],
        )

    def handle_table_batch(self, batch: TableBatch) -> None:
        arrow_table = batch.to_arrow()
        self.logger.info(
            "Received %d rows for %s", arrow_table.num_rows, batch.destination
        )


props = {
    "name": "engine",
    "snapshot.mode": "initial_only",
    # Add Debezium connector properties (connector.class, database.*, offset.storage, ...)
}

# Works identically with format="json" (default, schemas enabled) or format="connect".
engine = DebeziumEngine(properties=props, handler=ArrowConsumer())
engine.run()
```

### Raw Custom Python Consumer

Extend `BasePythonChangeHandler` and implement `handle_batch` to consume raw `ChangeEvent` batches directly:

```python
from pydbzengine import BasePythonChangeHandler, ChangeEvent, DebeziumEngine


class LogChangeHandler(BasePythonChangeHandler):
    def handle_batch(self, records: list[ChangeEvent]) -> int:
        for record in records:
            self.logger.info(
                "destination=%s key=%s value=%s",
                record.destination(),
                record.key(),
                record.value(),
            )
        return len(records)


if __name__ == "__main__":
    props = {
        "name": "engine",
        "snapshot.mode": "initial_only",
        # Add Debezium connector properties (connector.class, database.*, offset.storage, ...)
    }

    # Default format is "json"; pass format="connect" for Kafka Connect SourceRecords.
    engine = DebeziumEngine(properties=props, handler=LogChangeHandler())
    engine.run()
```
### Consume events to Apache Iceberg

```python
from pyiceberg.catalog import load_catalog
from pydbzengine import DebeziumEngine
from pydbzengine.handlers.iceberg import IcebergChangeHandlerV2

conf = {
    "uri": "http://localhost:8181",
    # "s3.path-style.access": "true",
    "warehouse": "warehouse",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
}
catalog = load_catalog(name="rest", **conf)
handler = IcebergChangeHandlerV2(
    catalog=catalog,
    destination_namespace=(
        "iceberg",
        "debezium_cdc_data",
    ),
)

dbz_props = {
    "name": "engine",
    "snapshot.mode": "always",
    # ....
    # Add further Debezium connector configuration properties here.  For example:
    # "connector.class": "io.debezium.connector.mysql.MySqlConnector",
}
engine = DebeziumEngine(properties=dbz_props, handler=handler)
engine.run()
```


### Consume events with dlt 
For the full code please see [dlt_consuming.py](pydbzengine/examples/dlt_consuming.py)

```python
from pydbzengine import DebeziumEngine
from pydbzengine.helper import Utils
from pydbzengine.handlers.dlt import DltChangeHandler
import dlt

# Create a dlt pipeline and set destination. in this case DuckDb.
dlt_pipeline = dlt.pipeline(
    pipeline_name="dbz_cdc_events_example",
    destination="duckdb",
    dataset_name="dbz_data",
)

handler = DltChangeHandler(dlt_pipeline=dlt_pipeline)
dbz_props = {
    "name": "engine",
    "snapshot.mode": "always",
    # ....
}
engine = DebeziumEngine(properties=dbz_props, handler=handler)

# Run the Debezium engine asynchronously with a timeout.
# This runs for a limited time and then terminates automatically.
Utils.run_engine_async(engine=engine, timeout_sec=60)
```

### Contributors

<a href="https://github.com/memiiso/pydbzengine/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/pydbzengine" />
</a>
