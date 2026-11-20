# pydbzengine

A Python interface for the [Debezium Embedded Engine](https://debezium.io/documentation/reference/stable/development/engine.html) to consume database Change Data Capture (CDC) events in Python.

## Features

*   **Python Interface**: Configure and run the embedded Debezium Engine using Python dicts and handler classes.
*   **Multi-Format Support**: Consume events in either `"json"` or Kafka `"connect"` (`SourceRecord`) wire formats.
*   **Schema-Aware Consumer (`pydbzengine.schema`)**: Compile Debezium schemas into Apache Arrow (`pyarrow.Schema` and `pyarrow.Table`) with automatic schema tracking and `SchemaDiff` calculation (see [Schema Subsystem](schema/index.md)).
*   **Apache Iceberg Handler**: Stream CDC events into Apache Iceberg tables (see [Iceberg Handler](handlers/iceberg.md)).
*   **dlt Integration**: Load CDC events into DuckDB, BigQuery, Snowflake, and other [dlt](https://dlthub.com/) destinations (see [dlt Handler](handlers/dlt.md)).
*   **Apache Airflow Operator**: Run Debezium engines inside Airflow DAGs via `DebeziumEngineOperator` (see [Airflow Operator](airflow.md)).
*   **Execution Helpers**: Run engines with timeouts or stop automatically after initial snapshots complete via `Utils` (see [Helper Utilities](helpers.md)).
*   **Debezium Connectors**: Bundled with standard Debezium connectors (PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, MongoDB, Db2).

## How it Works

`pydbzengine` uses [JPype](https://jpype.readthedocs.io/en/stable/) to start an embedded JVM and bridge Debezium's Java `DebeziumEngine` callbacks into Python batch handlers.

## Quick Start

```python
from pydbzengine import BasePythonChangeHandler, ChangeEvent, DebeziumEngine


class LogChangeHandler(BasePythonChangeHandler):
    def handle_batch(self, records: list[ChangeEvent]) -> int:
        for record in records:
            self.log.info(
                "Event on %s: key=%s, value=%s",
                record.destination(),
                record.key(),
                record.value(),
            )
        return len(records)


if __name__ == "__main__":
    props = {
        "name": "engine",
        # ... connector properties ...
    }

    # Runs in JSON format by default; pass format="connect" for Connect SourceRecords.
    engine = DebeziumEngine(properties=props, handler=LogChangeHandler())
    engine.run()
```
