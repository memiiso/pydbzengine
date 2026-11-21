<div class="dbz-hero" markdown>

<div class="dbz-hero__brand" markdown>
![Debezium](assets/debezium-icon.svg){ .dbz-hero__icon }

# pydbzengine { .dbz-hero__title }
</div>

<p class="dbz-hero__subtitle">
  A Pythonic interface for the
  <a href="https://debezium.io/documentation/reference/stable/development/engine.html">Debezium Embedded Engine</a>
  to stream database Change Data Capture (CDC) events directly in Python — without running Kafka Connect.
</p>

<a class="dbz-powered-by" href="https://debezium.io" target="_blank" rel="noopener">
  <span>Powered by</span>
  ![Debezium](assets/debezium-logo-dark.svg#only-light)
  ![Debezium](assets/debezium-logo-white.svg#only-dark)
</a>

<div class="dbz-hero__actions" markdown>
[Get Started](installation.md){ .md-button .md-button--primary }
[Schema Subsystem](schema/index.md){ .md-button }
[GitHub](https://github.com/memiiso/pydbzengine){ .md-button }
</div>

</div>

## Features

<div class="grid cards" markdown>

-   **Schema-Aware Arrow Consumer**

    ---

    Compile Debezium schemas into Apache Arrow (`pyarrow.Schema` and `pyarrow.Table`) with automatic schema tracking and `SchemaDiff` calculation.

    [Schema Subsystem →](schema/index.md)

-   **Multi-Format Embedded Engine**

    ---

    Configure and run the embedded Debezium Engine using Python dicts or `DebeziumConfig` in either `"json"` or Kafka `"connect"` (`SourceRecord`) wire formats.

    [Configuration Guide →](configuration.md)

-   **Apache Iceberg Handler**

    ---

    Stream CDC events directly into Apache Iceberg tables with automatic schema management (`IcebergChangeHandlerV2`) or raw JSON appends.

    [Iceberg Handler →](handlers/iceberg.md)

-   **dlt Integration**

    ---

    Load CDC events into DuckDB, BigQuery, Snowflake, and any other [dlt](https://dlthub.com/) destination using `DltChangeHandler`.

    [dlt Handler →](handlers/dlt.md)

-   **Apache Airflow & Utilities**

    ---

    Run Debezium engines inside Airflow DAGs via `DebeziumEngineOperator`, or execute bounded snapshot/timeout runs with `Utils`.

    [Airflow Operator →](airflow.md) · [Helpers →](helpers.md)

-   **Bundled Debezium Connectors**

    ---

    Ships with standard Debezium connectors for PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, MongoDB, and Db2 out of the box via [JPype](https://jpype.readthedocs.io/en/stable/).

    [How it Works →](how-it-works.md)

</div>

## Quick Start

=== "Schema-Aware (Arrow)"

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
                "Schema evolved on %s (compatible=%s)",
                new_schema.destination,
                diff.is_compatible,
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

    engine = DebeziumEngine(properties=props, handler=ArrowConsumer())
    engine.run()
    ```

=== "Raw Change Handler"

    ```python
    from pydbzengine import BasePythonChangeHandler, ChangeEvent, DebeziumEngine


    class LogChangeHandler(BasePythonChangeHandler):
        def handle_batch(self, records: list[ChangeEvent]) -> int:
            for record in records:
                self.logger.info(
                    "Event on %s: key=%s, value=%s",
                    record.destination(),
                    record.key(),
                    record.value(),
                )
            return len(records)


    if __name__ == "__main__":
        props = {
            "name": "engine",
            # Add Debezium connector properties (connector.class, database.*, offset.storage, ...)
        }

        # Runs in JSON format by default; pass format="connect" for Connect SourceRecords.
        engine = DebeziumEngine(properties=props, handler=LogChangeHandler())
        engine.run()
    ```
