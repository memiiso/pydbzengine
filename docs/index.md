# pydbzengine

A Pythonic interface for the [Debezium Engine](https://debezium.io/documentation/reference/stable/development/engine.html), allowing you to consume database Change Data Capture (CDC) events directly in your Python applications.

## Features

*   **Pure Python Interface**: Interact with the powerful Debezium Engine using simple Python classes and methods.
*   **Multi-Format Support**: Unified engine supporting both JSON and Kafka Connect record formats (`json` and `connect`).
*   **Pluggable Event Handlers**: Easily create custom handlers to process CDC events according to your specific needs.
*   **Built-in Iceberg Handler**: Stream change events directly into Apache Iceberg tables with zero boilerplate.
*   **Seamless Integration**: Designed to work with popular Python data tools like [dlt (data load tool)](https://dlthub.com/).
*   **Apache Airflow Operator**: Run Debezium engines directly within Airflow DAGs using the built-in `DebeziumEngineOperator` (see [Airflow docs](airflow.md)).
*   **Asynchronous & Snapshot Helpers**: Run engines with time limits or terminate them automatically once initial snapshots complete using `Utils` (see [Helper docs](helpers.md)).
*   **All Debezium Connectors**: Supports all standard Debezium connectors (PostgreSQL, MySQL, SQL Server, Oracle, etc.).

## How it Works

This library acts as a bridge between the Python world and the Java-based Debezium Engine. It uses [JPype](https://jpype.readthedocs.io/en/stable/) to manage the JVM and interact with Debezium's Java classes, exposing a clean, Pythonic API so you can focus on your data logic without writing Java code.

## Quick Start

```python
from typing import List
from pydbzengine import ChangeEvent, BasePythonChangeHandler, DebeziumEngine


class PrintChangeHandler(BasePythonChangeHandler):
    def handleJsonBatch(self, records: List[ChangeEvent]):
        for record in records:
            print(f"Record: {record.value()}")


if __name__ == "__main__":
    props = {
        "name": "engine",
        # ... set your connector properties ...
    }

    # By default, runs in JSON format. Specify format="connect" for Connect format.
    engine = DebeziumEngine(properties=props, handler=PrintChangeHandler())
    engine.run()
```
