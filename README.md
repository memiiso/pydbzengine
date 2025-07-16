[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![PyPI version](https://badge.fury.io/py/pydbzengine.svg)](https://badge.fury.io/py/pydbzengine)
[![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/memiiso/pydbzengine/graphs/contributors)
[![Create Pypi Release](https://github.com/memiiso/pydbzengine/actions/workflows/release.yml/badge.svg)](https://github.com/memiiso/pydbzengine/actions/workflows/release.yml)

# pydbzengine

A Pythonic interface for the [Debezium Engine](https://debezium.io/documentation/reference/stable/development/engine.html), allowing you to consume database Change Data Capture (CDC) events directly in your Python applications.

## Features

*   **Pure Python Interface**: Interact with the powerful Debezium Engine using simple Python classes and methods.
*   **Pluggable Event Handlers**: Easily create custom handlers to process CDC events according to your specific needs.
*   **Built-in Iceberg Handler**: Stream change events directly into Apache Iceberg tables with zero boilerplate.
*   **Seamless Integration**: Designed to work with popular Python data tools like [dlt (data load tool)](https://dlthub.com/).
*   **All Debezium Connectors**: Supports all standard Debezium connectors (PostgreSQL, MySQL, SQL Server, Oracle, etc.).

## How it Works

This library acts as a bridge between the Python world and the Java-based Debezium Engine. It uses [Pyjnius](https://pyjnius.readthedocs.io/en/latest/) to manage the JVM and interact with Debezium's Java classes, exposing a clean, Pythonic API so you can focus on your data logic without writing Java code.

## Installation

### Prerequisites
You must have a **Java Development Kit (JDK) version 11 or newer** installed and available in your system's `PATH`.

### From PyPI

```shell
# For core functionality
pip install pydbzengine

# To include dependencies for the Apache Iceberg handler
pip install 'pydbzengine[iceberg]'

# To include dependencies for the dlt handler example
pip install 'pydbzengine[dlt]'
```

## How to Use

### Consume events With custom Python consumer

1. First install the packages, `pip install pydbzengine[dev]`
3. Second extend the BasePythonChangeHandler and implement your python consuming logic. see the example below

```python
from typing import List
from pydbzengine import ChangeEvent, BasePythonChangeHandler
from pydbzengine import Properties, DebeziumJsonEngine


class PrintChangeHandler(BasePythonChangeHandler):
    """
    A custom change event handler class.

    This class processes batches of Debezium change events received from the engine.
    The `handleJsonBatch` method is where you implement your logic for consuming
    and processing these events.  Currently, it prints basic information about
    each event to the console.
    """

    def handleJsonBatch(self, records: List[ChangeEvent]):
        """
        Handles a batch of Debezium change events.

        This method is called by the Debezium engine with a list of ChangeEvent objects.
        Change this method to implement your desired processing logic.  For example,
        you might parse the event data, transform it, and load it into a database or
        other destination.

        Args:
            records: A list of ChangeEvent objects representing the changes captured by Debezium.
        """
        print(f"Received {len(records)} records")
        for record in records:
            print(f"destination: {record.destination()}")
            print(f"key: {record.key()}")
            print(f"value: {record.value()}")
        print("--------------------------------------")


if __name__ == '__main__':
    props = Properties()
    props.setProperty("name", "engine")
    props.setProperty("snapshot.mode", "initial_only")
    # Add further Debezium connector configuration properties here.  For example:
    # props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector")
    # props.setProperty("database.hostname", "your_database_host")
    # props.setProperty("database.port", "3306")

    # Create a DebeziumJsonEngine instance, passing the configuration properties and the custom change event handler.
    engine = DebeziumJsonEngine(properties=props, handler=PrintChangeHandler())

    # Start the Debezium engine to begin consuming and processing change events.
    engine.run()

```
### Consume events to Apache Iceberg

```python
from pyiceberg.catalog import load_catalog
from pydbzengine import DebeziumJsonEngine
from pydbzengine.handlers.iceberg import IcebergChangeHandler
from pydbzengine import Properties

conf = {
    "uri": "http://localhost:8181",
    # "s3.path-style.access": "true",
    "warehouse": "warehouse",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
}
catalog = load_catalog(name="rest", **conf)
handler = IcebergChangeHandler(catalog=catalog, destination_namespace=("iceberg", "debezium_cdc_data",))

dbz_props = Properties()
dbz_props.setProperty("name", "engine")
dbz_props.setProperty("snapshot.mode", "always")
# ....
# Add further Debezium connector configuration properties here.  For example:
# dbz_props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector")
engine = DebeziumJsonEngine(properties=dbz_props, handler=handler)
engine.run()
```


### Consume events with dlt 
For the full code please see [dlt_consuming.py](pydbzengine/examples/dlt_consuming.py)

```python
from pydbzengine import DebeziumJsonEngine
from pydbzengine.helper import Utils
from pydbzengine.handlers.dlt import DltChangeHandler
from pydbzengine import Properties
import dlt

# Create a dlt pipeline and set destination. in this case DuckDb.
dlt_pipeline = dlt.pipeline(
    pipeline_name="dbz_cdc_events_example",
    destination="duckdb",
    dataset_name="dbz_data"
)

handler = DltChangeHandler(dlt_pipeline=dlt_pipeline)
dbz_props = Properties()
dbz_props.setProperty("name", "engine")
dbz_props.setProperty("snapshot.mode", "always")
# ....
engine = DebeziumJsonEngine(properties=dbz_props, handler=handler)

# Run the Debezium engine asynchronously with a timeout.
# This runs for a limited time and then terminates automatically.
Utils.run_engine_async(engine=engine, timeout_sec=60)
```

### Contributors

<a href="https://github.com/memiiso/pydbzengine/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/pydbzengine" />
</a>
