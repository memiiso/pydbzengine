[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
[![Create Pypi Release](https://github.com/memiiso/pydbzengine/actions/workflows/release.yml/badge.svg)](https://github.com/memiiso/pydbzengine/actions/workflows/release.yml)
# pydbzengine

A Python module to use [Debezium Engine](https://debezium.io/) in python. Consume Database CDC events using python.

Java integration is using [Pyjnius](https://pyjnius.readthedocs.io/en/latest/), It is a Python library for accessing
Java classes

## Installation

Python+Java integration requires a Java Development Kit (JDK). Ensure a JDK is installed on environment.

install:

```shell
pip install pydbzengine
# install from github:
pip install https://github.com/memiiso/pydbzengine/archive/master.zip --upgrade --user
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

https://github.com/memiiso/pydbzengine/blob/c4a88228aa66a2dc41b3dcc192615b1357326b66/pydbzengine/examples/dlt_consuming.py#L92-L153

### Contributors

<a href="https://github.com/memiiso/pydbzengine/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/pydbzengine" />
</a>

