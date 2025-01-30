[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
[![Create Pypi Release](https://github.com/memiiso/pydbzengine/actions/workflows/release.yml/badge.svg)](https://github.com/memiiso/pydbzengine/actions/workflows/release.yml)
# pydbzengine

A Python module to use [Debezium Engine](https://debezium.io/) in python. Consume Database CDC events using python.

Java integration is using [Pyjnius](https://pyjnius.readthedocs.io/en/latest/), It is a Python library for accessing
Java classes

## Installation

install:

```shell
pip install pydbzengine
# install from github:
pip install https://github.com/memiiso/pydbzengine/archive/master.zip --upgrade --user
```

## How to Use

First install the packages, `pip install pydbzengine[dev]`

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
#### How to consume events with dlt 
For dlt example please see [dlt_consuming.py](pydbzengine/examples/dlt_consuming.py)

https://github.com/memiiso/pydbzengine/blob/9a26d4c2ee2c5c48ee500d572288f2c27ad17a6c/pydbzengine/examples/dlt_consuming.py#L92-L160


### Contributors

<a href="https://github.com/memiiso/pydbzengine/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/pydbzengine" />
</a>

