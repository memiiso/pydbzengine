[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
[![Create Pypi Release](https://github.com/memiiso/pydbzengine/actions/workflows/release.yml/badge.svg)](https://github.com/memiiso/pydbzengine/actions/workflows/release.yml)
# pydbzengine

A Python module to use [debezium](https://debezium.io/) in python. Consume Database CDC events using python.

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

A python Example:

```python
from typing import List
from pydbzengine import ChangeEvent, RecordCommitter, BasePythonChangeHandler
from pydbzengine import Properties, DebeziumJsonEngine


class TestChangeHandler(BasePythonChangeHandler):
    """
    An example implementation of a handler class, where we need to process the data received from java.
    """

    def handleJsonBatch(self, records: List[ChangeEvent], committer: RecordCommitter):
        print(f"Received {len(records)} records")
        print(f"Record 1 table: {records[0].destination()}")
        print(f"Record 1 key: {records[0].key()}")
        print(f"Record 1 value: {records[0].value()}")
        print("--------------------------------------")
        # @TODO your code goes here
        # @TODO process the data, for-example read it into pandas and save to destination etc.
        for r in records:
            committer.markProcessed(r)
        committer.markBatchFinished()


if __name__ == '__main__':
    props = Properties()
    props.setProperty("name", "engine")
    props.setProperty("snapshot.mode", "initial_only")
    # ..... add further Debezium config properties

    # pass teh config and your handler class to the DebeziumJsonEngine
    engine = DebeziumJsonEngine(properties=props, handler=TestChangeHandler())
    engine.run()
```

Above code outputs logs like below

```asciidoc
2025-01-28 17:59:11,375 [INFO] [main] org.apache.kafka.connect.json.JsonConverterConfig (AbstractConfig.java:371) - JsonConverterConfig values:
converter.type = key decimal.format = BASE64 replace.null.with.default = true schemas.cache.size = 1000 schemas.enable = false

2025-01-28 17:59:11,378 [INFO] [main] org.apache.kafka.connect.json.JsonConverterConfig (AbstractConfig.java:371) - JsonConverterConfig values:
converter.type = value decimal.format = BASE64 replace.null.with.default = true schemas.cache.size = 1000 schemas.enable = false

......further debezium logs.........

2025-01-28 17:59:11,909 [INFO] [pool-4-thread-1] io.debezium.relational.RelationalSnapshotChangeEventSource (RelationalSnapshotChangeEventSource.java:660) - Finished exporting 9 records for table 'inventory.products' (4 of 5 tables); total duration '00:00:00.003' 2025-01-28 17:59:11,909 [INFO] [pool-4-thread-1] io.debezium.relational.RelationalSnapshotChangeEventSource (RelationalSnapshotChangeEventSource.java:614) - Exporting data from table 'inventory.products_on_hand' (5 of 5 tables) Received 2 records Record 1 table: testc.inventory.orders Record 1 key: {"id":10004} Record 1 value: {"id":10004,"order_date":16852,"purchaser":1003,"quantity":1,"product_id":107,"__deleted":"false","__op":"r","__table":"orders","__source_ts_ms":1738083551906,"__ts_ms":1738083551905}

--------------------------------------
Received 2 records
Record 1 table: testc.inventory.products
Record 1 key: {"id":102}
Record 1 value: {"id":102,"name":"car battery","description":"12V car battery","weight":8.1,"__deleted":"false","__op":"r","__table":"products","__source_ts_ms":1738083551906,"__ts_ms":1738083551909}
--------------------------------------

......further debezium logs.........
```

### Contributors

<a href="https://github.com/memiiso/pydbzengine/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/pydbzengine" />
</a>

