# Apache Airflow Operator

`pydbzengine` provides built-in integration with Apache Airflow via the [DebeziumEngineOperator](file:///Users/simseki/IdeaProjects/pydbzengine/pydbzengine/airflow.py#L12-L25) located in [airflow.py](file:///Users/simseki/IdeaProjects/pydbzengine/pydbzengine/airflow.py).

This operator wraps the [DebeziumJsonEngine](file:///Users/simseki/IdeaProjects/pydbzengine/pydbzengine/__init__.py#L68) to run it as a task in Airflow, handling task execution and proper interruption/shutdown when a task is killed.

## Prerequisites

To use the Airflow operator, you must install the `dev` extra dependencies or have `apache-airflow` installed:

```shell
pip install "pydbzengine[dev] @ git+https://github.com/memiiso/pydbzengine.git"
```

## Operator Reference

### `DebeziumEngineOperator`

The operator constructor takes a pre-configured `DebeziumJsonEngine` instance and standard Airflow `BaseOperator` arguments.

```python
class DebeziumEngineOperator(BaseOperator):
    def __init__(self, engine: DebeziumJsonEngine, **kwargs) -> None
```

*   **`engine`**: A configured instance of [DebeziumJsonEngine](file:///Users/simseki/IdeaProjects/pydbzengine/pydbzengine/__init__.py#L68).
*   **`**kwargs`**: Pass-through arguments for `BaseOperator` (e.g. `task_id`, `dag`, `retries`).

When Airflow halts the task, the operator automatically calls `engine.interrupt()` (via `on_kill`) to gracefully shut down the Debezium engine and the JVM.

## Example DAG

Here is an example demonstrating how to run Debezium within a DAG using a custom handler that prints change records:

```python
from datetime import datetime, timedelta
from typing import List
from airflow import DAG
from pydbzengine import ChangeEvent, BasePythonChangeHandler, DebeziumJsonEngine
from pydbzengine.airflow import DebeziumEngineOperator

class MyPrintHandler(BasePythonChangeHandler):
    def handleJsonBatch(self, records: List[ChangeEvent]):
        for record in records:
            print(f"Captured event on table {record.destination()}: {record.value()}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='debezium_cdc_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define Debezium configurations
    dbz_properties = {
        "name": "airflow-cdc-engine",
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "localhost",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "postgres",
        "topic.prefix": "airflow_cdc",
        "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
        "offset.storage.file.filename": "/tmp/offsets.dat",
        "snapshot.mode": "initial_only",
        "schema.history.internal": "io.debezium.storage.file.history.FileSchemaHistory",
        "schema.history.internal.file.filename": "/tmp/schema_history.dat"
    }

    # Instantiate engine and handler
    engine = DebeziumJsonEngine(properties=dbz_properties, handler=MyPrintHandler())

    # Create the task
    run_debezium_task = DebeziumEngineOperator(
        task_id='run_debezium_cdc',
        engine=engine,
    )
```
