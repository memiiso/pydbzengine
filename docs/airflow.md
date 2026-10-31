# Apache Airflow Operator

`pydbzengine.airflow` provides `DebeziumEngineOperator` to execute a `DebeziumEngine` instance inside an Apache Airflow DAG task.

## Installation

Install the `airflow` extra:

```shell
pip install "pydbzengine[airflow] @ git+https://github.com/memiiso/pydbzengine.git"
```

## Operator Reference

```python
class DebeziumEngineOperator(BaseOperator):
    def __init__(self, engine: DebeziumEngine, **kwargs: Any) -> None
```

*   **`engine`**: Configured `DebeziumEngine` instance.
*   **`**kwargs`**: Standard `BaseOperator` arguments (`task_id`, `dag`, `retries`, etc.).

When Airflow terminates or times out the task, `on_kill()` calls `engine.interrupt()` to close the underlying Debezium engine.

## Example DAG

```python
from datetime import datetime, timedelta

from airflow import DAG

from pydbzengine import BasePythonChangeHandler, ChangeEvent, DebeziumEngine
from pydbzengine.airflow import DebeziumEngineOperator


class LogChangeHandler(BasePythonChangeHandler):
    def handle_batch(self, records: list[ChangeEvent]) -> int:
        for record in records:
            self.log.info(
                "Captured event on %s: %s",
                record.destination(),
                record.value(),
            )
        return len(records)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="debezium_cdc_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
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
        "schema.history.internal.file.filename": "/tmp/schema_history.dat",
    }

    engine = DebeziumEngine(properties=dbz_properties, handler=LogChangeHandler())

    run_debezium_task = DebeziumEngineOperator(
        task_id="run_debezium_cdc",
        engine=engine,
    )
```
