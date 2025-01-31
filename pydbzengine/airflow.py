import sys
try:
    from airflow.models import BaseOperator
except ImportError:
    print("Error: airflow is required for this functionality.", file=sys.stderr)  # Print to stderr
    print("Please install it using 'pip install apache-airflow' (or the appropriate command for your Airflow installation).", file=sys.stderr)
    raise

from pydbzengine import DebeziumJsonEngine


class DebeziumEngineOperator(BaseOperator):

    def __init__(self, engine: DebeziumJsonEngine, **kwargs) -> None:
        super().__init__(**kwargs)
        self.engine = engine
        self.kill_called = False

    def execute(self, context):
        self.log.info(f"Starting Debezium engine")
        self.engine.run()

    def on_kill(self) -> None:
        self.kill_called = True
        self.engine.interrupt()