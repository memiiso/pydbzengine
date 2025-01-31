from airflow.models import BaseOperator

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