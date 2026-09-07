from __future__ import annotations

from typing import TYPE_CHECKING, Any

try:
    from airflow.models import BaseOperator
except ImportError as e:
    raise ImportError(
        "apache-airflow is required to use DebeziumEngineOperator. "
        "Please install it using 'pip install apache-airflow' or 'pip install pydbzengine[airflow]'."
    ) from e

if TYPE_CHECKING:
    from pydbzengine import DebeziumEngine


class DebeziumEngineOperator(BaseOperator):
    def __init__(
        self,
        engine: DebeziumEngine,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.engine = engine
        self.kill_called = False

    def execute(self, context: Any) -> None:
        self.log.info("Starting Debezium engine")
        self.engine.run()

    def on_kill(self) -> None:
        self.kill_called = True
        self.engine.interrupt()


__all__ = [
    "DebeziumEngineOperator",
]
