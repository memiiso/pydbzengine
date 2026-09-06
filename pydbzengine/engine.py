from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydbzengine.logger import LoggingMixin

if TYPE_CHECKING:
    from types import TracebackType

    from pydbzengine._jvm import PythonChangeConsumer
    from pydbzengine.base import BasePythonChangeHandler


class DebeziumJsonEngine(LoggingMixin):
    """
    Main class to manage the Debezium embedded engine (legacy JSON format engine).
    """

    def __init__(
        self,
        properties: dict[str, Any] | Any,
        handler: BasePythonChangeHandler,
    ) -> None:
        """Initializes the legacy Debezium JSON embedded engine."""
        self.properties = properties

        if self.properties is None:
            raise ValueError("Please provide debezium config properties!")
        if handler is None:
            raise ValueError(
                "Please provide handler class, see example class `pydbzengine.BasePythonChangeHandler`!"
            )

        self._handler = handler
        self._engine: Any = None

    @cached_property
    def consumer(self) -> PythonChangeConsumer:
        from pydbzengine._jvm import PythonChangeConsumer

        return PythonChangeConsumer()

    def _build_engine(self) -> Any:
        from pydbzengine._jvm import DebeziumEngine as JDebeziumEngine
        from pydbzengine._jvm import EngineFormat, Properties

        java_props = Properties()
        if isinstance(self.properties, dict):
            for key, value in self.properties.items():
                java_props.setProperty(str(key), str(value))
        else:
            java_props = self.properties

        return (
            JDebeziumEngine.create(EngineFormat.JSON)
            .using(java_props)
            .notifying(self.consumer)
            .build()
        )

    @property
    def engine(self) -> Any:
        if self._engine is None:
            self._engine = self._build_engine()
        return self._engine

    def run(self) -> None:
        """Starts the Debezium embedded engine."""
        self.consumer.set_change_handler(self._handler)
        self.consumer.clear_error()
        self.engine.run()
        self.consumer.raise_if_failed()

    def close(self) -> None:
        """Closes the Debezium embedded engine."""
        if self._engine is not None:
            try:
                self._engine.close()
            except Exception as e:
                self.logger.warning("Error during Debezium engine shutdown: %s", e)
            finally:
                self._engine = None

    def interrupt(self) -> None:
        """Interrupts the Debezium embedded engine."""
        self.close()

    def __enter__(self) -> DebeziumJsonEngine:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
