from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydbzengine.logger import LoggingMixin

if TYPE_CHECKING:
    from types import TracebackType

    from pydbzengine.engine._jvm import PythonChangeConsumer
    from pydbzengine.engine.base import BasePythonChangeHandler


class DebeziumEngine(LoggingMixin):
    """
    Main class to manage the Debezium embedded engine for JSON and Connect formats.
    """

    def __init__(
        self,
        properties: dict[str, Any] | Any,
        handler: BasePythonChangeHandler,
        format: str | Any = "json",
        jvm_opts: list[str] | None = None,
        extra_classpaths: list[str] | None = None,
        jvm_path: str | None = None,
    ) -> None:
        """Initializes the Debezium embedded engine."""
        if properties is None:
            raise ValueError("Please provide debezium config properties!")
        if handler is None:
            raise ValueError(
                "Please provide handler class, see example class `pydbzengine.BasePythonChangeHandler`!"
            )

        self.properties = properties
        self.handler = handler
        self.format = format
        self.jvm_opts = jvm_opts
        self.extra_classpaths = extra_classpaths
        self.jvm_path = jvm_path
        self._engine: Any = None

    @cached_property
    def consumer(self) -> PythonChangeConsumer:
        self._ensure_jvm_started()
        from pydbzengine.engine._jvm import PythonChangeConsumer

        return PythonChangeConsumer()

    def _resolve_format(self) -> Any:
        self._ensure_jvm_started()
        from pydbzengine.engine._jvm import EngineFormat

        if isinstance(self.format, str):
            fmt_str = self.format.strip().lower()
            if fmt_str == "connect":
                return EngineFormat.CONNECT
            elif fmt_str == "json":
                return EngineFormat.JSON
            raise ValueError(
                f"Unsupported format: '{self.format}'. Must be 'json', 'connect', or an EngineFormat instance."
            )
        return self.format

    def _ensure_jvm_started(self) -> None:
        from pydbzengine.engine.jvm import ensure_jvm_started

        ensure_jvm_started(
            jvm_opts=self.jvm_opts,
            extra_classpaths=self.extra_classpaths,
            jvm_path=self.jvm_path,
        )

    def _build_engine(self) -> Any:
        self._ensure_jvm_started()
        from pydbzengine.engine._jvm import DebeziumEngine as JDebeziumEngine
        from pydbzengine.engine._jvm import Properties

        engine_format = self._resolve_format()

        java_props = Properties()
        if isinstance(self.properties, dict):
            for key, value in self.properties.items():
                java_props.setProperty(str(key), str(value))
        else:
            java_props = self.properties

        return (
            JDebeziumEngine.create(engine_format)
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
        self.consumer.set_change_handler(self.handler)
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

    def __enter__(self) -> DebeziumEngine:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


class DebeziumJsonEngine(DebeziumEngine):
    """
    Legacy Debezium JSON embedded engine.
    Maintained for backward compatibility, pre-configured with format='json'.
    """

    def __init__(
        self,
        properties: dict[str, Any] | Any,
        handler: BasePythonChangeHandler,
        jvm_opts: list[str] | None = None,
        extra_classpaths: list[str] | None = None,
        jvm_path: str | None = None,
    ) -> None:
        super().__init__(
            properties=properties,
            handler=handler,
            format="json",
            jvm_opts=jvm_opts,
            extra_classpaths=extra_classpaths,
            jvm_path=jvm_path,
        )
