from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pydbzengine.logger import LoggingMixin

if TYPE_CHECKING:
    from pydbzengine.engine.config import DebeziumConfig


class EventFormat(str, Enum):
    """Wire serialization format produced by the Debezium engine."""

    JSON = "json"
    CONNECT = "connect"

    @classmethod
    def resolve_from(cls, format: str | EventFormat | Any) -> EventFormat:
        """Resolves an EventFormat from a string, enum value, or Java EngineFormat class."""
        if isinstance(format, cls):
            return format
        if isinstance(format, type) and type(format).__name__ == "_JClass":
            format = format.__name__
        if not isinstance(format, str):
            raise TypeError(
                f"Expected EventFormat or str for format, got {type(format).__name__}: {format!r}"
            )
        fmt_str = format.strip().lower()
        if fmt_str in (
            "json",
            "io.debezium.engine.format.json",
            "org.apache.kafka.connect.json.jsonconverter",
        ):
            return cls.JSON
        if fmt_str in (
            "connect",
            "io.debezium.embedded.connect",
            "org.apache.kafka.connect.storage.connectconverter",
        ):
            return cls.CONNECT
        raise ValueError(f"Unsupported format: {format!r}")


class RecordCommitter(ABC):
    """
    Type-hinting stub for Debezium's Java RecordCommitter.

    Note:
        Method names (markProcessed, markBatchFinished) intentionally use camelCase
        to strictly mirror the underlying Java interface methods of
        `io.debezium.engine.DebeziumEngine$RecordCommitter`.
    """

    @abstractmethod
    def markProcessed(self, record: Any) -> None:
        """Marks a single record as processed in the Java Debezium engine."""
        pass

    @abstractmethod
    def markBatchFinished(self) -> None:
        """Marks the entire batch as finished in the Java Debezium engine."""
        pass


@runtime_checkable
class ChangeEvent(Protocol):
    """
    Protocol for Debezium ChangeEvent / Kafka ConnectRecord.

    Note:
        Method names (key, value, destination, partition) mirror the underlying
        `org.apache.kafka.connect.connector.ConnectRecord` Java interface.
    """

    def key(self) -> Any:
        """Returns the record key (may be string, dict, or None for keyless tables)."""
        ...

    def value(self) -> Any:
        """Returns the record value/payload (may be string, dict, or None for tombstones)."""
        ...

    def destination(self) -> str:
        """Returns the destination topic/table name."""
        ...

    def partition(self) -> int:
        """Returns the partition the record belongs to."""
        ...


class BasePythonChangeHandler(ABC, LoggingMixin):
    """
    Abstract base class for change event handlers invoked by the JPype bridge.

    Note:
        `handleJsonBatch` intentionally uses camelCase to match the Java callback
        contract expected by `PythonChangeConsumer` (DebeziumEngine$ChangeConsumer).
    """

    def configure(self, config: DebeziumConfig) -> None:
        """
        Lifecycle hook invoked by DebeziumEngine to bind the engine configuration
        into the handler.
        """
        pass

    def handleJsonBatch(self, records: list[ChangeEvent]) -> None:
        """
        Java bridge callback contract. Default implementation delegates to `handle_batch`.

        Args:
            records: A list of ChangeEvent objects representing the changes.
        """
        if type(self).handle_batch is BasePythonChangeHandler.handle_batch:
            raise NotImplementedError(
                f"Handler '{type(self).__name__}' must implement either 'handle_batch' or 'handleJsonBatch'."
            )
        self.handle_batch(records)

    def handle_batch(self, records: list[ChangeEvent]) -> int | None:
        """
        Pythonic handler method for processing batches of CDC change events.
        Default implementation delegates to `handleJsonBatch`.

        Args:
            records: A list of ChangeEvent objects representing the changes.
        """
        if type(self).handleJsonBatch is BasePythonChangeHandler.handleJsonBatch:
            raise NotImplementedError(
                f"Handler '{type(self).__name__}' must implement either 'handle_batch' or 'handleJsonBatch'."
            )
        self.handleJsonBatch(records)
        return None

    def group_by_destination(
        self,
        records: Sequence[ChangeEvent],
    ) -> dict[str, list[ChangeEvent]]:
        """
        Groups a sequence of change events by their destination table name.

        Preserves arrival order of events within each destination group.
        Subclasses may override this method to customize table routing.
        """
        grouped: dict[str, list[ChangeEvent]] = {}
        for record in records:
            dest = record.destination()
            if not dest:
                raise ValueError("Record contains an empty or missing destination.")
            if dest not in grouped:
                grouped[dest] = []
            grouped[dest].append(record)
        return grouped
