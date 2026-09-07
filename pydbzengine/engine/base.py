from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydbzengine.logger import LoggingMixin


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


class ChangeEvent(ABC):
    """
    Type-hinting stub for Debezium ChangeEvent / Kafka ConnectRecord.

    Note:
        Method names (key, value, destination, partition) mirror the underlying
        `org.apache.kafka.connect.connector.ConnectRecord` Java interface.
    """

    @abstractmethod
    def key(self) -> Any:
        """Returns the record key (may be string, dict, or None for keyless tables)."""
        pass

    @abstractmethod
    def value(self) -> Any:
        """Returns the record value/payload (may be string, dict, or None for tombstones)."""
        pass

    @abstractmethod
    def destination(self) -> str:
        """Returns the destination topic/table name."""
        pass

    @abstractmethod
    def partition(self) -> int:
        """Returns the partition the record belongs to."""
        pass


class BasePythonChangeHandler(ABC, LoggingMixin):
    """
    Abstract base class for change event handlers invoked by the JPype bridge.

    Note:
        `handleJsonBatch` intentionally uses camelCase to match the Java callback
        contract expected by `PythonChangeConsumer` (DebeziumEngine$ChangeConsumer).
    """

    @abstractmethod
    def handleJsonBatch(self, records: list[ChangeEvent]) -> None:
        """
        Handles a batch of change events received from the Java Debezium engine.

        Args:
            records: A list of ChangeEvent objects representing the changes.
        """
        pass
