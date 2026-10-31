from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from enum import Enum
from typing import Any, ClassVar, Protocol, runtime_checkable

from pydbzengine.logger import LoggingMixin


class EventFormat(str, Enum):
    """Wire serialization format produced by the Debezium engine."""

    JSON = "json"
    CONNECT = "connect"

    @classmethod
    def _parse(cls, candidate: Any) -> EventFormat:
        if isinstance(candidate, cls):
            return candidate
        if not isinstance(candidate, str):
            raise TypeError(
                f"Expected EventFormat or str for format, got {type(candidate).__name__}: {candidate!r}"
            )
        fmt_str = candidate.strip().lower()
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
        raise ValueError(f"Unsupported event format: {candidate!r}")

    @classmethod
    def resolve(
        cls,
        properties: Mapping[str, Any] | None = None,
    ) -> EventFormat:
        if properties is not None and not isinstance(properties, Mapping):
            raise TypeError(
                f"Expected Mapping for properties, got {type(properties).__name__}"
            )
        props = properties if properties is not None else {}

        general_fmt: EventFormat | None = None
        for key in ("format", "engine.format", "converter.format", "converter.class"):
            if key in props:
                general_fmt = cls._parse(props[key])
                break

        value_fmt: EventFormat | None = None
        for key in (
            "value.format",
            "format.value",
            "value.converter",
            "value.converter.class",
        ):
            if key in props:
                value_fmt = cls._parse(props[key])
                break

        key_fmt: EventFormat | None = None
        for key in (
            "key.format",
            "format.key",
            "key.converter",
            "key.converter.class",
        ):
            if key in props:
                key_fmt = cls._parse(props[key])
                break

        if general_fmt is None and value_fmt is None and key_fmt is None:
            return cls.JSON

        effective_val = value_fmt if value_fmt is not None else general_fmt
        effective_key = key_fmt if key_fmt is not None else general_fmt

        if (
            effective_val is not None
            and effective_key is not None
            and effective_val is not effective_key
        ):
            raise ValueError(
                f"Having two different formats for event ({effective_val.value!r}) "
                f"and key ({effective_key.value!r}) is not supported."
            )
        if (
            general_fmt is not None
            and effective_val is not None
            and general_fmt is not effective_val
        ):
            raise ValueError(
                f"Conflicting format configuration in properties: engine format "
                f"{general_fmt.value!r} does not match converter format {effective_val.value!r}."
            )

        return effective_val if effective_val is not None else effective_key  # type: ignore[return-value]


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

    DEFAULT_HEARTBEAT_PREFIX: ClassVar[str] = "__debezium-heartbeat"

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
            raw_dest = record.destination()
            if raw_dest is None:
                raise ValueError("Record contains an empty or missing destination.")
            if not (
                isinstance(raw_dest, str)
                or type(raw_dest).__name__ == "java.lang.String"
            ):
                raise TypeError(
                    f"Expected str for record destination, got {type(raw_dest).__name__}: {raw_dest!r}"
                )
            dest = str(raw_dest).strip()
            if not dest:
                raise ValueError("Record contains an empty or missing destination.")
            grouped.setdefault(dest, []).append(record)
        return grouped
