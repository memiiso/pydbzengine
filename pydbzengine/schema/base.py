from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, Generic, Protocol, TypeVar, runtime_checkable

from pydbzengine.logger import LoggingMixin
from pydbzengine.schema.models import CanonicalSchema


@runtime_checkable
class SupportsChangePayload(Protocol):
    """Protocol for change events exposing a value() payload method."""

    def value(self) -> Any: ...


T_Schema = TypeVar("T_Schema")
T_Table = TypeVar("T_Table")
T_Batch = TypeVar("T_Batch")
T_Record = TypeVar("T_Record", bound=SupportsChangePayload)


class BaseSchemaReader(ABC, LoggingMixin):
    """Abstract base class for extracting canonical schemas from CDC change events."""

    @abstractmethod
    def can_read(self, record: Any) -> bool:
        """Returns True if this reader can extract schema from the given record."""
        pass

    @abstractmethod
    def extract_schema(
        self,
        record: Any,
        flattening_enabled: bool = True,
        include_metadata: bool = True,
    ) -> CanonicalSchema:
        """Strictly extracts a CanonicalSchema from the record."""
        pass

    def extract_fingerprint(
        self,
        record: Any,
        flattening_enabled: bool = True,
        include_metadata: bool = True,
    ) -> str:
        """Returns the schema fingerprint for fast boundary checking."""
        return self.extract_schema(
            record,
            flattening_enabled=flattening_enabled,
            include_metadata=include_metadata,
        ).fingerprint


class BaseSchemaConverter(ABC, Generic[T_Schema]):
    """Abstract base class for converting CanonicalSchema to a target schema representation."""

    @abstractmethod
    def convert_schema(self, schema: CanonicalSchema) -> T_Schema:
        """Translates CanonicalSchema into target schema (e.g. pyiceberg Schema, pa.Schema)."""
        pass


class BaseSchemaEvolver(ABC, Generic[T_Table], LoggingMixin):
    """Abstract base class for inspecting and evolving a target table's schema."""

    @abstractmethod
    def needs_evolution(self, table: T_Table, schema: CanonicalSchema) -> bool:
        """Returns True if the incoming schema contains fields missing from the target table."""
        pass

    @abstractmethod
    def evolve_schema(self, table: T_Table, schema: CanonicalSchema) -> bool:
        """Applies schema changes to the table catalog. Returns True if schema was modified."""
        pass


class BaseTableWriter(ABC, Generic[T_Table, T_Batch, T_Record], LoggingMixin):
    """Abstract base class for preparing batches and writing data to a target table."""

    @abstractmethod
    def build_batch(self, schema: CanonicalSchema, records: list[T_Record]) -> T_Batch:
        """Transforms a chunk of change events into a typed target batch."""
        pass

    @abstractmethod
    def write_batch(self, table: T_Table, batch: T_Batch) -> int:
        """Appends/writes the batch to the target table. Returns number of rows written."""
        pass


class StreamPartitioner:
    """Utility class for slicing and partitioning CDC change event streams by schema boundaries."""

    @staticmethod
    def chunk_by_schema(
        records: list[T_Record],
        schema_reader: BaseSchemaReader,
        flattening_enabled: bool = True,
    ) -> Iterator[tuple[CanonicalSchema, list[T_Record]]]:
        """
        Slices a stream of CDC change events into contiguous chunks sharing the exact same schema.

        Guarantees strict arrival order without reordering:
        When a schema transition occurs mid-stream (e.g. ALTER TABLE), this generator yields
        the completed pre-transition chunk before starting a new chunk for the post-transition schema.
        """
        if not records:
            return

        current_schema: CanonicalSchema | None = None
        current_chunk: list[T_Record] = []

        for record in records:
            if not schema_reader.can_read(record):
                continue

            record_schema = schema_reader.extract_schema(
                record, flattening_enabled=flattening_enabled
            )

            if current_schema is None:
                current_schema = record_schema
                current_chunk.append(record)
            elif record_schema.fingerprint == current_schema.fingerprint:
                current_chunk.append(record)
            else:
                yield current_schema, current_chunk
                current_schema = record_schema
                current_chunk = [record]

        if current_chunk and current_schema is not None:
            yield current_schema, current_chunk


class BaseStreamSynchronizer(ABC, Generic[T_Table, T_Batch, T_Record], LoggingMixin):
    """Abstract base class for coordinating in-stream schema boundary synchronization."""

    def __init__(
        self,
        schema_reader: BaseSchemaReader,
        evolver: BaseSchemaEvolver[T_Table],
        writer: BaseTableWriter[T_Table, T_Batch, T_Record],
        flattening_enabled: bool = True,
    ) -> None:
        self.schema_reader = schema_reader
        self.evolver = evolver
        self.writer = writer
        self.flattening_enabled = flattening_enabled

    def split_into_chunks(
        self, records: list[T_Record]
    ) -> list[tuple[CanonicalSchema, list[T_Record]]]:
        """Splits incoming records into contiguous runs sharing the exact same schema."""
        return list(
            StreamPartitioner.chunk_by_schema(
                records,
                self.schema_reader,
                flattening_enabled=self.flattening_enabled,
            )
        )

    @abstractmethod
    def process_records(self, table: T_Table, records: list[T_Record]) -> int:
        """Processes a list of change events against a target table."""
        pass
