from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    NamedTuple,
    TypeVar,
)

from pydbzengine.schema.base import SupportsChangeEvent

if TYPE_CHECKING:
    from pydbzengine.schema.base import BaseSchemaReader
    from pydbzengine.schema.models import CanonicalSchema

T_Record = TypeVar("T_Record", bound=SupportsChangeEvent)


class StreamChunk(NamedTuple, Generic[T_Record]):
    """
    A contiguous slice of CDC records sharing the exact same CanonicalSchema.

    Attributes:
        schema: The CanonicalSchema shared by all records in this slice.
        records: Contiguous list of change event records.
    """

    schema: CanonicalSchema
    records: list[T_Record]


class StreamPartitioner:
    """Utility class for slicing CDC change event streams by schema boundaries."""

    @staticmethod
    def chunk_by_schema(
        records: Sequence[T_Record],
        schema_reader: BaseSchemaReader,
        flattening_enabled: bool = True,
    ) -> Iterator[StreamChunk[T_Record]]:
        """
        Slices a stream of CDC change events into contiguous chunks sharing the exact same schema.

        Guarantees strict arrival order (V1 -> V2) without reordering:
        When a schema transition occurs mid-stream (e.g. ALTER TABLE), this generator yields
        the completed pre-transition chunk before starting a new chunk for the post-transition schema.
        """
        current_schema: CanonicalSchema | None = None
        current_chunk: list[T_Record] = []
        last_raw_val: Any = None

        for record in records:
            if not schema_reader.can_read(record):
                continue

            raw_val = record.value() if hasattr(record, "value") else None
            # Check if this record's schema matches the active chunk schema
            if (
                raw_val is not None
                and raw_val is last_raw_val
                and current_schema is not None
            ):
                record_schema = current_schema
            else:
                record_schema = schema_reader.extract_schema(
                    record, flattening_enabled=flattening_enabled
                )
                last_raw_val = raw_val

            if (
                current_schema is not None
                and record_schema.fingerprint != current_schema.fingerprint
            ):
                yield StreamChunk(schema=current_schema, records=current_chunk)
                current_chunk = []

            current_schema = record_schema
            current_chunk.append(record)

        if current_chunk and current_schema is not None:
            yield StreamChunk(schema=current_schema, records=current_chunk)
