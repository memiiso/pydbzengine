from __future__ import annotations

from typing import Generic, TypeVar

from pydbzengine.schema.base import (
    BaseSchemaEvolver,
    BaseSchemaReader,
    BaseStreamSynchronizer,
    BaseTableWriter,
    SupportsChangePayload,
)

T_Table = TypeVar("T_Table")
T_Batch = TypeVar("T_Batch")
T_Record = TypeVar("T_Record", bound=SupportsChangePayload)


class InStreamSynchronizer(
    BaseStreamSynchronizer[T_Table, T_Batch, T_Record],
    Generic[T_Table, T_Batch, T_Record],
):
    """
    Target-agnostic in-stream schema boundary synchronizer.
    Splits records into contiguous runs sharing the exact same schema fingerprint,
    triggers table evolution on boundary transitions, and writes typed batches.
    Zero dependency on specific target engines (Iceberg, Arrow, DLT, etc.).
    """

    def __init__(
        self,
        schema_reader: BaseSchemaReader,
        evolver: BaseSchemaEvolver[T_Table],
        writer: BaseTableWriter[T_Table, T_Batch, T_Record],
        flattening_enabled: bool = True,
    ) -> None:
        super().__init__(
            schema_reader=schema_reader,
            evolver=evolver,
            writer=writer,
            flattening_enabled=flattening_enabled,
        )

    def process_records(self, table: T_Table, records: list[T_Record]) -> int:
        """
        Processes a list of change events against a target table.
        Splits by schema boundary, evolves the target table if new fields exist,
        and writes batches using the target writer.
        Returns total number of records written.
        """
        chunks = self.split_into_chunks(records)
        total_written = 0

        for schema, chunk_records in chunks:
            # 1. Evolve target table schema if new fields detected
            if self.evolver.needs_evolution(table, schema):
                evolved = self.evolver.evolve_schema(table, schema)
                if evolved:
                    self.logger.info(
                        "Evolved target table for schema fingerprint %s",
                        schema.fingerprint[:12],
                    )

            # 2. Build target batch and write
            batch = self.writer.build_batch(schema, chunk_records)
            written = self.writer.write_batch(table, batch)
            total_written += written

        return total_written
