from __future__ import annotations

from collections.abc import Mapping
from functools import cached_property
from typing import Any

from pydbzengine.engine.base import (
    BasePythonChangeHandler,
    ChangeEvent,
    EventFormat,
)
from pydbzengine.schema.converter import EventConverter
from pydbzengine.schema.models import (
    ChangeBatch,
    SchemaDiff,
    TableBatch,
    TableSchema,
)

__all__ = [
    "SchemaAwareChangeHandler",
]


class SchemaAwareChangeHandler(BasePythonChangeHandler):
    """
    Base class for schema-driven CDC consumers.

    Automatically converts incoming `ChangeEvent` batches via `EventConverter`,
    tracks active `TableSchema` versions per destination, and dispatches
    table creation, schema evolution, and homogeneous table batches.
    """

    def __init__(
        self,
        properties: Mapping[str, Any] | None = None,
        *,
        default_variable_scale: int = 10,
        converter: EventConverter | None = None,
    ) -> None:
        super().__init__()
        if converter is not None and not isinstance(converter, EventConverter):
            raise TypeError(
                f"Expected EventConverter for converter, got {type(converter).__name__}"
            )
        self.converter: EventConverter = (
            converter
            if converter is not None
            else EventConverter.from_properties(
                properties=properties,
                default_variable_scale=default_variable_scale,
            )
        )
        self.properties: Mapping[str, Any] = self.converter.properties
        self._schemas: dict[str, TableSchema] = {}

    @cached_property
    def format(self) -> EventFormat:
        return self.converter.format

    @cached_property
    def schemas_enabled(self) -> bool:
        return self.converter.schemas_enabled

    @property
    def schemas(self) -> dict[str, TableSchema]:
        """Returns a copy of the currently tracked TableSchemas by destination."""
        return dict(self._schemas)

    def get_schema(self, destination: str) -> TableSchema | None:
        """Returns the latest observed TableSchema for `destination`, if any."""
        return self._schemas.get(destination)

    def handle_batch(self, records: list[ChangeEvent]) -> int:
        if not records:
            return 0
        batch = self.converter.convert(records)
        self.handle_change_batch(batch)
        return len(records)

    def handle_change_batch(self, batch: ChangeBatch) -> None:
        """
        Routes a converted ChangeBatch into lifecycle hooks (`on_table_created`,
        `on_schema_evolved`) and per-table `handle_table_batch`.
        """
        for table_batch in batch.table_batches():
            dest = table_batch.destination
            new_schema = table_batch.schema
            old_schema = self._schemas.get(dest)
            if old_schema is None:
                self._schemas[dest] = new_schema
                self.on_table_created(new_schema)
            elif new_schema != old_schema:
                diff = new_schema.diff(old_schema)
                self._schemas[dest] = new_schema
                self.on_schema_evolved(new_schema, old_schema, diff)
            self.handle_table_batch(table_batch)

    def on_table_created(self, schema: TableSchema) -> None:
        """Invoked the first time a data TableSchema is observed for a destination."""

    def on_schema_evolved(
        self,
        new_schema: TableSchema,
        old_schema: TableSchema,
        diff: SchemaDiff,
    ) -> None:
        """Invoked when a destination's TableSchema evolves between or within batches."""

    def handle_table_batch(self, batch: TableBatch) -> None:
        """Invoked for each homogeneous TableBatch of data records."""
