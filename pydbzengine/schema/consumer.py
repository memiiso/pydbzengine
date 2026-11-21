from __future__ import annotations

from pydbzengine.engine.base import BasePythonChangeHandler, ChangeEvent
from pydbzengine.engine.config import DebeziumConfig
from pydbzengine.schema.converter import EventConverter
from pydbzengine.schema.models import (
    ChangeBatch,
    SchemaDiff,
    TableBatch,
    TableSchema,
)
from pydbzengine.schema.types import validate_variable_scale

__all__ = ["SchemaAwareChangeHandler"]


class SchemaAwareChangeHandler(BasePythonChangeHandler):
    """
    Base class for schema-driven CDC consumers.

    Automatically converts incoming `ChangeEvent` batches via `EventConverter`,
    tracks active `TableSchema` versions per destination, and dispatches
    table creation, schema evolution, and homogeneous table batches.
    """

    def __init__(
        self,
        *,
        converter: EventConverter | None = None,
        config: DebeziumConfig | None = None,
        default_variable_scale: int = 10,
    ) -> None:
        super().__init__()
        self.default_variable_scale: int = validate_variable_scale(
            default_variable_scale
        )
        if converter is not None and config is not None:
            raise ValueError(
                "Pass either 'converter' or 'config' to SchemaAwareChangeHandler, not both."
            )
        if converter is not None and not isinstance(converter, EventConverter):
            raise TypeError(
                f"Expected EventConverter for converter, got {type(converter).__name__}"
            )
        self.converter: EventConverter | None = converter
        self._schemas: dict[str, TableSchema] = {}
        if config is not None:
            self.configure(config)

    def configure(self, config: DebeziumConfig) -> None:
        self.converter = EventConverter.from_config(
            config=config,
            default_variable_scale=self.default_variable_scale,
        )

    @property
    def schemas(self) -> dict[str, TableSchema]:
        """Returns a copy of the currently tracked TableSchemas by destination."""
        return dict(self._schemas)

    def get_schema(self, destination: str) -> TableSchema | None:
        """Returns the latest observed TableSchema for `destination`, if any."""
        return self._schemas.get(destination)

    def handle_batch(self, records: list[ChangeEvent]) -> int:
        if self.converter is None:
            raise RuntimeError(
                f"Handler '{type(self).__name__}' is not configured. "
                "Pass the handler to DebeziumEngine or call configure() before handling batches."
            )
        if not records:
            return 0
        self.handle_change_batch(self.converter.convert(records))
        return len(records)

    def reconcile_schema(self, schema: TableSchema) -> SchemaDiff | None:
        """
        Reconciles tracked destination schema state atomically, invoking
        `on_table_created` on first observation or `on_schema_evolved` when
        structural changes occur. Returns the computed `SchemaDiff` if evolved.
        """
        destination = schema.destination
        old_schema = self._schemas.get(destination)
        if old_schema is None:
            self.on_table_created(schema)
            self._schemas[destination] = schema
            return None
        if schema != old_schema:
            diff = schema.diff_from(old_schema)
            if diff.has_changes:
                self.on_schema_evolved(schema, old_schema, diff)
            self._schemas[destination] = schema
            return diff if diff.has_changes else None
        return None

    def handle_change_batch(self, batch: ChangeBatch) -> None:
        """
        Dispatches homogeneous DATA TableBatches with automatic schema reconciliation.

        Override this method to split `batch` via `batch.of_kind(...)`,
        `batch.group_by_kind()`, or `batch.group_by_destination()` and process
        non-data groups (TOMBSTONE, SCHEMA_CHANGE, HEARTBEAT) or custom ordering.
        """
        for table_batch in batch.to_table_batches():
            self.reconcile_schema(table_batch.schema)
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
