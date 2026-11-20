from __future__ import annotations

from pydbzengine.engine.base import BasePythonChangeHandler, ChangeEvent
from pydbzengine.engine.config import DebeziumConfig
from pydbzengine.schema.converter import EventConverter
from pydbzengine.schema.models import (
    ChangeBatch,
    ChangeRecord,
    EventKind,
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

    def handle_change_batch(self, batch: ChangeBatch) -> None:
        """
        Routes a converted ChangeBatch in causal order into lifecycle hooks
        (`on_table_created`, `on_schema_evolved`, `on_tombstone`,
        `on_schema_change`, `on_heartbeat`) and per-table `handle_table_batch`.
        """
        for item in batch.destination_events():
            if isinstance(item, TableBatch):
                dest = item.destination
                new_schema = item.schema
                old_schema = self._schemas.get(dest)
                if old_schema is None:
                    self.on_table_created(new_schema)
                    self._schemas[dest] = new_schema
                elif new_schema != old_schema:
                    diff = new_schema.diff(old_schema)
                    if diff.has_changes:
                        self.on_schema_evolved(new_schema, old_schema, diff)
                    self._schemas[dest] = new_schema
                self.handle_table_batch(item)
            elif item.kind is EventKind.TOMBSTONE:
                self.on_tombstone(item)
            elif item.kind is EventKind.SCHEMA_CHANGE:
                self.on_schema_change(item)
            elif item.kind is EventKind.HEARTBEAT:
                self.on_heartbeat(item)

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

    def on_tombstone(self, record: ChangeRecord) -> None:
        """Invoked for each delete tombstone ChangeRecord in the batch."""

    def on_schema_change(self, record: ChangeRecord) -> None:
        """Invoked for each DDL schema_change ChangeRecord in the batch."""

    def on_heartbeat(self, record: ChangeRecord) -> None:
        """Invoked for each heartbeat ChangeRecord in the batch."""
