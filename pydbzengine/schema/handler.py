from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydbzengine.engine.base import (
    BasePythonChangeHandler,
    ChangeEvent,
    EventFormat,
)
from pydbzengine.schema.converter import (
    ConnectEventConverter,
    EventConverter,
    JsonEventConverter,
    SchemalessJsonEventConverter,
)
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
        if properties is not None and not isinstance(properties, Mapping):
            raise TypeError(
                f"Expected Mapping for properties, got {type(properties).__name__}"
            )
        if converter is not None and not isinstance(converter, EventConverter):
            raise TypeError(
                f"Expected EventConverter for converter, got {type(converter).__name__}"
            )
        self.properties: Mapping[str, Any] = (
            properties if properties is not None else {}
        )
        if converter is not None:
            self.converter: EventConverter = converter
            self.format: EventFormat = converter.format
            self.schemas_enabled: bool = converter.schemas_enabled
            self.heartbeat_prefix: str = converter.heartbeat_prefix
            self.default_variable_scale: int = converter.default_variable_scale
        else:
            self.format = EventFormat.resolve(self.properties)
            self.schemas_enabled = (
                True
                if self.format is EventFormat.CONNECT
                else self._resolve_schemas_enabled(self.properties)
            )
            self.heartbeat_prefix = self._resolve_heartbeat_prefix(self.properties)
            self.default_variable_scale = default_variable_scale
            self.converter = self._build_converter()
        self._schemas: dict[str, TableSchema] = {}

    def _build_converter(self) -> EventConverter:
        if self.format is EventFormat.CONNECT:
            return ConnectEventConverter(
                heartbeat_prefix=self.heartbeat_prefix,
                default_variable_scale=self.default_variable_scale,
            )
        if self.schemas_enabled:
            return JsonEventConverter(
                heartbeat_prefix=self.heartbeat_prefix,
                default_variable_scale=self.default_variable_scale,
            )
        return SchemalessJsonEventConverter(
            heartbeat_prefix=self.heartbeat_prefix,
            default_variable_scale=self.default_variable_scale,
        )

    @classmethod
    def _resolve_heartbeat_prefix(cls, props: Mapping[str, Any]) -> str:
        raw_prefix = props.get("topic.heartbeat.prefix", cls.DEFAULT_HEARTBEAT_PREFIX)
        if not isinstance(raw_prefix, str) or not raw_prefix.strip():
            raise ValueError(
                f"topic.heartbeat.prefix must be a non-empty string, got {raw_prefix!r}"
            )
        return raw_prefix.strip()

    @staticmethod
    def _parse_bool_prop(props: Mapping[str, Any], key: str) -> bool:
        val = props[key]
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            norm = val.strip().lower()
            if norm in ("true", "false"):
                return norm == "true"
        raise ValueError(f"Invalid boolean value for {key}: {val!r}")

    @classmethod
    def _resolve_schemas_enabled(cls, props: Mapping[str, Any]) -> bool:
        general_enabled: bool | None = None
        for key in ("converter.schemas.enable", "schemas.enable"):
            if key in props:
                general_enabled = cls._parse_bool_prop(props, key)
                break

        val_enabled = (
            cls._parse_bool_prop(props, "value.converter.schemas.enable")
            if "value.converter.schemas.enable" in props
            else None
        )
        key_enabled = (
            cls._parse_bool_prop(props, "key.converter.schemas.enable")
            if "key.converter.schemas.enable" in props
            else None
        )

        if general_enabled is None and val_enabled is None and key_enabled is None:
            return True

        effective_val = val_enabled if val_enabled is not None else general_enabled
        effective_key = key_enabled if key_enabled is not None else general_enabled

        if (
            effective_val is not None
            and effective_key is not None
            and effective_val != effective_key
        ):
            raise ValueError(
                "Having two different schema configurations for event "
                f"(schemas.enable={effective_val}) and key (schemas.enable={effective_key}) is not supported."
            )

        return effective_val if effective_val is not None else effective_key  # type: ignore[return-value]

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
