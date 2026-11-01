from __future__ import annotations

from collections.abc import Collection, Iterator
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property
from typing import Any

import pyarrow as pa

from pydbzengine.schema.types import (
    DataTypeHandler,
    SchemaField,
    StructTypeHandler,
)

__all__ = [
    "DDL_MARKER_FIELDS",
    "ChangeBatch",
    "ChangeRecord",
    "EventKind",
    "SchemaDiff",
    "TableBatch",
    "TableSchema",
]

DDL_MARKER_FIELDS: frozenset[str] = frozenset({"ddl", "databaseName", "tableChanges"})


class EventKind(str, Enum):
    """Categorization of a change event based on payload presence, destination, and schema markers."""

    DATA = "data"
    HEARTBEAT = "heartbeat"
    SCHEMA_CHANGE = "schema_change"
    TOMBSTONE = "tombstone"

    @classmethod
    def classify(
        cls,
        destination: str,
        field_names: Collection[str],
        heartbeat_prefix: str,
    ) -> EventKind:
        if destination.startswith(heartbeat_prefix):
            return cls.HEARTBEAT
        if DDL_MARKER_FIELDS.issubset(field_names):
            return cls.SCHEMA_CHANGE
        return cls.DATA


@dataclass(frozen=True, slots=True)
class SchemaDiff:
    """Describes structural differences between a previous schema and a new schema."""

    added_fields: tuple[pa.Field, ...] = ()
    removed_fields: tuple[pa.Field, ...] = ()
    changed_fields: tuple[tuple[pa.Field, pa.Field], ...] = ()

    @property
    def has_changes(self) -> bool:
        return bool(self.added_fields or self.removed_fields or self.changed_fields)

    @property
    def is_additive(self) -> bool:
        return (
            bool(self.added_fields)
            and not self.removed_fields
            and not self.changed_fields
        )

    @property
    def is_compatible(self) -> bool:
        if self.removed_fields:
            return False
        if not self.changed_fields:
            return True
        old_s = pa.schema([old_f for old_f, _ in self.changed_fields])
        new_s = pa.schema([new_f for _, new_f in self.changed_fields])
        try:
            unified = pa.unify_schemas([new_s, old_s], promote_options="default")
        except pa.ArrowException:
            return False
        return bool(unified.equals(new_s))


@dataclass(frozen=True)
class TableSchema:
    """Compiled analytical and operational schema descriptor for a CDC destination."""

    destination: str
    kind: EventKind
    arrow_schema: pa.Schema
    primary_keys: tuple[str, ...] = ()
    key_schema: pa.Schema | None = None
    value_handler: StructTypeHandler | None = field(default=None, compare=False)
    key_handler: DataTypeHandler | None = field(default=None, compare=False)

    @property
    def value_schema(self) -> pa.Schema:
        return self.arrow_schema

    @property
    def names(self) -> list[str]:
        return self.arrow_schema.names

    @property
    def table_name(self) -> str:
        return self.destination.rsplit(".", 1)[-1]

    @cached_property
    def fields(self) -> tuple[SchemaField, ...]:
        if self.value_handler is None:
            return ()
        return self.value_handler.fields

    def diff(self, previous: TableSchema | pa.Schema) -> SchemaDiff:
        """Computes the structural difference from `previous` schema to `self`."""
        old_pa = (
            previous.arrow_schema if isinstance(previous, TableSchema) else previous
        )
        new_pa = self.arrow_schema

        old_map: dict[str, pa.Field] = {f.name: f for f in old_pa}
        new_map: dict[str, pa.Field] = {f.name: f for f in new_pa}

        added = tuple(f for name, f in new_map.items() if name not in old_map)
        removed = tuple(f for name, f in old_map.items() if name not in new_map)
        changed = tuple(
            (old_map[name], new_f)
            for name, new_f in new_map.items()
            if name in old_map and old_map[name].type != new_f.type
        )
        return SchemaDiff(
            added_fields=added,
            removed_fields=removed,
            changed_fields=changed,
        )


@dataclass(frozen=True, slots=True)
class ChangeRecord:
    """
    Unified domain model representing a single converted CDC change event
    (data records, heartbeats, DDL schema changes, and delete tombstones).
    """

    schema: TableSchema | None = None
    value: dict[str, Any] | None = None
    key: dict[str, Any] | None = None
    partition: int | None = None
    destination: str = ""

    def __post_init__(self) -> None:
        if self.schema is not None and not self.destination:
            object.__setattr__(self, "destination", self.schema.destination)

    @property
    def kind(self) -> EventKind:
        if self.value is None or self.schema is None:
            return EventKind.TOMBSTONE
        return self.schema.kind


@dataclass(frozen=True, slots=True)
class TableBatch:
    """A homogeneous batch of consecutive ChangeRecords sharing a single TableSchema."""

    schema: TableSchema
    records: tuple[ChangeRecord, ...] = ()

    @property
    def destination(self) -> str:
        return self.schema.destination

    @property
    def rows(self) -> list[dict[str, Any]]:
        return [r.value for r in self.records if r.value is not None]

    def to_arrow(self, target_schema: pa.Schema | None = None) -> pa.Table:
        resolved = self.schema.arrow_schema if target_schema is None else target_schema
        return pa.Table.from_pylist(self.rows, schema=resolved)

    def __len__(self) -> int:
        return len(self.records)

    def __iter__(self) -> Iterator[ChangeRecord]:
        return iter(self.records)


@dataclass(frozen=True, slots=True)
class ChangeBatch:
    """
    Ordered collection of converted ChangeRecord items produced from an engine batch.
    Preserves arrival order across all event kinds (data, heartbeat, schema_change, tombstone).
    """

    records: tuple[ChangeRecord, ...] = ()

    @property
    def data_records(self) -> tuple[ChangeRecord, ...]:
        return self.of_kind(EventKind.DATA)

    def of_kind(self, kind: EventKind) -> tuple[ChangeRecord, ...]:
        """Returns all records matching the requested EventKind in arrival order."""
        return tuple(r for r in self.records if r.kind is kind)

    def table_batches(
        self,
        kind: EventKind = EventKind.DATA,
    ) -> tuple[TableBatch, ...]:
        """
        Groups records of `kind` by destination in arrival order and slices them
        into homogeneous TableBatch runs whenever the TableSchema changes.
        """
        if kind is EventKind.TOMBSTONE:
            raise ValueError(
                "Cannot build TableBatch runs for EventKind.TOMBSTONE; use of_kind(EventKind.TOMBSTONE)."
            )
        runs_by_destination: dict[
            str, list[tuple[TableSchema, list[ChangeRecord]]]
        ] = {}
        for record in self.of_kind(kind):
            if record.schema is None:
                continue
            runs = runs_by_destination.setdefault(record.destination, [])
            if runs and runs[-1][0] == record.schema:
                runs[-1][1].append(record)
            else:
                runs.append((record.schema, [record]))
        return tuple(
            TableBatch(schema=schema, records=tuple(rec_list))
            for runs in runs_by_destination.values()
            for schema, rec_list in runs
        )

    def __iter__(self) -> Iterator[ChangeRecord]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)
