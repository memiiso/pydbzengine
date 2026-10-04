from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pydbzengine.schema.base import BaseSchemaReader, SupportsChangeEvent
from pydbzengine.schema.decoders import DEFAULT_VALUE_DECODER, ValueDecoder
from pydbzengine.schema.models import CanonicalSchema
from pydbzengine.schema.readers import (
    DebeziumSchemaReader,
    InferredSchemaReader,
    KeyReader,
)

_DEFAULT_SCHEMA_READER = DebeziumSchemaReader()


@dataclass(frozen=True)
class CdcEvent:
    """
    Unified, immutable representation of a Change Data Capture (CDC) event.

    Bundles decoded row state, canonical schema, primary key coordinates,
    and source metadata in a single, cohesive domain model.
    """

    destination: str
    op: str = ""
    after: dict[str, Any] | None = None
    before: dict[str, Any] | None = None
    value: dict[str, Any] | None = None
    key: dict[str, Any] = field(default_factory=dict)
    schema: CanonicalSchema | None = None
    timestamp: datetime | None = None
    partition: int = 0
    source: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_record(
        cls,
        record: Any,
        flattening_enabled: bool = True,
        decoder: ValueDecoder | None = None,
        schema_reader: BaseSchemaReader | None = None,
    ) -> CdcEvent:
        """Builds a CdcEvent directly from an engine ChangeEvent or any object exposing CDC accessors."""
        return CdcEventParser.parse_record(
            record=record,
            flattening_enabled=flattening_enabled,
            decoder=decoder,
            schema_reader=schema_reader,
        )

    @classmethod
    def from_json(
        cls,
        value: str | bytes | dict[str, Any] | None,
        key: str | bytes | dict[str, Any] | None = None,
        destination: str = "",
        partition: int = 0,
        record_ref: Any = None,
        flattening_enabled: bool = True,
        decoder: ValueDecoder | None = None,
        schema_reader: BaseSchemaReader | None = None,
    ) -> CdcEvent:
        """Builds a CdcEvent directly from raw JSON string, bytes, or parsed dictionaries."""
        if not destination or not str(destination).strip():
            raise ValueError("destination is required and cannot be empty for CDC event parsing")
        return CdcEventParser.parse_json(
            value=value,
            key=key,
            destination=destination,
            partition=partition,
            record_ref=record_ref,
            flattening_enabled=flattening_enabled,
            decoder=decoder,
            schema_reader=schema_reader,
        )

    # --- Properties ---

    @property
    def row(self) -> dict[str, Any]:
        """Returns the active row state: `before` for delete, `after` for create/update/read, falling back to `value`."""
        if self.is_delete:
            if self.before is not None:
                return self.before
            return self.value if self.value is not None else {}
        if self.after is not None:
            return self.after
        return self.value if self.value is not None else {}

    @property
    def is_create(self) -> bool:
        return self.op in ("c", "r", "i")

    @property
    def is_update(self) -> bool:
        return self.op == "u"

    @property
    def is_delete(self) -> bool:
        if self.op == "d":
            return True
        if isinstance(self.value, dict):
            val_del = self.value.get("__deleted")
            if val_del is None:
                val_del = self.value.get("deleted")
            if val_del is not None and str(val_del).strip().lower() in ("true", "t", "1"):
                return True
        return False

    @property
    def is_snapshot(self) -> bool:
        if self.op == "r":
            return True
        if isinstance(self.source, dict):
            snap = str(self.source.get("snapshot", "")).strip().lower()
            return snap in ("true", "last")
        return False

    @property
    def is_tombstone(self) -> bool:
        return self.after is None and self.before is None and not self.op

    @property
    def is_heartbeat(self) -> bool:
        return self.op == "h" or "__debezium-heartbeat" in self.destination

    @property
    def is_schema_change(self) -> bool:
        """Returns True if this event represents a DDL or schema evolution change."""
        if isinstance(self.value, dict):
            if "ddl" in self.value or "tableChanges" in self.value:
                return True
        if isinstance(self.row, dict):
            if "ddl" in self.row or "tableChanges" in self.row:
                return True
        return (
            "schema-changes" in self.destination
            or "schema_changes" in self.destination
            or self.op == "m"
        )

    @property
    def primary_keys(self) -> tuple[str, ...]:
        """Returns primary key column names from schema or key dictionary."""
        if self.schema and self.schema.primary_keys:
            return self.schema.primary_keys
        return tuple(self.key.keys())

    @property
    def fingerprint(self) -> str:
        """Deterministic SHA-256 schema fingerprint for O(1) equality."""
        return self.schema.fingerprint if self.schema else ""

    def to_dict(self, include_audit: bool = True) -> dict[str, Any]:
        """Produces a dictionary of the active row, optionally adding audit fields."""
        data = dict(self.row)
        if include_audit:
            data["_dbz_op"] = self.op
            data["_dbz_ts_ms"] = (
                int(self.timestamp.timestamp() * 1000) if self.timestamp else None
            )
        return data


class CdcEventParser:
    """Parses and deserializes Change Data Capture (CDC) events into CdcEvent models."""

    @classmethod
    def parse_record(
        cls,
        record: SupportsChangeEvent,
        flattening_enabled: bool = True,
        decoder: ValueDecoder | None = None,
        schema_reader: BaseSchemaReader | None = None,
    ) -> CdcEvent:
        dest = str(record.destination())
        part = int(record.partition())
        raw_val = record.value()
        raw_key = record.key()

        return cls.parse_json(
            value=raw_val,
            key=raw_key,
            destination=dest,
            partition=part,
            record_ref=record,
            flattening_enabled=flattening_enabled,
            decoder=decoder,
            schema_reader=schema_reader,
        )

    @classmethod
    def parse_json(
        cls,
        value: str | bytes | dict[str, Any] | None,
        key: str | bytes | dict[str, Any] | None = None,
        destination: str = "",
        partition: int = 0,
        record_ref: Any = None,
        flattening_enabled: bool = True,
        decoder: ValueDecoder | None = None,
        schema_reader: BaseSchemaReader | None = None,
    ) -> CdcEvent:
        if not destination or not str(destination).strip():
            raise ValueError("destination is required and cannot be empty for CDC event parsing")

        dec = decoder or DEFAULT_VALUE_DECODER
        decoded_key = cls._parse_key(key)

        # Tombstone event detection (null or empty value payload)
        if value is None or (
            isinstance(value, (str, bytes)) and not str(value).strip()
        ):
            if not decoded_key:
                raise ValueError(
                    f"Invalid CDC event for destination '{destination}': "
                    "payload is empty/None but no record key was provided. "
                    "Tombstone events must contain a primary key."
                )
            return CdcEvent(
                destination=destination,
                op="",
                key=decoded_key,
                partition=partition,
            )

        data = cls._ensure_dict(value)

        # Extract Canonical Schema
        schema = cls._resolve_schema(
            data=data,
            destination=destination,
            record_ref=record_ref,
            flattening_enabled=flattening_enabled,
            schema_reader=schema_reader,
        )

        # Extract Payload & Source Metadata
        payload = data.get("payload", data) if isinstance(data, dict) else {}
        if not isinstance(payload, dict):
            payload = {}

        if "after" in payload or "before" in payload:
            op, after, before, ts, source, val_dict = cls._parse_envelope_payload(
                payload, schema, dec
            )
        else:
            op, after, before, ts, source, val_dict = cls._parse_flattened_payload(
                payload, schema, dec
            )

        return CdcEvent(
            destination=destination,
            op=op,
            after=after,
            before=before,
            value=val_dict,
            key=decoded_key,
            schema=schema,
            timestamp=ts,
            partition=partition,
            source=source,
        )

    @staticmethod
    def _ensure_dict(raw: Any) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, (str, bytes)):
            if isinstance(raw, str):
                s = raw.strip()
            else:
                try:
                    s = raw.decode("utf-8").strip()
                except UnicodeDecodeError as e:
                    raise ValueError(
                        f"Failed to decode event bytes as UTF-8: {e}"
                    ) from e

            if not s:
                return {}
            try:
                parsed = json.loads(s)
                return parsed if isinstance(parsed, dict) else {"value": parsed}
            except json.JSONDecodeError as e:
                raise ValueError(f"Failed to parse event JSON: {e}") from e
        return {"value": raw}

    @staticmethod
    def _parse_key(raw_key: Any) -> dict[str, Any]:
        return KeyReader.parse_key_payload(raw_key)

    @staticmethod
    def _parse_envelope_payload(
        payload: dict[str, Any],
        schema: CanonicalSchema | None,
        decoder: ValueDecoder,
    ) -> tuple[
        str,
        dict[str, Any] | None,
        dict[str, Any] | None,
        datetime | None,
        dict[str, Any],
        dict[str, Any],
    ]:
        op = str(payload.get("op", "c")).lower()
        source = payload.get("source", {})
        if not isinstance(source, dict):
            source = {}

        ts_raw = payload.get("ts_ms") or payload.get("__ts_ms") or source.get("ts_ms")
        ts: datetime | None = None
        if ts_raw is not None:
            ts = decoder.decode_timestamp(ts_raw)

        before_raw = payload.get("before")
        after_raw = payload.get("after")

        before = (
            decoder.decode_row(before_raw, schema)
            if isinstance(before_raw, dict)
            else None
        )
        after = (
            decoder.decode_row(after_raw, schema)
            if isinstance(after_raw, dict)
            else None
        )
        return op, after, before, ts, source, payload

    @staticmethod
    def _parse_flattened_payload(
        payload: dict[str, Any],
        schema: CanonicalSchema | None,
        decoder: ValueDecoder,
    ) -> tuple[
        str,
        dict[str, Any] | None,
        dict[str, Any] | None,
        datetime | None,
        dict[str, Any],
        dict[str, Any],
    ]:
        is_schema_change = "ddl" in payload or "tableChanges" in payload
        deleted_raw = payload.get("__deleted")
        if deleted_raw is None:
            deleted_raw = payload.get("deleted", "")
        is_deleted = (
            str(deleted_raw).lower() in ("true", "t", "1")
            or str(payload.get("__op") or payload.get("op", "")).lower() == "d"
        )
        if is_deleted:
            op = "d"
        elif is_schema_change:
            op = str(payload.get("op", "m")).lower()
        else:
            op = str(payload.get("__op") or payload.get("op", "c")).lower()

        source = payload.get("source", {})
        if not isinstance(source, dict):
            source = {}

        ts_raw = payload.get("__ts_ms") or payload.get("ts_ms") or source.get("ts_ms")
        ts = None
        if ts_raw is not None:
            ts = decoder.decode_timestamp(ts_raw)

        decoded_row = decoder.decode_row(payload, schema)
        val_dict: dict[str, Any] = decoded_row if decoded_row is not None else payload
        source_dict: dict[str, Any] = source
        if is_deleted:
            return op, None, decoded_row, ts, source_dict, val_dict
        return op, decoded_row, None, ts, source_dict, val_dict

    @classmethod
    def _resolve_schema(
        cls,
        data: dict[str, Any],
        destination: str,
        record_ref: Any = None,
        flattening_enabled: bool = True,
        schema_reader: BaseSchemaReader | None = None,
    ) -> CanonicalSchema | None:
        reader = schema_reader or _DEFAULT_SCHEMA_READER
        target = record_ref if record_ref is not None else data
        extracted: CanonicalSchema | None = None
        if reader.can_read(target):
            extracted = reader.extract_schema(
                target, flattening_enabled=flattening_enabled
            )
        elif record_ref is not None and reader.can_read(data):
            extracted = reader.extract_schema(
                data, flattening_enabled=flattening_enabled
            )

        if extracted is not None:
            if destination and (
                extracted.identifier == "unknown" or not extracted.identifier
            ):
                return CanonicalSchema(
                    identifier=destination,
                    fields=extracted.fields,
                    primary_keys=extracted.primary_keys,
                )
            return extracted

        payload = data.get("payload", data) if isinstance(data, dict) else {}
        row = payload.get("after") or payload.get("before") or payload
        if isinstance(row, dict) and row:
            return InferredSchemaReader.infer_schema(destination or "default", row)

        return None
