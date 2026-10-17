from __future__ import annotations

import json
import uuid
from abc import abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, ClassVar

import pyarrow as pa

from pydbzengine.engine.base import BasePythonChangeHandler, ChangeEvent
from pydbzengine.schema.coercers import (
    coerce_binary,
    coerce_date_iso,
    coerce_time_iso,
    coerce_time_ms,
    coerce_time_ns,
    coerce_timestamp_iso_naive,
    coerce_timestamp_iso_utc,
    coerce_timestamp_ms,
    coerce_timestamp_ms_utc,
    coerce_timestamp_ns,
    coerce_timestamp_ns_utc,
    make_decimal_coercer,
    make_list_coercer,
    make_map_coercer,
    make_struct_coercer,
    make_variable_scale_decimal_coercer,
)
from pydbzengine.schema.models import (
    DebeziumConfig,
    SchemaDiff,
    TableBatch,
    TableSchema,
)

__all__ = [
    "BaseBatchChangeHandler",
    "SchemaBatchReader",
]


@dataclass
class _ActiveChunk:
    destination: str
    table_schema: TableSchema
    rows: list[dict[str, Any]] = field(default_factory=list)
    records: list[ChangeEvent] = field(default_factory=list)
    schema_changed: bool = False
    schema_diff: SchemaDiff | None = None

    def to_batch(self) -> TableBatch:
        return TableBatch(
            destination=self.destination,
            table_schema=self.table_schema,
            arrow_table=pa.Table.from_pylist(
                self.rows, schema=self.table_schema.arrow_schema
            ),
            schema_changed=self.schema_changed,
            schema_diff=self.schema_diff,
            records=tuple(self.records),
        )


class SchemaBatchReader:
    """
    Single-pass, schema-oriented reader that converts raw Debezium ChangeEvents
    into strongly-typed TableBatches with automatic schema evolution tracking.
    """

    PRIMITIVE_ARROW_MAP: ClassVar[dict[str, pa.DataType]] = {
        # Booleans
        "boolean": pa.bool_(),
        "bool": pa.bool_(),
        # Signed integers
        "int8": pa.int8(),
        "byte": pa.int8(),
        "tinyint": pa.int8(),
        "int16": pa.int16(),
        "short": pa.int16(),
        "smallint": pa.int16(),
        "int32": pa.int32(),
        "int": pa.int32(),
        "integer": pa.int32(),
        "int64": pa.int64(),
        "long": pa.int64(),
        "bigint": pa.int64(),
        # Unsigned integers
        "uint8": pa.uint8(),
        "uint16": pa.uint16(),
        "uint32": pa.uint32(),
        "uint64": pa.uint64(),
        # Floating point
        "float": pa.float32(),
        "float4": pa.float32(),
        "float16": pa.float32(),
        "float32": pa.float32(),
        "real": pa.float32(),
        "double": pa.float64(),
        "float8": pa.float64(),
        "float64": pa.float64(),
        # Strings & Text
        "string": pa.string(),
        "text": pa.string(),
        "varchar": pa.string(),
        "char": pa.string(),
        "character": pa.string(),
        "nvarchar": pa.string(),
        "clob": pa.string(),
        "json": pa.string(),
        "jsonb": pa.string(),
        "xml": pa.string(),
        "uuid": pa.string(),
        # Temporal primitives
        "date": pa.date32(),
        "time": pa.time64("us"),
        "timestamp": pa.timestamp("us"),
        "timestamptz": pa.timestamp("us", tz="UTC"),
        "timestamp_tz": pa.timestamp("us", tz="UTC"),
        "timestamp_ltz": pa.timestamp("us", tz="UTC"),
        # Binaries
        "bytes": pa.binary(),
        "binary": pa.binary(),
        "fixed": pa.binary(),
        "blob": pa.binary(),
        "varbinary": pa.binary(),
    }

    def __init__(
        self,
        config: DebeziumConfig | None = None,
        *,
        flatten: bool = True,
        delete_handling_mode: str | None = None,
        default_variable_scale: int = 10,
        strict_schemas: bool = False,
    ) -> None:
        self.config = config
        self.flatten = flatten
        raw_delete_mode = (
            delete_handling_mode
            if delete_handling_mode is not None
            else (config.delete_handling_mode if config is not None else "drop")
        )
        mode = raw_delete_mode.strip().lower()
        if mode not in ("drop", "rewrite", "none"):
            raise ValueError(
                f"Invalid delete_handling_mode: {raw_delete_mode!r}. "
                "Expected 'drop', 'rewrite', or 'none'."
            )
        self.delete_handling_mode = mode
        self.default_variable_scale = (
            config.default_variable_scale
            if config is not None
            else default_variable_scale
        )
        self.strict_schemas = strict_schemas
        self._connect_schema_cache: dict[
            tuple[str, bool, str],
            tuple[dict[str, Any], tuple[str, ...], TableSchema],
        ] = {}
        self._active_schemas: dict[str, TableSchema] = {}

    def read_batch(self, records: Sequence[ChangeEvent]) -> list[TableBatch]:
        """
        Reads a batch of Debezium ChangeEvent records in a single pass, converting them
        into strongly-typed TableBatches with automatic schema evolution tracking.
        """
        completed_batches: list[TableBatch] = []
        active_chunks: dict[str, _ActiveChunk] = {}

        for record in records:
            parsed = self._parse_record_single_pass(record)
            if parsed is None:
                continue

            destination, table_schema, row = parsed

            # Check if destination currently has an active chunk in this batch
            current_chunk = active_chunks.get(destination)
            if current_chunk is not None:
                if current_chunk.table_schema.fingerprint != table_schema.fingerprint:
                    # Mid-batch schema evolution: flush previous chunk in strict arrival order
                    completed_batches.append(current_chunk.to_batch())
                    diff = table_schema.diff_from(current_chunk.table_schema)
                    self._active_schemas[destination] = table_schema
                    current_chunk = _ActiveChunk(
                        destination=destination,
                        table_schema=table_schema,
                        schema_changed=True,
                        schema_diff=diff,
                    )
                    active_chunks[destination] = current_chunk
            else:
                prev_schema = self._active_schemas.get(destination)
                schema_changed = (
                    prev_schema is not None
                    and prev_schema.fingerprint != table_schema.fingerprint
                )
                schema_diff = (
                    table_schema.diff_from(prev_schema)
                    if schema_changed and prev_schema is not None
                    else None
                )
                self._active_schemas[destination] = table_schema
                current_chunk = _ActiveChunk(
                    destination=destination,
                    table_schema=table_schema,
                    schema_changed=schema_changed,
                    schema_diff=schema_diff,
                )
                active_chunks[destination] = current_chunk

            if table_schema.row_coercers:
                table_schema.coerce_row_in_place(row)

            current_chunk.rows.append(row)
            current_chunk.records.append(record)

        for chunk in active_chunks.values():
            if chunk.rows:
                completed_batches.append(chunk.to_batch())

        return completed_batches

    def _parse_record_single_pass(
        self, record: ChangeEvent
    ) -> tuple[str, TableSchema, dict[str, Any]] | None:
        raw_dest = record.destination()
        destination = str(raw_dest).strip() if raw_dest is not None else ""
        if not destination:
            raise ValueError("Record contains an empty or missing destination.")

        # Skip Debezium heartbeat messages
        if destination.startswith("__debezium-heartbeat"):
            return None

        value_schema_fn = getattr(record, "valueSchema", None)
        if callable(value_schema_fn):
            j_val_schema = value_schema_fn()
            if j_val_schema is not None:
                return self._parse_java_connect_record(
                    record, destination, j_val_schema
                )

        raw_val = record.value()
        if raw_val is None:
            return None

        if isinstance(raw_val, (str, bytes, bytearray, memoryview)):
            val_str = (
                raw_val.strip()
                if isinstance(raw_val, str)
                else bytes(raw_val).decode("utf-8").strip()
            )
            if not val_str:
                return None
            try:
                data = json.loads(val_str)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Invalid JSON in event for destination '{destination}': {e}"
                ) from e
        elif isinstance(raw_val, dict):
            data = raw_val
        else:
            raise TypeError(
                f"Unsupported event value type {type(raw_val).__name__} for destination '{destination}'"
            )

        if not isinstance(data, dict):
            raise ValueError(
                f"Expected JSON object in event for destination '{destination}', got {type(data).__name__}"
            )

        primary_keys = tuple(self.extract_primary_keys(record))

        if "schema" in data and "payload" in data:
            top_schema = data["schema"]
            if not isinstance(top_schema, dict):
                raise ValueError(
                    f"Expected dict 'schema' in event for destination '{destination}', got {type(top_schema).__name__}"
                )
            payload = data["payload"]
            if payload is None:
                return None
            if not isinstance(payload, dict):
                raise ValueError(
                    f"Expected dict 'payload' in event for destination '{destination}', got {type(payload).__name__}"
                )
            if self._is_schema_change_event(payload):
                return None
            table_schema = self.compile_connect_schema(
                destination=destination,
                top_schema=top_schema,
                primary_keys=primary_keys,
            )
            has_env = any(
                isinstance(f, dict)
                and f.get("field") in ("after", "before")
                and f.get("type") == "struct"
                for f in top_schema["fields"]
            )
            row = self._extract_row_from_payload(payload, has_envelope_schema=has_env)
            if row is None:
                return None
            return destination, table_schema, row

        if self.strict_schemas:
            raise ValueError(
                f"Event for destination '{destination}' lacks a Debezium Connect 'schema' block."
            )

        if "payload" in data and data["payload"] is None and len(data) == 1:
            return None
        payload = (
            data["payload"]
            if ("payload" in data and isinstance(data["payload"], dict))
            else data
        )
        if self._is_schema_change_event(payload):
            return None
        row = self._extract_row_from_payload(payload, has_envelope_schema=False)
        if row is None:
            return None

        table_schema = self.infer_table_schema(
            destination=destination,
            row=row,
            primary_keys=primary_keys,
            previous_schema=self._active_schemas.get(destination),
        )
        return destination, table_schema, row

    def _parse_java_connect_record(
        self, record: ChangeEvent, destination: str, j_val_schema: Any
    ) -> tuple[str, TableSchema, dict[str, Any]] | None:
        primary_keys = tuple(self.extract_primary_keys(record))
        schema_dict = self._java_schema_to_dict(j_val_schema)
        field_names = {
            f.get("field") for f in schema_dict.get("fields", []) if isinstance(f, dict)
        }
        if (
            "ddl" in field_names
            and "databaseName" in field_names
            and "tableChanges" in field_names
        ):
            return None

        table_schema = self.compile_connect_schema(
            destination=destination,
            top_schema=schema_dict,
            primary_keys=primary_keys,
        )
        raw_val = record.value()
        if raw_val is None:
            return None

        has_env = any(
            isinstance(f, dict)
            and f.get("field") in ("after", "before")
            and f.get("type") == "struct"
            for f in schema_dict.get("fields", [])
        )
        is_deleted = False
        if self.flatten and has_env:
            if isinstance(raw_val, Mapping):
                after = raw_val.get("after")
                before = raw_val.get("before")
            elif hasattr(raw_val, "get"):
                getter = getattr(raw_val, "getWithoutDefault", None) or raw_val.get
                after = getter("after")
                before = getter("before")
            else:
                raise TypeError(
                    f"Unsupported Connect record value type {type(raw_val).__name__} for destination '{destination}'"
                )
            if after is not None:
                raw_val = after
            else:
                if self.delete_handling_mode == "drop" or before is None:
                    return None
                raw_val = before
                is_deleted = True

        if isinstance(raw_val, Mapping):
            row = {k: self._unwrap_java_value(v) for k, v in raw_val.items()}
        elif hasattr(raw_val, "get"):
            getter = getattr(raw_val, "getWithoutDefault", None) or raw_val.get
            row = {
                col: self._unwrap_java_value(getter(col))
                for col in table_schema.column_names
                if col != "__deleted"
            }
        else:
            raise TypeError(
                f"Unsupported Connect record value type {type(raw_val).__name__} for destination '{destination}'"
            )

        if self.flatten and has_env and self.delete_handling_mode == "rewrite":
            row["__deleted"] = is_deleted
        return destination, table_schema, row

    @classmethod
    def _unwrap_java_value(cls, val: Any) -> Any:
        """Recursively converts nested Java Connect Structs, Lists, and Maps into Python dicts/lists."""
        if val is None or isinstance(
            val,
            (
                str,
                int,
                float,
                bool,
                bytes,
                bytearray,
                memoryview,
                Decimal,
                date,
                datetime,
                time,
            ),
        ):
            return val
        if isinstance(val, Mapping):
            return {k: cls._unwrap_java_value(v) for k, v in val.items()}
        if isinstance(val, Sequence):
            return [cls._unwrap_java_value(x) for x in val]
        if hasattr(val, "schema") and hasattr(val, "get"):
            j_schema = val.schema()
            fields = j_schema.fields() if j_schema is not None else None
            if fields is not None:
                getter = getattr(val, "getWithoutDefault", None) or val.get
                return {
                    str(f.name()): cls._unwrap_java_value(getter(f.name()))
                    for f in fields
                }
        return val

    @staticmethod
    def _is_schema_change_event(payload: Mapping[str, Any]) -> bool:
        """Returns True if the payload is a Debezium DDL schema history event."""
        return (
            "ddl" in payload and "databaseName" in payload and "tableChanges" in payload
        )

    def _extract_row_from_payload(
        self, payload: dict[str, Any], has_envelope_schema: bool
    ) -> dict[str, Any] | None:
        if not self.flatten:
            return dict(payload)

        if has_envelope_schema:
            return self._extract_envelope_row(payload)

        # Schemaless envelope detection: only treat as envelope if not configured as flattened
        if self.config is None or not self.config.is_flattened:
            if "op" in payload and ("after" in payload or "before" in payload):
                return self._extract_envelope_row(payload)

        return dict(payload)

    def _extract_envelope_row(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        after = payload.get("after")
        if after is not None:
            if not isinstance(after, dict):
                raise ValueError(
                    f"Expected dict for envelope 'after', got {type(after).__name__}"
                )
            row = dict(after)
            if self.delete_handling_mode == "rewrite" and "__deleted" not in row:
                row["__deleted"] = False
            return row

        if self.delete_handling_mode == "drop":
            return None

        before = payload.get("before")
        if before is not None:
            if not isinstance(before, dict):
                raise ValueError(
                    f"Expected dict for envelope 'before', got {type(before).__name__}"
                )
            row = dict(before)
            if self.delete_handling_mode == "rewrite":
                row["__deleted"] = True
            return row
        return None

    def compile_connect_schema(
        self,
        destination: str,
        top_schema: dict[str, Any],
        primary_keys: tuple[str, ...] = (),
    ) -> TableSchema:
        """
        Compiles a Debezium Connect schema dictionary directly into a TableSchema
        (wrapping `pa.Schema` and sparse `row_coercers`), using memoization per destination.
        """
        cache_key = (destination, self.flatten, self.delete_handling_mode)
        cached = self._connect_schema_cache.get(cache_key)
        if cached is not None:
            last_top_schema, last_pks, last_table_schema = cached
            if (
                top_schema is last_top_schema or top_schema == last_top_schema
            ) and primary_keys == last_pks:
                return last_table_schema

        if not isinstance(top_schema, dict) or not isinstance(
            top_schema.get("fields"), list
        ):
            raise ValueError(
                f"Invalid Debezium schema block in event for destination '{destination}': "
                "missing 'fields' list."
            )

        raw_fields = top_schema["fields"]
        unwrapped_envelope = False
        if self.flatten:
            field_map = {f.get("field"): f for f in raw_fields if isinstance(f, dict)}
            if "after" in field_map or "before" in field_map:
                row_struct = (
                    field_map["after"] if "after" in field_map else field_map["before"]
                )
                if row_struct.get("type") == "struct":
                    if not isinstance(row_struct.get("fields"), list):
                        raise ValueError(
                            "Debezium envelope missing struct 'fields' list in 'after'/'before'."
                        )
                    raw_fields = row_struct["fields"]
                    unwrapped_envelope = True

        pa_fields: list[pa.Field] = []
        coercers: list[tuple[str, Callable[[Any], Any]]] = []

        for f_def in raw_fields:
            if not isinstance(f_def, dict):
                raise ValueError(
                    f"Expected dict in schema 'fields', got {type(f_def).__name__}"
                )
            pa_f, coerce_fn = self._compile_connect_field(f_def)
            pa_fields.append(pa_f)
            if coerce_fn is not None:
                coercers.append((pa_f.name, coerce_fn))

        col_names = {f.name for f in pa_fields}
        if (
            unwrapped_envelope
            and self.delete_handling_mode == "rewrite"
            and "__deleted" not in col_names
        ):
            pa_fields.append(pa.field("__deleted", pa.bool_(), nullable=True))
            col_names.add("__deleted")

        valid_pks = tuple(pk for pk in primary_keys if pk in col_names)
        if valid_pks:
            pk_set = set(valid_pks)
            for f in pa_fields:
                if f.name in pk_set and (
                    pa.types.is_list(f.type)
                    or pa.types.is_large_list(f.type)
                    or pa.types.is_map(f.type)
                ):
                    raise ValueError(
                        f"Complex field '{f.name}' of type {f.type} cannot be part of the primary key."
                    )

        table_schema = TableSchema(
            destination=destination,
            arrow_schema=pa.schema(pa_fields),
            primary_keys=valid_pks,
            row_coercers=tuple(coercers),
        )
        self._connect_schema_cache[cache_key] = (
            top_schema,
            primary_keys,
            table_schema,
        )
        return table_schema

    def _compile_connect_field(
        self, field_def: dict[str, Any]
    ) -> tuple[pa.Field, Callable[[Any], Any] | None]:
        name = field_def.get("field")
        if not name or not isinstance(name, str):
            raise ValueError(
                f"Encountered field without valid name in schema: {field_def}"
            )

        optional = bool(field_def.get("optional", True))
        doc = field_def.get("doc")
        logical_name = field_def.get("name")

        pa_type, coerce_fn = self._compile_connect_type(field_def)

        metadata: dict[bytes, bytes] = {}
        if doc:
            metadata[b"doc"] = str(doc).encode("utf-8")
        if logical_name:
            metadata[b"debezium.logical_type"] = str(logical_name).encode("utf-8")

        pa_f = pa.field(
            name,
            pa_type,
            nullable=optional,
            metadata=metadata if metadata else None,
        )
        return pa_f, coerce_fn

    def _compile_connect_type(
        self, field_def: dict[str, Any]
    ) -> tuple[pa.DataType, Callable[[Any], Any] | None]:
        type_str = str(field_def.get("type", "")).lower()
        raw_name = field_def.get("name")
        type_name = str(raw_name) if raw_name is not None else ""
        raw_field = field_def.get("field")
        field_name = str(raw_field) if raw_field is not None else ""
        raw_params = field_def.get("parameters")
        params: Mapping[str, Any] = (
            raw_params if isinstance(raw_params, Mapping) else {}
        )

        # 1. Logical Dates
        if type_name == "io.debezium.time.Date":
            # Epoch days (int32) is natively understood by PyArrow pa.date32() in C++
            return pa.date32(), None
        if type_name in (
            "org.apache.kafka.connect.data.Date",
            "io.debezium.time.IsoDate",
        ):
            return pa.date32(), coerce_date_iso

        # 2. Logical Times
        if type_name == "io.debezium.time.MicroTime":
            # Microseconds of day (int64) is natively understood by PyArrow pa.time64("us") in C++
            return pa.time64("us"), None
        if type_name in (
            "io.debezium.time.Time",
            "org.apache.kafka.connect.data.Time",
        ):
            return pa.time64("us"), coerce_time_ms
        if type_name == "io.debezium.time.NanoTime":
            return pa.time64("us"), coerce_time_ns
        if type_name in ("io.debezium.time.IsoTime", "io.debezium.time.ZonedTime"):
            return pa.time64("us"), coerce_time_iso

        # 3. Logical Timestamps
        if type_name == "io.debezium.time.MicroTimestamp":
            # Epoch microseconds (int64) is natively understood by PyArrow pa.timestamp("us") in C++
            return pa.timestamp("us"), None
        if type_name in (
            "io.debezium.time.Timestamp",
            "org.apache.kafka.connect.data.Timestamp",
            "io.debezium.connector.mongodb.data.BsonTimestamp",
            "io.debezium.data.BsonTimestamp",
        ):
            return pa.timestamp("us"), coerce_timestamp_ms
        if type_name == "io.debezium.time.NanoTimestamp":
            return pa.timestamp("us"), coerce_timestamp_ns
        if type_name == "io.debezium.time.IsoTimestamp":
            return pa.timestamp("us"), coerce_timestamp_iso_naive
        if type_name == "io.debezium.time.ZonedTimestamp":
            return pa.timestamp("us", tz="UTC"), coerce_timestamp_iso_utc

        # 4. Logical Decimals
        if type_name == "io.debezium.data.VariableScaleDecimal":
            scale_val = params.get("scale")
            scale = (
                int(scale_val)
                if scale_val is not None and str(scale_val).strip() != ""
                else self.default_variable_scale
            )
            precision = self._resolve_decimal_precision(params, scale, field_name)
            return (
                pa.decimal128(precision, scale),
                make_variable_scale_decimal_coercer(default_scale=scale),
            )
        if type_name == "org.apache.kafka.connect.data.Decimal" or type_str in (
            "decimal",
            "numeric",
            "number",
        ):
            scale_val = params.get("scale")
            scale = (
                int(scale_val)
                if scale_val is not None and str(scale_val).strip() != ""
                else self.default_variable_scale
            )
            precision = self._resolve_decimal_precision(params, scale, field_name)
            return pa.decimal128(precision, scale), make_decimal_coercer(scale)

        # 5. Other Debezium Logical Types
        if type_name in (
            "io.debezium.time.Year",
            "io.debezium.connector.oracle.data.IntervalYearToMonth",
            "io.debezium.time.IntervalYearToMonth",
        ):
            return pa.int32(), None
        if type_name in (
            "io.debezium.time.MicroDuration",
            "io.debezium.time.NanoDuration",
            "io.debezium.connector.oracle.data.IntervalDayToSecond",
            "io.debezium.time.IntervalDayToSecond",
        ):
            return pa.int64(), None
        if type_name in (
            "io.debezium.time.Interval",
            "io.debezium.data.Uuid",
            "io.debezium.data.Json",
            "io.debezium.data.Xml",
            "io.debezium.data.Enum",
            "io.debezium.data.EnumSet",
            "io.debezium.connector.postgresql.data.Ltree",
            "io.debezium.data.Ltree",
            "io.debezium.connector.mongodb.data.ObjectId",
            "io.debezium.data.ObjectId",
        ):
            return pa.string(), None
        if type_name == "io.debezium.data.Bits":
            return pa.binary(), coerce_binary
        if type_name in (
            "io.debezium.connector.postgresql.data.Hstore",
            "io.debezium.data.Hstore",
        ):
            if type_str == "string":
                return pa.string(), None
            return pa.map_(pa.string(), pa.string()), None

        # 6. Special Debezium metadata timestamp columns
        if type_str in ("int64", "long") and not type_name:
            if field_name in ("__ts_ms", "__source_ts_ms", "ts_ms"):
                return pa.timestamp("us", tz="UTC"), coerce_timestamp_ms_utc
            if field_name in ("__ts_ns", "__source_ts_ns", "ts_ns"):
                return pa.timestamp("us", tz="UTC"), coerce_timestamp_ns_utc

        # 7. Standard Primitives
        if type_str in self.PRIMITIVE_ARROW_MAP:
            pa_type = self.PRIMITIVE_ARROW_MAP[type_str]
            if pa.types.is_binary(pa_type):
                return pa_type, coerce_binary
            return pa_type, None

        # 8. Complex / Nested Types (struct, array, map)
        if type_str in ("struct", "object"):
            raw_fields = field_def.get("fields", [])
            if not isinstance(raw_fields, list):
                raise ValueError(
                    f"Expected 'fields' list in struct schema, got {type(raw_fields).__name__}: {field_def}"
                )
            sub_pa_fields: list[pa.Field] = []
            sub_coercers: list[tuple[str, Callable[[Any], Any]]] = []

            for sub_f in raw_fields:
                if not isinstance(sub_f, dict):
                    raise ValueError(
                        f"Expected dict in struct 'fields', got {type(sub_f).__name__}"
                    )
                pa_f, fn = self._compile_connect_field(sub_f)
                sub_pa_fields.append(pa_f)
                if fn is not None:
                    sub_coercers.append((pa_f.name, fn))

            if not sub_pa_fields:
                if type_name in (
                    "io.debezium.data.geometry.Geometry",
                    "io.debezium.data.geometry.Geography",
                ):
                    sub_pa_fields = [
                        pa.field("wkb", pa.binary()),
                        pa.field("srid", pa.int32()),
                    ]
                    sub_coercers = [("wkb", coerce_binary)]
                elif type_name == "io.debezium.data.geometry.Point":
                    sub_pa_fields = [
                        pa.field("x", pa.float64()),
                        pa.field("y", pa.float64()),
                        pa.field("srid", pa.int32()),
                    ]
                else:
                    raise ValueError(
                        f"Struct field '{field_name}' has no sub-fields: {field_def}"
                    )

            struct_coercer = (
                make_struct_coercer(tuple(sub_coercers)) if sub_coercers else None
            )
            return pa.struct(sub_pa_fields), struct_coercer

        if type_str in ("array", "list"):
            elem_schema = (
                field_def["items"] if "items" in field_def else field_def.get("item")
            )
            if not isinstance(elem_schema, dict):
                raise ValueError(
                    f"Array field missing valid 'items' schema: {field_def}"
                )
            elem_type, elem_coercer = self._compile_connect_type(elem_schema)
            list_coercer = (
                make_list_coercer(elem_coercer) if elem_coercer is not None else None
            )
            return pa.list_(elem_type), list_coercer

        if type_str in ("map", "dict"):
            key_schema = (
                field_def["keys"] if "keys" in field_def else field_def.get("key")
            )
            val_schema = (
                field_def["values"] if "values" in field_def else field_def.get("value")
            )
            if not isinstance(key_schema, dict) or not isinstance(val_schema, dict):
                raise ValueError(
                    f"Map field missing valid 'keys' or 'values' schema: {field_def}"
                )
            key_type, key_coercer = self._compile_connect_type(key_schema)
            if pa.types.is_nested(key_type):
                raise ValueError(
                    f"Invalid key type for map field '{field_name}': {key_type}. "
                    "Map keys cannot be struct, array, or map."
                )
            val_type, val_coercer = self._compile_connect_type(val_schema)
            map_coercer = (
                make_map_coercer(key_coercer, val_coercer)
                if (key_coercer is not None or val_coercer is not None)
                else None
            )
            return pa.map_(key_type, val_type), map_coercer

        raise ValueError(
            f"Unsupported Debezium Connect type: '{type_str}' (logical name: '{type_name}')"
        )

    @staticmethod
    def _resolve_decimal_precision(
        params: Mapping[str, Any], scale: int, field_name: str
    ) -> int:
        precision_val = (
            params["connect.decimal.precision"]
            if "connect.decimal.precision" in params
            else params.get("precision")
        )
        precision = (
            int(precision_val)
            if precision_val is not None and str(precision_val).strip() != ""
            else 38
        )
        if precision < 1 or precision > 38:
            raise ValueError(
                f"Decimal precision must be between 1 and 38, got {precision} for field '{field_name}'"
            )
        if scale < 0 or scale > precision:
            raise ValueError(
                f"Decimal scale ({scale}) must be between 0 and precision ({precision}) for field '{field_name}'"
            )
        return precision

    @classmethod
    def infer_table_schema(
        cls,
        destination: str,
        row: Mapping[str, Any],
        primary_keys: tuple[str, ...] = (),
        previous_schema: TableSchema | None = None,
    ) -> TableSchema:
        """
        Infers a TableSchema dynamically from a schemaless row dictionary.
        Preserves known column types from `previous_schema` when a column's value is None.
        """
        pa_fields: list[pa.Field] = []
        coercers: list[tuple[str, Callable[[Any], Any]]] = []

        for k, v in row.items():
            prev_field = (
                previous_schema.field(k) if previous_schema is not None else None
            )
            if v is None and prev_field is not None:
                pa_fields.append(prev_field)
                continue

            pa_type, fn = cls.infer_arrow_type(
                v, previous_type=prev_field.type if prev_field else None
            )
            pa_fields.append(pa.field(k, pa_type, nullable=True))
            if fn is not None:
                coercers.append((k, fn))

        valid_pks = tuple(pk for pk in primary_keys if pk in row)
        return TableSchema(
            destination=destination,
            arrow_schema=pa.schema(pa_fields),
            primary_keys=valid_pks,
            row_coercers=tuple(coercers),
        )

    @classmethod
    def infer_arrow_type(
        cls, val: Any, previous_type: pa.DataType | None = None
    ) -> tuple[pa.DataType, Callable[[Any], Any] | None]:
        """Infers a PyArrow DataType (and optional coercer) from a Python value."""
        if val is None:
            return (previous_type if previous_type is not None else pa.string()), None
        if isinstance(val, str):
            return pa.string(), None
        if isinstance(val, bool):
            return pa.bool_(), None
        if isinstance(val, timedelta):
            return (
                pa.int64(),
                lambda v: (
                    int(v.total_seconds() * 1_000_000)
                    if isinstance(v, timedelta)
                    else int(v)
                ),
            )
        if isinstance(val, int):
            return pa.int64(), None
        if isinstance(val, float):
            return pa.float64(), None
        if isinstance(val, Decimal):
            exp = val.as_tuple().exponent
            scale = min(38, max(0, abs(exp) if isinstance(exp, int) else 2))
            return pa.decimal128(38, scale), None
        if isinstance(val, datetime):
            if val.tzinfo is not None:
                return pa.timestamp("us", tz="UTC"), None
            return pa.timestamp("us"), None
        if isinstance(val, date):
            return pa.date32(), None
        if isinstance(val, time):
            return pa.time64("us"), None
        if isinstance(val, uuid.UUID):
            return pa.string(), str
        if isinstance(val, (bytes, bytearray, memoryview)):
            return pa.binary(), coerce_binary
        if isinstance(val, dict):
            if not val:
                raise ValueError("Cannot infer Arrow struct schema from an empty dict")
            sub_fields: list[pa.Field] = []
            sub_coercers: list[tuple[str, Callable[[Any], Any]]] = []
            prev_sub_map = (
                {f.name: f.type for f in previous_type}
                if (previous_type is not None and pa.types.is_struct(previous_type))
                else {}
            )
            for sk, sv in val.items():
                st, sfn = cls.infer_arrow_type(sv, previous_type=prev_sub_map.get(sk))
                sub_fields.append(pa.field(sk, st, nullable=True))
                if sfn is not None:
                    sub_coercers.append((sk, sfn))
            return (
                pa.struct(sub_fields),
                make_struct_coercer(tuple(sub_coercers)) if sub_coercers else None,
            )
        if isinstance(val, (list, tuple)):
            first_non_null = next((x for x in val if x is not None), None)
            elem_type, elem_fn = cls.infer_arrow_type(first_non_null)
            return (
                pa.list_(elem_type),
                make_list_coercer(elem_fn) if elem_fn is not None else None,
            )
        raise TypeError(
            f"Cannot infer Arrow type for value of type {type(val).__name__}: {val!r}"
        )

    @classmethod
    def extract_primary_keys(cls, record: ChangeEvent) -> list[str]:
        """Extracts primary key column names from record key schema or key payload."""
        key_schema_fn = getattr(record, "keySchema", None)
        if callable(key_schema_fn):
            ks = key_schema_fn()
            if ks is not None:
                fields = ks.fields()
                if fields:
                    return [str(f.name()) for f in fields]

        raw_key = record.key()
        if raw_key is None:
            return []

        if hasattr(raw_key, "schema"):
            ks = raw_key.schema()
            if ks is not None and hasattr(ks, "fields"):
                fields = ks.fields()
                if fields:
                    return [str(f.name()) for f in fields]
        if hasattr(raw_key, "keySet"):
            return [str(k) for k in raw_key.keySet()]

        if isinstance(raw_key, Mapping):
            key_data = dict(raw_key)
        elif isinstance(raw_key, (str, bytes, bytearray, memoryview)):
            s = (
                raw_key.strip()
                if isinstance(raw_key, str)
                else bytes(raw_key).decode("utf-8").strip()
            )
            if not s:
                return []
            try:
                key_data = json.loads(s)
            except json.JSONDecodeError as err:
                raise ValueError(f"Malformed JSON in CDC record key: {s!r}") from err
            if not isinstance(key_data, dict):
                raise ValueError(
                    f"Expected JSON object in CDC record key, got {type(key_data).__name__}: {s!r}"
                )
        else:
            raise TypeError(
                f"Unsupported CDC record key type {type(raw_key).__name__}: {raw_key!r}"
            )

        if "schema" in key_data and isinstance(key_data["schema"], dict):
            schema_fields = key_data["schema"].get("fields", [])
            if not isinstance(schema_fields, list):
                raise ValueError("Invalid key schema 'fields': expected list")
            return [
                f["field"]
                for f in schema_fields
                if isinstance(f, dict) and "field" in f
            ]
        if "payload" in key_data and isinstance(key_data["payload"], dict):
            return list(key_data["payload"].keys())
        return list(key_data.keys())

    def _java_schema_to_dict(self, j_schema: Any) -> dict[str, Any]:
        if j_schema is None:
            raise ValueError("Java Connect schema cannot be None")

        j_type = j_schema.type()
        if j_type is None:
            raise ValueError("Java Connect schema type() cannot be None")
        type_str = str(j_type.name()).lower()

        schema_def: dict[str, Any] = {
            "type": type_str,
            "optional": bool(j_schema.isOptional()),
            "doc": str(j_schema.doc()) if j_schema.doc() else None,
            "name": str(j_schema.name()) if j_schema.name() else None,
        }
        params = j_schema.parameters()
        schema_def["parameters"] = dict(params) if params is not None else {}

        if type_str == "struct":
            fields = j_schema.fields()
            if fields is None:
                raise ValueError("Java Connect STRUCT schema missing fields()")
            sub_fields: list[dict[str, Any]] = []
            for sub_f in fields:
                sub_def = self._java_schema_to_dict(sub_f.schema())
                sub_def["field"] = str(sub_f.name())
                sub_fields.append(sub_def)
            schema_def["fields"] = sub_fields
        elif type_str == "array":
            val_schema = j_schema.valueSchema()
            if val_schema is None:
                raise ValueError("Java Connect ARRAY schema missing valueSchema()")
            schema_def["items"] = self._java_schema_to_dict(val_schema)
        elif type_str == "map":
            key_schema = j_schema.keySchema()
            val_schema = j_schema.valueSchema()
            if key_schema is None or val_schema is None:
                raise ValueError(
                    "Java Connect MAP schema missing keySchema() or valueSchema()"
                )
            schema_def["keys"] = self._java_schema_to_dict(key_schema)
            schema_def["values"] = self._java_schema_to_dict(val_schema)

        return schema_def


class BaseBatchChangeHandler(BasePythonChangeHandler):
    """
    Schema-aware base change handler that reads incoming Debezium ChangeEvent batches
    in a single pass via `SchemaBatchReader` and hands over strongly-typed `TableBatch`
    objects to downstream implementations.
    """

    def __init__(
        self,
        config: DebeziumConfig | None = None,
        reader: SchemaBatchReader | None = None,
    ) -> None:
        self.config = config
        if reader is not None:
            self.reader = reader
        elif config is not None:
            self.reader = config.create_batch_reader()
        else:
            self.reader = SchemaBatchReader()

    def handleJsonBatch(self, records: list[ChangeEvent]) -> None:
        """Converts raw Debezium records into TableBatches and dispatches them."""
        self.log.info(f"Received {len(records)} records")
        batches = self.reader.read_batch(records)
        if batches:
            self.handle_batches(batches)
        self.log.info(f"Consumed {len(records)} records")

    def handle_batches(self, batches: list[TableBatch]) -> None:
        """
        Processes all converted TableBatches in arrival order.
        By default, dispatches each batch to `handle_table_batch(batch)`.
        Subclasses (such as DltChangeHandler) may override this to process all batches together.
        """
        for batch in batches:
            self.handle_table_batch(batch)

    @abstractmethod
    def handle_table_batch(self, batch: TableBatch) -> None:
        """Handles a single strongly-typed TableBatch for one destination and schema version."""
        raise NotImplementedError
