from __future__ import annotations

import json
from collections.abc import Collection, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, ClassVar

import pyarrow as pa

from pydbzengine.engine.base import ChangeEvent
from pydbzengine.schema.compiler import ConnectSchemaCompiler
from pydbzengine.schema.models import (
    ConverterConfig,
    EventFormat,
    TableSchema,
    unify_arrow_schemas,
)

__all__ = [
    "DDL_MARKER_FIELDS",
    "HEARTBEAT_PREFIX",
    "SchemaConverter",
    "is_heartbeat",
    "is_schema_change",
    "is_tombstone",
]

HEARTBEAT_PREFIX = "__debezium-heartbeat"
DDL_MARKER_FIELDS: frozenset[str] = frozenset({"ddl", "databaseName", "tableChanges"})


def is_heartbeat(record_or_dest: ChangeEvent | str | Any) -> bool:
    """Returns True if a record or destination string belongs to a Debezium heartbeat topic."""
    if isinstance(record_or_dest, str):
        dest = record_or_dest.strip()
    elif hasattr(record_or_dest, "destination") and callable(
        record_or_dest.destination
    ):
        raw_dest = record_or_dest.destination()
        dest = str(raw_dest).strip() if raw_dest is not None else ""
    else:
        return False
    return dest.startswith(HEARTBEAT_PREFIX)


def is_schema_change(
    record_or_payload: ChangeEvent | Mapping[str, Any] | Collection[str] | Any,
) -> bool:
    """Returns True if a record, payload dict, or field-name set represents a Debezium DDL history event."""
    if isinstance(record_or_payload, Mapping):
        if DDL_MARKER_FIELDS.issubset(record_or_payload.keys()):
            return True
        payload = record_or_payload.get("payload")
        if isinstance(payload, Mapping) and DDL_MARKER_FIELDS.issubset(payload.keys()):
            return True
        schema = record_or_payload.get("schema")
        if isinstance(schema, Mapping) and isinstance(schema.get("fields"), list):
            f_names = {
                f.get("field") for f in schema["fields"] if isinstance(f, Mapping)
            }
            return DDL_MARKER_FIELDS.issubset(f_names)
        return False

    if isinstance(record_or_payload, Collection) and not isinstance(
        record_or_payload, (str, bytes, bytearray, memoryview)
    ):
        return DDL_MARKER_FIELDS.issubset(record_or_payload)

    source_rec = (
        record_or_payload
        if hasattr(record_or_payload, "valueSchema")
        else (
            record_or_payload.value()
            if hasattr(record_or_payload, "value") and callable(record_or_payload.value)
            else None
        )
    )
    if source_rec is None:
        return False

    schema_fn = getattr(source_rec, "valueSchema", None)
    if callable(schema_fn):
        j_schema = schema_fn()
        if j_schema is not None and hasattr(j_schema, "fields"):
            fields = j_schema.fields()
            if fields is not None:
                return DDL_MARKER_FIELDS.issubset({str(f.name()) for f in fields})
        return False

    if isinstance(source_rec, (str, bytes, bytearray, memoryview)):
        s = (
            bytes(source_rec).decode("utf-8").strip()
            if isinstance(source_rec, (bytes, bytearray, memoryview))
            else source_rec.strip()
        )
        if not s:
            return False
        try:
            parsed = json.loads(s)
        except json.JSONDecodeError:
            return False
        return isinstance(parsed, Mapping) and is_schema_change(parsed)

    if isinstance(source_rec, Mapping):
        return is_schema_change(source_rec)

    return False


def is_tombstone(record: ChangeEvent | Mapping[str, Any] | Any) -> bool:
    """Returns True if a record or payload is an empty delete tombstone (null payload)."""
    if record is None:
        return True
    if isinstance(record, Mapping):
        if not record:
            return False
        return "payload" in record and record["payload"] is None

    if hasattr(record, "valueSchema") and callable(record.valueSchema):
        return record.value() is None

    if hasattr(record, "value") and callable(record.value):
        raw_val = record.value()
        if raw_val is None:
            return True
        if hasattr(raw_val, "valueSchema") and callable(raw_val.valueSchema):
            return raw_val.value() is None
        if isinstance(raw_val, (str, bytes, bytearray, memoryview)):
            s = (
                bytes(raw_val).decode("utf-8").strip()
                if isinstance(raw_val, (bytes, bytearray, memoryview))
                else raw_val.strip()
            )
            if not s or s == "null":
                return True
            try:
                parsed = json.loads(s)
            except json.JSONDecodeError:
                return False
            if parsed is None:
                return True
            return isinstance(parsed, Mapping) and (
                "payload" in parsed and parsed["payload"] is None
            )
        if isinstance(raw_val, Mapping):
            return "payload" in raw_val and raw_val["payload"] is None
    return False


def _sanitize_null_fields(schema: pa.Schema) -> pa.Schema:
    """Promotes all-null inferred fields (`pa.null()`) to `pa.string()` for Lakehouse/Iceberg compatibility."""
    fields: list[pa.Field] = []
    changed = False
    for f in schema:
        new_t, t_changed = _sanitize_null_type(f.type)
        if t_changed:
            fields.append(f.with_type(new_t).with_nullable(True))
            changed = True
        else:
            fields.append(f)
    return pa.schema(fields, metadata=schema.metadata) if changed else schema


def _sanitize_null_type(dtype: pa.DataType) -> tuple[pa.DataType, bool]:
    if pa.types.is_null(dtype):
        return pa.string(), True
    if pa.types.is_struct(dtype):
        sub_fields: list[pa.Field] = []
        sub_changed = False
        for sf in dtype:
            st, sc = _sanitize_null_type(sf.type)
            sub_fields.append(sf.with_type(st) if sc else sf)
            sub_changed = sub_changed or sc
        return (pa.struct(sub_fields), True) if sub_changed else (dtype, False)
    if pa.types.is_list(dtype):
        vt, vc = _sanitize_null_type(dtype.value_type)
        return (pa.list_(vt), True) if vc else (dtype, False)
    return dtype, False


class SchemaConverter:
    """
    Pure technical CDC Event Converter that resolves `ConverterConfig` at init time
    and compiles JSON or Kafka Connect `SourceRecord` events into Apache Arrow Tables.
    """

    PRIMITIVE_ARROW_MAP: ClassVar[dict[str, pa.DataType]] = (
        ConnectSchemaCompiler.PRIMITIVE_ARROW_MAP
    )

    is_heartbeat = staticmethod(is_heartbeat)
    is_schema_change = staticmethod(is_schema_change)
    is_tombstone = staticmethod(is_tombstone)

    def __init__(
        self,
        config: ConverterConfig | Mapping[str, Any] | None = None,
        *,
        format: EventFormat | str | None = None,
        schemas_enabled: bool | None = None,
        default_variable_scale: int | None = None,
    ) -> None:
        self.config = ConverterConfig.from_properties(
            config,
            format=format,
            schemas_enabled=schemas_enabled,
            default_variable_scale=default_variable_scale,
        )
        self.default_variable_scale = self.config.default_variable_scale
        self.compiler = ConnectSchemaCompiler(
            default_variable_scale=self.default_variable_scale
        )
        self._connect_cache: dict[
            str,
            tuple[Any, tuple[str, ...], dict[str, Any], TableSchema],
        ] = {}

    def to_arrow_batches(
        self,
        records: Sequence[ChangeEvent] | ChangeEvent,
    ) -> list[tuple[TableSchema, pa.Table]]:
        """
        Converts CDC records into homogeneous `(TableSchema, pa.Table)` slices,
        splitting the batch at mid-batch DDL schema evolution boundaries.
        """
        seq = (
            [records]
            if isinstance(records, ChangeEvent) or not isinstance(records, Sequence)
            else records
        )
        if not seq:
            return []

        if not self.config.is_connect and not self.config.schemas_enabled:
            return self._convert_json_schemaless_batch(seq)

        chunks = (
            self._extract_connect_chunks(seq)
            if self.config.is_connect
            else self._extract_json_schema_chunks(seq)
        )
        return [
            (schema, schema.to_arrow(rows, coerce=False)) for schema, rows in chunks
        ]

    def to_arrow(
        self,
        records: Sequence[ChangeEvent] | ChangeEvent,
        *,
        target_schema: pa.Schema | TableSchema | None = None,
    ) -> pa.Table | None:
        """
        Converts a single ChangeEvent or a sequence of ChangeEvents into a single `pa.Table`.
        Unifies additive mid-batch schema evolutions via `pa.unify_schemas()` or aligns
        to `target_schema` when provided. Returns `None` if no data records are present.
        """
        seq = (
            [records]
            if isinstance(records, ChangeEvent) or not isinstance(records, Sequence)
            else records
        )
        if not seq:
            return None

        if not self.config.is_connect and not self.config.schemas_enabled:
            batches = self._convert_json_schemaless_batch(
                seq, target_schema=target_schema
            )
            return batches[0][1] if batches else None

        chunks = (
            self._extract_connect_chunks(seq)
            if self.config.is_connect
            else self._extract_json_schema_chunks(seq)
        )
        if not chunks:
            return None

        if len(chunks) == 1 and target_schema is None:
            schema, rows = chunks[0]
            return schema.to_arrow(rows, coerce=False)

        all_rows = [row for _, rows in chunks for row in rows]
        if target_schema is not None:
            resolved_target = (
                target_schema.arrow_schema
                if isinstance(target_schema, TableSchema)
                else target_schema
            )
            return pa.Table.from_pylist(all_rows, schema=resolved_target)

        unified_pa = unify_arrow_schemas([s.arrow_schema for s, _ in chunks])
        return pa.Table.from_pylist(all_rows, schema=unified_pa)

    def to_rows(
        self,
        records: Sequence[ChangeEvent] | ChangeEvent,
    ) -> tuple[TableSchema | None, list[dict[str, Any]]]:
        """
        Converts records into `(table_schema, coerced_rows)`.
        """
        seq = (
            [records]
            if isinstance(records, ChangeEvent) or not isinstance(records, Sequence)
            else records
        )
        if not seq:
            return None, []

        if not self.config.is_connect and not self.config.schemas_enabled:
            batches = self._convert_json_schemaless_batch(seq)
            if not batches:
                return None, []
            schema, tbl = batches[0]
            return schema, tbl.to_pylist()

        chunks = (
            self._extract_connect_chunks(seq)
            if self.config.is_connect
            else self._extract_json_schema_chunks(seq)
        )
        if not chunks:
            return None, []

        evolved_schema = chunks[0][0]
        for s, _ in chunks[1:]:
            evolved_schema = evolved_schema.evolve(s)
        all_rows = [row for _, rows in chunks for row in rows]
        return evolved_schema, all_rows

    def extract_event(
        self,
        record: ChangeEvent,
    ) -> tuple[TableSchema, dict[str, Any]] | None:
        """Extracts `(table_schema, coerced_row)` from a single ChangeEvent."""
        schema, rows = self.to_rows([record])
        if schema is None or not rows:
            return None
        return schema, rows[0]

    def compile_connect_schema(
        self,
        destination: str,
        top_schema: dict[str, Any],
        primary_keys: tuple[str, ...] = (),
    ) -> TableSchema:
        """Compiles a Debezium Connect schema dictionary into a `TableSchema`."""
        return self.compiler.compile(
            destination=destination,
            top_schema=top_schema,
            primary_keys=primary_keys,
        )

    def extract_primary_keys(self, record: ChangeEvent) -> list[str]:
        """Extracts primary key column names from a record's key."""
        if self.config.is_connect:
            return self._extract_connect_pks(record)
        return self._extract_json_pks(record)

    def _extract_json_schema_chunks(
        self,
        records: Sequence[ChangeEvent],
    ) -> list[tuple[TableSchema, list[dict[str, Any]]]]:
        chunks: list[tuple[TableSchema, list[dict[str, Any]]]] = []
        pks_by_dest: dict[str, tuple[str, ...]] = {}

        for record in records:
            dest = self._require_destination(record)
            raw_val = record.value()
            if raw_val is None:
                continue

            data = self._decode_json_object(raw_val, f"event for destination '{dest}'")
            if data is None:
                continue

            if "payload" in data and data["payload"] is None:
                continue

            if "schema" not in data or "payload" not in data:
                raise ValueError(
                    f"Event for destination '{dest}' is missing required 'schema' and 'payload' blocks. "
                    "If your connector is configured with 'schemas.enable=false' or 'value.converter.schemas.enable=false', "
                    "initialize the converter with 'SchemaConverter(schemas_enabled=False)'."
                )

            top_schema = data["schema"]
            if not isinstance(top_schema, dict):
                raise ValueError(
                    f"Expected dict 'schema' in event for destination '{dest}', got {type(top_schema).__name__}"
                )
            payload = data["payload"]
            if not isinstance(payload, dict):
                raise ValueError(
                    f"Expected dict 'payload' in event for destination '{dest}', got {type(payload).__name__}"
                )

            pks = pks_by_dest.get(dest)
            if pks is None:
                pks = tuple(self._extract_json_pks(record))
                pks_by_dest[dest] = pks

            table_schema = self.compiler.compile(
                destination=dest,
                top_schema=top_schema,
                primary_keys=pks,
            )
            row = table_schema.coerce_row(dict(payload))

            if chunks and (
                chunks[-1][0] is table_schema or chunks[-1][0] == table_schema
            ):
                chunks[-1][1].append(row)
            else:
                chunks.append((table_schema, [row]))

        return chunks

    def _convert_json_schemaless_batch(
        self,
        records: Sequence[ChangeEvent],
        *,
        target_schema: pa.Schema | TableSchema | None = None,
    ) -> list[tuple[TableSchema, pa.Table]]:
        rows: list[dict[str, Any]] = []
        dest: str | None = None
        pks: tuple[str, ...] = ()

        for record in records:
            d = self._require_destination(record)
            raw_val = record.value()
            if raw_val is None:
                continue
            data = self._decode_json_object(raw_val, f"event for destination '{d}'")
            if data is None:
                continue
            if "payload" in data:
                payload = data["payload"]
                if payload is None:
                    continue
                if not isinstance(payload, dict):
                    raise ValueError(
                        f"Expected dict 'payload' in event for destination '{d}', got {type(payload).__name__}"
                    )
            else:
                payload = data

            if dest is None:
                dest = d
                pks = tuple(self._extract_json_pks(record))
            rows.append(dict(payload))

        if not rows or dest is None:
            return []

        if target_schema is not None:
            resolved_target = (
                target_schema.arrow_schema
                if isinstance(target_schema, TableSchema)
                else target_schema
            )
            tbl = pa.Table.from_pylist(rows, schema=resolved_target)
            valid_pks = tuple(pk for pk in pks if pk in resolved_target.names)
            return [
                (
                    TableSchema(
                        destination=dest,
                        arrow_schema=resolved_target,
                        primary_keys=valid_pks,
                    ),
                    tbl,
                )
            ]

        raw_tbl = pa.Table.from_pylist(rows)
        sanitized_pa = _sanitize_null_fields(raw_tbl.schema)
        if not sanitized_pa.equals(raw_tbl.schema):
            raw_tbl = pa.Table.from_pylist(rows, schema=sanitized_pa)

        candidate_names = set(sanitized_pa.names)
        for f in sanitized_pa:
            if f.name in ("after", "before") and pa.types.is_struct(f.type):
                for sub_f in f.type:
                    candidate_names.add(sub_f.name)
        valid_pks = tuple(pk for pk in pks if pk in candidate_names)

        table_schema = TableSchema(
            destination=dest,
            arrow_schema=sanitized_pa,
            primary_keys=valid_pks,
        )
        return [(table_schema, raw_tbl)]

    def _extract_connect_chunks(
        self,
        records: Sequence[ChangeEvent],
    ) -> list[tuple[TableSchema, list[dict[str, Any]]]]:
        chunks: list[tuple[TableSchema, list[dict[str, Any]]]] = []

        for record in records:
            dest = self._require_destination(record)
            source_rec: Any = (
                record if hasattr(record, "valueSchema") else record.value()
            )
            if source_rec is None:
                continue

            schema_fn = getattr(source_rec, "valueSchema", None)
            if not callable(schema_fn):
                raise TypeError(
                    f"Expected Java Connect SourceRecord with valueSchema() for destination '{dest}', "
                    f"got {type(source_rec).__name__}"
                )
            j_val_schema = schema_fn()
            if j_val_schema is None:
                continue

            raw_val = source_rec.value()
            if raw_val is None:
                continue

            schema_dict, table_schema = self._resolve_connect_schema(
                dest, j_val_schema, record
            )
            unwrapped = self._unwrap_java_value(raw_val, schema_dict)
            if not isinstance(unwrapped, dict):
                raise TypeError(
                    f"Unsupported Connect record value type {type(raw_val).__name__} for destination '{dest}'"
                )
            row = table_schema.coerce_row(unwrapped)

            if chunks and (
                chunks[-1][0] is table_schema or chunks[-1][0] == table_schema
            ):
                chunks[-1][1].append(row)
            else:
                chunks.append((table_schema, [row]))

        return chunks

    def _resolve_connect_schema(
        self,
        destination: str,
        j_val_schema: Any,
        record: ChangeEvent,
    ) -> tuple[dict[str, Any], TableSchema]:
        cached = self._connect_cache.get(destination)
        if cached is not None:
            last_j_schema, _, last_schema_dict, last_table_schema = cached
            if j_val_schema is last_j_schema or j_val_schema == last_j_schema:
                return last_schema_dict, last_table_schema

        pks = tuple(self._extract_connect_pks(record))
        schema_dict = self._java_schema_to_dict(j_val_schema)
        table_schema = self.compiler.compile(
            destination=destination,
            top_schema=schema_dict,
            primary_keys=pks,
        )
        self._connect_cache[destination] = (
            j_val_schema,
            pks,
            schema_dict,
            table_schema,
        )
        return schema_dict, table_schema

    @staticmethod
    def _require_destination(record: ChangeEvent) -> str:
        raw_dest = record.destination()
        destination = str(raw_dest).strip() if raw_dest is not None else ""
        if not destination:
            raise ValueError("Record contains an empty or missing destination.")
        return destination

    @staticmethod
    def _decode_json_object(raw: Any, context: str) -> dict[str, Any] | None:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, Mapping):
            return dict(raw)
        if isinstance(raw, (bytes, bytearray, memoryview)):
            payload_str = bytes(raw).decode("utf-8").strip()
        elif isinstance(raw, str) or type(raw).__name__ == "java.lang.String":
            payload_str = str(raw).strip()
        else:
            raise TypeError(
                f"Unsupported payload type {type(raw).__name__} for {context}: {raw!r}"
            )

        if not payload_str:
            return None
        try:
            parsed = json.loads(payload_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {context}: {e}") from e

        if not isinstance(parsed, dict):
            raise ValueError(
                f"Expected JSON object in {context}, got {type(parsed).__name__}"
            )
        return parsed

    @classmethod
    def _extract_json_pks(cls, record: ChangeEvent) -> list[str]:
        raw_key = record.key()
        if raw_key is None:
            return []

        key_data = cls._decode_json_object(raw_key, "CDC record key")
        if key_data is None:
            return []

        if "schema" in key_data or "payload" in key_data:
            key_schema = key_data.get("schema")
            if isinstance(key_schema, dict) and "fields" in key_schema:
                schema_fields = key_schema["fields"]
                if not isinstance(schema_fields, list):
                    raise ValueError("Invalid key schema 'fields': expected list")
                return [
                    f["field"]
                    for f in schema_fields
                    if isinstance(f, dict) and "field" in f
                ]
            key_payload = key_data.get("payload")
            if key_payload is None:
                return []
            if not isinstance(key_payload, dict):
                raise ValueError(
                    f"Expected dict for key 'payload', got {type(key_payload).__name__}"
                )
            return list(key_payload.keys())

        return list(key_data.keys())

    @classmethod
    def _extract_connect_pks(cls, record: ChangeEvent) -> list[str]:
        source_rec: Any = record
        if not hasattr(source_rec, "keySchema"):
            inner = record.value()
            if hasattr(inner, "keySchema"):
                source_rec = inner

        key_schema_fn = getattr(source_rec, "keySchema", None)
        if callable(key_schema_fn):
            ks = key_schema_fn()
            if ks is not None:
                fields = ks.fields()
                if fields:
                    return [str(f.name()) for f in fields]

        raw_key = source_rec.key()
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

        return cls._extract_json_pks(record)

    @classmethod
    def _unwrap_java_value(
        cls,
        val: Any,
        schema_def: Mapping[str, Any] | None = None,
        *,
        logical_name: str | None = None,
    ) -> Any:
        """Recursively normalizes Java Connect Structs and objects into native Python types at the JNI boundary."""
        if val is None:
            return None
        if hasattr(val, "booleanValue"):
            return bool(val.booleanValue())
        if isinstance(val, bool):
            return val
        if isinstance(val, int):
            return int(val)
        if isinstance(val, float):
            return float(val)
        if isinstance(val, str) or type(val).__name__ == "java.lang.String":
            return str(val)
        if isinstance(
            val,
            (
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
        if hasattr(val, "toPlainString"):
            return Decimal(str(val.toPlainString()))
        if hasattr(val, "toEpochDay"):
            return int(val.toEpochDay())
        if hasattr(val, "getTime"):
            ms = int(val.getTime())
            resolved_logical = (
                logical_name
                if logical_name is not None
                else (schema_def.get("name") if schema_def is not None else None)
            )
            if resolved_logical == "org.apache.kafka.connect.data.Date":
                return ms // 86_400_000
            return ms
        if type(val).__name__ == "byte[]":
            return bytes(b & 0xFF for b in val)
        if hasattr(val, "array"):
            return bytes(val.array())
        if hasattr(val, "get") and not isinstance(val, Mapping):
            getter = getattr(val, "getWithoutDefault", None) or val.get
            if schema_def is not None and isinstance(schema_def.get("fields"), list):
                return {
                    f_def["field"]: cls._unwrap_java_value(
                        getter(f_def["field"]), f_def
                    )
                    for f_def in schema_def["fields"]
                }
            if hasattr(val, "schema"):
                j_schema = val.schema()
                fields = j_schema.fields() if j_schema is not None else None
                if fields is not None:
                    result: dict[str, Any] = {}
                    for f in fields:
                        f_name = str(f.name())
                        sub_schema = f.schema()
                        sub_logical = (
                            str(sub_schema.name())
                            if (sub_schema is not None and sub_schema.name())
                            else None
                        )
                        result[f_name] = cls._unwrap_java_value(
                            getter(f_name), logical_name=sub_logical
                        )
                    return result
        if isinstance(val, Mapping):
            if schema_def is not None and isinstance(schema_def.get("fields"), list):
                field_map = {f["field"]: f for f in schema_def["fields"]}
                return {
                    str(k): cls._unwrap_java_value(v, field_map.get(str(k)))
                    for k, v in val.items()
                }
            val_schema = schema_def.get("values") if schema_def is not None else None
            return {
                str(k): cls._unwrap_java_value(
                    v, val_schema if isinstance(val_schema, Mapping) else None
                )
                for k, v in val.items()
            }
        if isinstance(val, Sequence):
            item_schema = schema_def.get("items") if schema_def is not None else None
            return [
                cls._unwrap_java_value(
                    x, item_schema if isinstance(item_schema, Mapping) else None
                )
                for x in val
            ]
        return val

    @classmethod
    def _java_schema_to_dict(cls, j_schema: Any) -> dict[str, Any]:
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
        schema_def["parameters"] = (
            {str(k): str(v) for k, v in params.items()} if params is not None else {}
        )

        if type_str == "struct":
            fields = j_schema.fields()
            if fields is None:
                raise ValueError("Java Connect STRUCT schema missing fields()")
            sub_fields: list[dict[str, Any]] = []
            for sub_f in fields:
                sub_def = cls._java_schema_to_dict(sub_f.schema())
                sub_def["field"] = str(sub_f.name())
                sub_fields.append(sub_def)
            schema_def["fields"] = sub_fields
        elif type_str == "array":
            val_schema = j_schema.valueSchema()
            if val_schema is None:
                raise ValueError("Java Connect ARRAY schema missing valueSchema()")
            schema_def["items"] = cls._java_schema_to_dict(val_schema)
        elif type_str == "map":
            key_schema = j_schema.keySchema()
            val_schema = j_schema.valueSchema()
            if key_schema is None or val_schema is None:
                raise ValueError(
                    "Java Connect MAP schema missing keySchema() or valueSchema()"
                )
            schema_def["keys"] = cls._java_schema_to_dict(key_schema)
            schema_def["values"] = cls._java_schema_to_dict(val_schema)

        return schema_def
