# Schema Subsystem Specification

This document is the authoritative specification for `pydbzengine.schema`. It defines the subsystem's scope, architecture, public surface, behavioral contracts, error categories, and engineering invariants.

---

## Scope

`pydbzengine.schema` transforms raw Debezium `ChangeEvent` streams into a unified Apache Arrow representation (`TableSchema`, `TableBatch`, `ChangeRecord`, and `ChangeBatch`) and provides `SchemaAwareChangeHandler` for schema-driven CDC consumers.

The subsystem is built around five foundational principles:
1. **Deterministic Setup & Java/Python Wire Parity**: `DebeziumConfig` freezes any `Mapping` (including Java `Properties`) into an immutable value object, resolves wire format (`EventFormat.CONNECT` vs. `EventFormat.JSON`), `schemas_enabled`, and `heartbeat_prefix` once with strict conflict checks, and emits canonical `to_engine_properties()` (including `converter.schemas.enable`) so the embedded Java engine and `EventConverter.from_config` always agree on the wire format.
2. **Low-Overhead Slotted Single-Pass Compilation**: Wire schemas (`FieldSpec`) compile once per destination into cached, `__slots__`-backed trees of `SchemaField` and `DataTypeHandler` instances with precomputed PyArrow types and field-path error context, converting JSON and Java Connect payloads in a single pass.
3. **Pure PyArrow Domain Models**: `TableSchema`, `SchemaDiff`, `ChangeRecord`, `TableBatch`, and `ChangeBatch` are immutable domain entities decoupled from wire-conversion internals. `TableBatch` is the sole Apache Arrow Table materializer (`to_arrow()`).
4. **Minimalist Enum-Driven Event Semantics**: Every `ChangeRecord` categorizes its payload via `EventKind` (`DATA`, `HEARTBEAT`, `SCHEMA_CHANGE`, `TOMBSTONE`) and passes converted values and keys straight through without envelope unwrapping.
5. **JVM Boundary Isolation**: Importing or using `pydbzengine.schema` never loads `jpype` or starts a JVM; `pydbzengine.engine` never imports `pydbzengine.schema`.

---

## Public Surface & Usage

All primary contracts (`EventConverter`, `ConnectEventConverter`, `JsonEventConverter`, `SchemalessJsonEventConverter`, `FieldSpec`, `SchemaField`, `DataTypeHandler`, `TypeRegistry`, `EventKind`, `TableSchema`, `SchemaDiff`, `ChangeRecord`, `TableBatch`, `ChangeBatch`, `SchemaAwareChangeHandler`, and `EventFormat`) are re-exported directly from `pydbzengine.schema`.

### Event Converters
- **`EventConverter`**: Abstract base class configured with `heartbeat_prefix`. Exposes `EventConverter.from_config(config, default_variable_scale=10)`, `convert(events) -> ChangeBatch`, and `convert_event(event) -> ChangeRecord`.
- **`ConnectEventConverter`** (`format = EventFormat.CONNECT`, `schemas_enabled = True`):
  - Consumes in-memory Java Connect `SourceRecord` events.
  - Compiles Java value and key schemas once per destination (retaining strong references to Java schema objects and preserving active table caches across tombstones) and converts payloads via `DataTypeHandler.convert_connect`.
- **`JsonEventConverter`** (`format = EventFormat.JSON`, `schemas_enabled = True`):
  - Consumes serialized JSON records containing explicit `{"schema": ..., "payload": ...}` envelopes.
  - Compiles JSON value and key schema mappings once per destination (preserving active table caches across tombstones) and converts payloads via `DataTypeHandler.convert_json`.
- **`SchemalessJsonEventConverter`** (`format = EventFormat.JSON`, `schemas_enabled = False`):
  - Consumes raw JSON object payloads without envelope wrappers.
  - Infers `TableSchema` strictly for `DATA` records per destination via `pa.Table.from_pylist` and merges schemas deterministically per field (recursing into nested structs, lists, and maps) across batches so sparse or nullable columns preserve their known Arrow types even if another column changes type.

### Schema Compilation & Data Type Handlers
- **`FieldSpec`**: Immutable, hashable normalized schema AST node parsed via `FieldSpec.from_json` or `FieldSpec.from_connect`. Validates non-empty `name` and normalized `type_name`, string `logical_name`/`doc`, and read-only sorted `parameters` (`MappingProxyType`), alongside nested `fields`, `items`, `keys`, and `values`.
- **`SchemaField`**: Slotted immutable compiled field pairing field metadata (`name`, `nullable`, `doc`, `logical_name`) with its `DataTypeHandler` and prebuilt `pa.Field` (encoding `doc` and `debezium.logical_type` in Arrow field metadata). Enforces `nullable=False` (`optional=False`) and prefixes field conversion failures with `Field '<name>': ...` for nested path diagnostics.
- **`DataTypeHandler`** slotted subclasses:
  - **`BooleanTypeHandler`**: Maps to `pa.bool_()`. Accepts only booleans (or Java `Boolean` in Connect mode).
  - **`IntTypeHandler`**: Maps to `pa.int8()`, `pa.int16()`, `pa.int32()`, or `pa.int64()` (including Debezium `Year`, `MicroDuration`, `NanoDuration`, `IntervalYearToMonth`, and `IntervalDayToSecond`). Rejects booleans, floats, and integers exceeding the declared Arrow bit-width bounds.
  - **`FloatTypeHandler`**: Maps to `pa.float32()` or `pa.float64()`. Accepts floats and ints; rejects booleans.
  - **`StringTypeHandler`**: Maps to `pa.string()` for UTF-8 strings and string-backed logical types (`Uuid`, `Json`, `Xml`, `Enum`, `EnumSet`, `Ltree`, `ObjectId`, `Tsvector`, `Interval`).
  - **`BinaryTypeHandler`**: Maps `bytes` and `io.debezium.data.Bits` to `pa.binary()`. Decodes RFC-4648 Base64 strings in JSON mode, and `bytes`, `byte[]`, or positioned Java `ByteBuffer` instances in Connect mode.
  - **`DecimalTypeHandler` & `VariableScaleDecimalHandler`**: Map to `pa.decimal128(precision, scale)` with validated bounds ($1 \le \text{precision} \le 38$, $0 \le \text{scale} \le \text{precision}$). When `VariableScaleDecimal` omits `scale`, defaults to $\min(\text{default\_variable\_scale}, \text{precision})$. Decode RFC-4648 Base64 two's-complement bytes (`decimal.format=BASE64`), numeric tokens (`int`, finite `float`, finite `Decimal` for `decimal.format=NUMERIC`), or Java `BigDecimal` values, quantizing with `ROUND_HALF_UP` and rejecting values whose scaled digits exceed `precision`.
  - **`DateTypeHandler`**: Maps epoch days, Java dates (`toEpochDay`, `getTime`), and ISO-8601 date strings to `pa.date32()`.
  - **`TimeTypeHandler`**: Maps `ms` to `pa.time32("ms")` and `us`/`ns` to `pa.time64(unit)`. Decodes integer time-of-day offsets (strictly validated within $[0, 86{,}400\text{s})$), Java time objects (`toNanoOfDay`, `getTime`), and ISO-8601 time strings (normalizing timezone offsets to UTC and preserving all 9 fractional digits when `unit="ns"`).
  - **`TimestampTypeHandler`**: Maps `ms`, `us`, and `ns` timestamps to `pa.timestamp(unit, tz)` (`tz="UTC"` for `ZonedTimestamp`, `tz=None` otherwise). Decodes integer epoch offsets, Java `Instant`/`Timestamp`/`Date` objects, and ISO-8601 strings (preserving nanosecond precision when `unit="ns"`).
  - **`StructTypeHandler`**: Maps nested structs and fixed logical structs (`Geometry`, `Geography`, `Point`, `SparseDoubleVector`) to prebuilt `pa.struct(...)` and `pa.Schema` instances with Debezium-aligned child nullability. Resolves `getWithoutDefault` once per Connect `Struct` so schema defaults are not substituted for `null` wire values.
  - **`ArrayTypeHandler`**: Maps arrays and dense vectors (`FloatVector`, `DoubleVector`) to `pa.list_(item_field.arrow_field)`, preserving element nullability and field metadata.
  - **`MapTypeHandler`**: Maps typed key-value maps and PostgreSQL `Hstore` fields to `pa.map_(key_field, value_field)`, preserving key/value field metadata and value nullability. Enforces non-null non-nested keys, coerces stringified JSON object keys to declared scalar types, and accepts `[key, value]` pair sequences in JSON mode.
- **`TypeRegistry`**: Validates `default_variable_scale` ($0 \dots 38$) and underlying Connect wire primitive types (`LOGICAL_WIRE_TYPES`), compiling `FieldSpec` trees into `SchemaField` and `DataTypeHandler` trees and matching fixed logical types before generic `struct`, `array`, or `map` compilation.

### Domain Models
- **`EventKind`**: Enum with values `DATA`, `HEARTBEAT`, `SCHEMA_CHANGE`, and `TOMBSTONE`. `EventKind.classify(destination, field_names, heartbeat_prefix, schema_name=None)` classifies records deterministically (`None` payload $\rightarrow$ `TOMBSTONE`; `heartbeat_prefix` or `*.Heartbeat` schema $\rightarrow$ `HEARTBEAT`; `*.SchemaChangeValue` / `*.SchemaChangeKey` schema or `{"ddl", "databaseName", "tableChanges"}` subset $\rightarrow$ `SCHEMA_CHANGE`; otherwise `DATA`).
- **`TableSchema`**: Immutable descriptor holding validated non-empty `destination`, `namespace`, `table_name`, `arrow_schema`, `primary_keys`, and optional `key_schema`. `diff(previous)` computes a `SchemaDiff` between two schemas.
- **`SchemaDiff`**: Immutable comparison result exposing `added_fields`, `removed_fields`, `changed_fields` (detecting both data type and nullability changes), `reordered`, `primary_keys_changed`, `has_changes`, `is_additive`, and `is_compatible` (accepting column additions, reordering, nullability relaxation, and lossless Arrow type widenings such as `int32` $\rightarrow$ `int64`, `float32` $\rightarrow$ `float64`, decimal precision/scale expansion, and temporal unit refinement, while rejecting dropped fields, primary-key changes, nullability tightening, type narrowings, and lossy `int64` $\rightarrow$ `float64` promotions).
- **`ChangeRecord`**: Immutable converted event holding normalized `destination`, `value`, `key`, `schema`, `partition`, and `kind`. Enforces non-empty `destination`, non-empty `key` on tombstones, and strict kind/value invariants (`value is None` $\iff$ `kind is EventKind.TOMBSTONE`).
- **`TableBatch`**: Immutable homogeneous run of consecutive `DATA` `ChangeRecord` items sharing a single `TableSchema` and destination. Exposes `destination`, `rows`, and `to_arrow(target_schema=None) -> pa.Table`.
- **`ChangeBatch`**: Immutable ordered collection of converted `ChangeRecord` items. Exposes `data_records`, `of_kind(kind)`, `destination_events()` (causal per-destination segmentation of `TableBatch` runs broken by `TableSchema` changes, `TOMBSTONE` deletes, or `SCHEMA_CHANGE` barriers, followed by `HEARTBEAT` events), and `table_batches()`.

### Schema-Aware Consumer Handler
- **`SchemaAwareChangeHandler`**: Subclasses `BasePythonChangeHandler`.
  - Supports embedded engine auto-configuration (`DebeziumEngine` calls `configure(config)`) as well as one-line standalone initialization via optional `converter` or `config` keyword arguments, validating `default_variable_scale` eagerly on construction.
  - `handle_batch(records)` converts incoming engine batches into a `ChangeBatch` and delegates to `handle_change_batch(batch)`.
  - `handle_change_batch(batch)` iterates over `batch.destination_events()` in causal order: for `TableBatch` items it invokes `on_table_created(schema)` on first observation or `on_schema_evolved(new_schema, old_schema, diff)` when the schema changes, commits the tracked `TableSchema` in `schemas` only after the lifecycle hook succeeds without raising, and dispatches `handle_table_batch(table_batch)`; for `TOMBSTONE`, `SCHEMA_CHANGE`, and `HEARTBEAT` records it dispatches `on_tombstone(record)`, `on_schema_change(record)`, and `on_heartbeat(record)`.

---

## Constraints & Invariants

1. **Fail-Fast & Deterministic**: Wire format and schema mode are fixed at converter instantiation. Conflicting configuration properties, malformed schemas, missing required `schema`/`payload` envelope blocks, keyless tombstones, non-finite decimals, integer/decimal precision overflows, null map keys, or incompatible wire types raise `ValueError` or `TypeError` immediately.
2. **No Fallback Cascades or Silent Coercion**: Never guess formats at runtime, swallow exceptions, or substitute sentinel defaults on invalid state.
3. **Single-Pass Value Conversion**: Traverse the compiled `DataTypeHandler` tree in a single pass for both JSON and Connect payloads without intermediate untyped dictionary unwrapping.
4. **Tombstone-Safe Schema Caching**: Cache compiled value and key schemas per destination while retaining strong references to underlying Java schema objects; tombstone events never evict active table value schemas.
5. **Atomic Consumer Schema State**: `SchemaAwareChangeHandler` updates its tracked destination schema only after `on_table_created` or `on_schema_evolved` returns cleanly, preserving retry safety on downstream catalog errors.
6. **Strict Module Boundaries**: `pydbzengine.schema.models` depends only on `pyarrow`; `pydbzengine.schema` never imports `jpype`, `pyiceberg`, `dlt`, or `airflow`; `pydbzengine.engine` never imports `pydbzengine.schema`.

---

## Errors

| Error Category | Exception | Trigger Conditions |
| :--- | :--- | :--- |
| **Unconfigured Handler** | `RuntimeError` | Calling `SchemaAwareChangeHandler.handle_batch` before providing a `converter`, `config`, or calling `configure(config)`. |
| **Invalid Type / Wire Payload** | `TypeError` | Passing a non-mapping to `DebeziumConfig`, a non-matching Python or Java Connect runtime type to a `DataTypeHandler`, `EventConverter`, or `SchemaAwareChangeHandler`. |
| **Malformed Schema, Config, or Value** | `ValueError` | Conflicting format/schema/heartbeat settings, empty destination, keyless tombstone, null value for non-nullable field, missing JSON `schema`/`payload` block, unsupported Connect or mismatched logical wire type, invalid ISO temporal string, out-of-bounds integer or time offset, invalid Base64 binary/decimal, or Decimal precision/scale out of bounds. |
