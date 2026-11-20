# Schema Subsystem Specification

This document is the authoritative specification for `pydbzengine.schema`. It defines the subsystem's scope, architecture, public surface, behavioral contracts, error categories, and engineering invariants.

---

## Scope

`pydbzengine.schema` transforms schema-carrying Debezium `ChangeEvent` streams (`EventFormat.CONNECT` or `EventFormat.JSON` with `schemas_enabled=True`) into a unified Apache Arrow representation (`TableSchema`, `TableBatch`, `ChangeRecord`, and `ChangeBatch`) and provides `SchemaAwareChangeHandler` for schema-driven CDC consumers. Consumers working with schemaless JSON payloads (`schemas_enabled=False`) should subclass `BasePythonChangeHandler` directly.

The subsystem is built around five foundational principles:
1. **Deterministic Setup & Java/Python Wire Parity**: `DebeziumConfig` freezes any `Mapping` (including Java `Properties`) into an immutable value object, resolves wire format (`EventFormat.CONNECT` vs. `EventFormat.JSON`), `schemas_enabled`, and `heartbeat_prefix` once with strict conflict checks, and emits canonical `to_engine_properties()` (including `converter.schemas.enable`) so the embedded Java engine and `EventConverter.from_config` always agree on the wire format.
2. **Low-Overhead Slotted Single-Pass Compilation**: Wire schemas (`FieldSpec` in `spec.py`) compile once per destination via `TypeRegistry` (`registry.py`) into cached, `__slots__`-backed trees of `SchemaField` and `DataTypeHandler` instances (`handlers.py`, `temporal.py`) with precomputed PyArrow types and field-path error context, converting JSON and Java Connect payloads in a single pass.
3. **Pure PyArrow Domain Models**: `TableSchema`, `SchemaDiff`, `ChangeRecord`, `TableBatch`, and `ChangeBatch` (`models.py`) are immutable domain entities decoupled from wire-conversion internals. `TableBatch` is the sole Apache Arrow Table materializer (`to_arrow()`).
4. **Strict Pass-Through Event Semantics**: Every `ChangeRecord` categorizes its payload via `EventKind` (`DATA`, `HEARTBEAT`, `SCHEMA_CHANGE`, `TOMBSTONE`) and passes converted values and keys straight through without implicit envelope unwrapping.
5. **JVM Boundary Isolation**: Importing or using `pydbzengine.schema` never loads `jpype` or starts a JVM; `pydbzengine.engine` never imports `pydbzengine.schema`.

---

## Public Surface & Usage

Consumer-facing contracts (`EventConverter`, `ConnectEventConverter`, `JsonEventConverter`, `EventKind`, `TableSchema`, `SchemaDiff`, `ChangeRecord`, `TableBatch`, `ChangeBatch`, `SchemaAwareChangeHandler`, and `EventFormat`) are re-exported directly from `pydbzengine.schema`. Compiler internals (`FieldSpec`, `SchemaField`, `DataTypeHandler`, `TypeRegistry`, and concrete type handlers) are importable from their dedicated submodules (`spec`, `handlers`, `temporal`, `registry`) for advanced extension.

### Event Converters (`converter.py`)
- **`EventConverter`**: Abstract base pipeline configured with `heartbeat_prefix` and `default_variable_scale`. Exposes `EventConverter.from_config(config, default_variable_scale=10)`, `compile_schema(destination, value_schema, key_schema=None) -> TableSchema`, `convert(events) -> ChangeBatch`, and `convert_event(event) -> ChangeRecord`. Fails fast when `config.schemas_enabled` is `False`, directing schemaless JSON consumers to `BasePythonChangeHandler`.
- **`ConnectEventConverter`** (`format = EventFormat.CONNECT`, `schemas_enabled = True`):
  - Consumes in-memory Java Connect `SourceRecord` events.
  - Normalizes empty-fields struct `keySchema` with null/empty struct key payloads to `(None, None)`.
  - Compiles Java value and key schemas once per destination (retaining strong references to Java schema objects and preserving active table caches across tombstones) and converts payloads via `DataTypeHandler.convert_connect`.
- **`JsonEventConverter`** (`format = EventFormat.JSON`, `schemas_enabled = True`):
  - Consumes serialized JSON records containing explicit `{"schema": ..., "payload": ...}` envelopes, decoding JSON floats with `parse_float=Decimal` to preserve full precision for `NUMERIC` decimals.
  - Compiles JSON value and key schema mappings once per destination (preserving active table caches across tombstones) and converts payloads via `DataTypeHandler.convert_json`.

### Schema Compilation & Data Type Handlers (`spec.py`, `handlers.py`, `temporal.py`, `registry.py`)
- **`FieldSpec`** (`spec.py`): Immutable, hashable normalized schema AST node parsed via `FieldSpec.from_json` or `FieldSpec.from_connect`. Validates non-empty `name` and normalized `type_name`, string `logical_name`/`doc`, and read-only sorted `parameters` (`MappingProxyType`), alongside nested `fields`, `items`, `keys`, and `values`.
- **`SchemaField`** (`handlers.py`): Slotted immutable compiled field pairing field metadata (`name`, `nullable`, `doc`, `logical_name`) with its `DataTypeHandler` and prebuilt `pa.Field` (encoding `doc` and `debezium.logical_type` in Arrow field metadata). Enforces `nullable=False` (`optional=False`) and prefixes field conversion failures with `Field '<name>': ...` for nested path diagnostics.
- **`DataTypeHandler`** slotted subclasses (`handlers.py` & `temporal.py`):
  - **`BooleanTypeHandler`**: Maps to `pa.bool_()`. Accepts only booleans (or Java `Boolean` in Connect mode).
  - **`IntTypeHandler`**: Maps to `pa.int8()`, `pa.int16()`, `pa.int32()`, or `pa.int64()`. Rejects booleans, floats, decimals (including Java `BigDecimal`), and integers (including Java `BigInteger`) exceeding the declared Arrow bit-width bounds.
  - **`FloatTypeHandler`**: Maps to `pa.float32()` or `pa.float64()`. Accepts floats, ints, and decimals; rejects booleans.
  - **`StringTypeHandler`**: Maps to `pa.string()` for UTF-8 strings and string-backed logical types (preserving `debezium.logical_type` metadata via `SchemaField`).
  - **`BinaryTypeHandler`**: Maps `bytes` and binary logical types (`io.debezium.data.Bits`) to `pa.binary()`. Decodes RFC-4648 Base64 strings in JSON mode, and `bytes`, `byte[]`, heap `ByteBuffer` (`hasArray()`), and direct or read-only `ByteBuffer` (bulk non-mutating copy via `duplicate()`) in Connect mode.
  - **`DecimalTypeHandler` & `VariableScaleDecimalHandler`**: Map to `pa.decimal128(precision, scale)` with validated bounds ($1 \le \text{precision} \le 38$, $0 \le \text{scale} \le \text{precision}$). When `VariableScaleDecimal` omits `scale`, defaults to $\min(\text{default\_variable\_scale}, \text{precision})$. Decode RFC-4648 Base64 two's-complement bytes (`decimal.format=BASE64`), numeric tokens (`int`, finite `float`, finite `Decimal` for `decimal.format=NUMERIC`), or non-empty Java `BigDecimal` / `ByteBuffer` values, quantizing with `ROUND_HALF_UP` and rejecting empty bytes or values whose scaled digits exceed `precision`.
  - **`DateTypeHandler`** (`temporal.py`): Maps epoch days, Java dates (`toEpochDay`, `toLocalDate` for timezone-safe `java.sql.Date`, `getTime` for UTC `java.util.Date`), and ISO-8601 date strings to `pa.date32()`. Rejects `java.sql.Time`, `java.sql.Timestamp`, out-of-bounds integers (`AtomicLong`/`BigInteger`), and non-integer numbers (`BigDecimal`, floats, booleans).
  - **`TimeTypeHandler`** (`temporal.py`): Maps `ms` to `pa.time32("ms")` and `us`/`ns` to `pa.time64(unit)`. Decodes integer time-of-day offsets (strictly validated within $[0, 86{,}400\text{s})$), Java time objects (`toNanoOfDay`, `toLocalTime` preserving sub-second milliseconds for `java.sql.Time` and normalizing `OffsetTime` to UTC, `getTime` for UTC `java.util.Date`), and ISO-8601 time strings (normalizing timezone offsets to UTC and preserving all 9 fractional digits when `unit="ns"`). Rejects `java.sql.Date`, `java.sql.Timestamp`, and non-integer numbers.
  - **`TimestampTypeHandler`** (`temporal.py`): Maps `ms`, `us`, and `ns` timestamps to `pa.timestamp(unit, tz)` (`tz="UTC"` for `ZonedTimestamp`, `tz=None` otherwise). Decodes int64-bounded integer epoch offsets, Java `Instant`/`Timestamp`/`Date`/`OffsetDateTime`/`ZonedDateTime` objects, and ISO-8601 strings (preserving nanosecond precision when `unit="ns"`). Rejects `java.sql.Date`, `java.sql.Time`, out-of-bounds integers, and non-integer numbers cleanly instead of invoking unsupported `toInstant()`.
  - **`StructTypeHandler`**, **`ArrayTypeHandler`**, **`MapTypeHandler`**: Compile nested Connect schemas directly from wire definitions (automatically handling `Point`, `Geometry`, `Geography`, `FloatVector`, `DoubleVector`, `SparseDoubleVector`, `Hstore`, and any nested structs/arrays/maps while preserving child nullability and `debezium.logical_type` metadata). `StructTypeHandler` prefers `getWithoutDefault` on Connect `Struct`; `MapTypeHandler` enforces non-null non-nested keys, coerces stringified JSON object keys to declared scalar types (raising `ValueError` on invalid int/float/bool string keys), and accepts `[key, value]` pair sequences in JSON mode.
- **`TypeRegistry`** (`registry.py`): Validates `default_variable_scale` ($0 \dots 38$) and compiles `FieldSpec` trees into `SchemaField` and `DataTypeHandler` trees via a unified `SEMANTIC_LOGICAL_HANDLERS: dict[str, tuple[str, DataTypeHandler]]` table for the 15 fixed semantic temporal logical types plus parameterized `Decimal` and `VariableScaleDecimal`, validating underlying Connect wire types strictly.

### Domain Models (`models.py`)
- **`EventKind`**: Enum with values `DATA`, `HEARTBEAT`, `SCHEMA_CHANGE`, and `TOMBSTONE`. `EventKind.classify(destination, field_names, heartbeat_prefix, schema_name=None)` classifies records deterministically (`None` payload $\rightarrow$ `TOMBSTONE`; `heartbeat_prefix` or `*.Heartbeat` schema $\rightarrow$ `HEARTBEAT`; `*.SchemaChangeValue` / `*.SchemaChangeKey` schema or `{"ddl", "databaseName", "tableChanges"}` subset $\rightarrow$ `SCHEMA_CHANGE`; otherwise `DATA`).
- **`TableSchema`**: Immutable descriptor holding validated non-empty `destination`, `namespace`, `table_name`, `arrow_schema`, `key_fields`, and optional `key_schema`. `diff(previous)` computes a `SchemaDiff` between two schemas.
- **`SchemaDiff`**: Immutable comparison result exposing `added_fields`, `removed_fields`, `changed_fields` (detecting both data type and nullability changes), `reordered`, `key_fields_changed`, `key_schema_changed`, `has_changes`, `is_additive`, and `is_compatible`:
  - `is_additive` is struct-aware: returns `True` when changes consist exclusively of nullable field additions at the top level or recursively inside struct fields (such as `before`/`after` in Debezium's default Envelope format), with no removals, reorderings, type/nullability mutations, or key changes.
  - `is_compatible` accepts nullable column additions (top-level and nested), reordering, nullability relaxation, and lossless Arrow type widenings (`int32` $\rightarrow$ `int64`, `float32` $\rightarrow$ `float64`, decimal precision/scale expansion, temporal unit refinement), while rejecting dropped fields, `nullable=False` additions (top-level or nested), `key_fields`/`key_schema` changes, nullability tightening, type narrowings, and lossy `int64`/`uint64`/`decimal` $\rightarrow$ floating promotions.
- **`ChangeRecord`**: Immutable converted event holding normalized `destination`, `value`, `key`, `schema`, `partition`, and `kind`. Enforces non-empty `destination`, non-empty `key` on tombstones, and strict kind/value invariants without mutating `kind` (`kind is EventKind.TOMBSTONE` requires `value is None` and non-empty `key`; all other kinds require `value is not None`).
- **`TableBatch`**: Immutable homogeneous run of consecutive `DATA` `ChangeRecord` items sharing a single `TableSchema` and destination. Exposes `destination`, `rows`, and `to_arrow(target_schema=None) -> pa.Table` (enforcing `nullable=False` recursively across top-level, struct, list, and map target fields with zero nulls for both existing and missing columns in `target_schema`).
- **`ChangeBatch`**: Immutable ordered collection of converted `ChangeRecord` items. Exposes `data_records`, `of_kind(kind)`, `destination_events()` (causal per-destination segmentation of `TableBatch` runs broken by `TableSchema` changes, `TOMBSTONE` deletes, or `SCHEMA_CHANGE` barriers, followed by `HEARTBEAT` events), and `table_batches()`.

### Schema-Aware Consumer Handler (`handler.py`)
- **`SchemaAwareChangeHandler`**: Subclasses `BasePythonChangeHandler`.
  - Supports embedded engine auto-configuration (`DebeziumEngine` calls `configure(config)`) as well as standalone initialization via optional `converter` or `config` keyword arguments, validating `default_variable_scale` eagerly on construction.
  - `handle_batch(records)` converts incoming engine batches into a `ChangeBatch` and delegates to `handle_change_batch(batch)`.
  - `handle_change_batch(batch)` iterates over `batch.destination_events()` in causal order: for `TableBatch` items it invokes `on_table_created(schema)` on first observation or `on_schema_evolved(new_schema, old_schema, diff)` when the schema changes, commits the tracked `TableSchema` in `schemas` only after the lifecycle hook succeeds without raising, and dispatches `handle_table_batch(table_batch)`; for `TOMBSTONE`, `SCHEMA_CHANGE`, and `HEARTBEAT` records it dispatches `on_tombstone(record)`, `on_schema_change(record)`, and `on_heartbeat(record)`.

---

## Constraints & Invariants

1. **Fail-Fast & Deterministic**: Wire format and schema mode are fixed at converter instantiation. Schemaless configs (`schemas_enabled=False`), conflicting configuration properties, malformed schemas, missing required `schema`/`payload` envelope blocks, keyless tombstones, mismatched `kind`/`value`, non-nullable target violations, non-finite decimals, integer/decimal precision overflows, null map keys, or incompatible wire types raise `ValueError` or `TypeError` immediately.
2. **No Fallback Cascades or Silent Coercion**: Never guess formats at runtime, swallow exceptions, mutate enum invariants silently, or substitute sentinel defaults on invalid state.
3. **Single-Pass Value Conversion**: Traverse the compiled `DataTypeHandler` tree in a single pass for both JSON and Connect payloads without intermediate untyped dictionary unwrapping.
4. **Tombstone-Safe Schema Caching**: Cache compiled value and key schemas per destination while retaining strong references to underlying Java schema objects; tombstone events never evict active table value schemas.
5. **Atomic Consumer Schema State**: `SchemaAwareChangeHandler` updates its tracked destination schema only after `on_table_created` or `on_schema_evolved` returns cleanly, preserving retry safety on downstream catalog errors.
6. **Strict Module Boundaries**: `pydbzengine.schema.models` depends only on `pyarrow`; `pydbzengine.schema.spec` has no `pyarrow` or JVM dependencies; `pydbzengine.schema` never imports `jpype`, `pyiceberg`, `dlt`, or `airflow`; `pydbzengine.engine` never imports `pydbzengine.schema`.

---

## Errors

| Error Category | Exception | Trigger Conditions |
| :--- | :--- | :--- |
| **Unconfigured Handler** | `RuntimeError` | Calling `SchemaAwareChangeHandler.handle_batch` before providing a `converter`, `config`, or calling `configure(config)`. |
| **Invalid Type / Wire Payload** | `TypeError` | Passing a non-mapping to `DebeziumConfig`, a non-matching Python or Java Connect runtime type (including `BigDecimal` to int/temporal handlers, `java.sql.Date`/`Time` to timestamp handler) to a `DataTypeHandler`, `EventConverter`, or `SchemaAwareChangeHandler`. |
| **Malformed Schema, Config, or Value** | `ValueError` | Passing `schemas_enabled=False` to `EventConverter.from_config`, conflicting format/schema/heartbeat settings (including `converter.schemas.enable=false` with `EventFormat.CONNECT`), empty destination, keyless tombstone, mismatched `ChangeRecord` `kind`/`value`, null value for non-nullable field or `target_schema` column, missing JSON `schema`/`payload` block, unsupported Connect or mismatched logical wire type, invalid ISO temporal string, out-of-bounds integer or time offset, invalid Base64 binary/decimal, invalid bool/int/float JSON map key, or Decimal precision/scale out of bounds. |
