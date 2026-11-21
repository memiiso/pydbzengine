# Schema Subsystem Specification

This document is the authoritative specification for `pydbzengine.schema`. It defines the subsystem's scope, architecture, public surface, behavioral contracts, error categories, and engineering invariants.

---

## Scope

`pydbzengine.schema` is an optional Apache Arrow subsystem (`pip install pydbzengine[arrow]`) that transforms schema-carrying Debezium `ChangeEvent` streams (`EventFormat.CONNECT` or `EventFormat.JSON` with `schemas_enabled=True`) into unified Apache Arrow representations (`TableSchema`, `TableBatch`, `ChangeRecord`, and `ChangeBatch`) and provides `SchemaAwareChangeHandler` for schema-driven CDC consumers. Consumers working with schemaless JSON payloads (`schemas_enabled=False`) should subclass `BasePythonChangeHandler` directly.

The subsystem is built around five foundational principles:
1. **Deterministic Setup & Java/Python Wire Parity**: `DebeziumConfig` freezes any `Mapping` (including Java `Properties`) into an immutable value object, resolves wire format (`EventFormat.CONNECT` vs. `EventFormat.JSON`), `schemas_enabled` (`value_schemas_enabled` / `key_schemas_enabled`), and `heartbeat_prefix` with strict conflict checks, and emits normalized `to_engine_properties()` without altering unrelated converter settings for raw `BasePythonChangeHandler` consumers. `EventConverter.from_config` enforces symmetric JSON/Connect schema requirements strictly for `pydbzengine.schema`.
2. **Clean OOP Format Split & Single-Pass Compilation**: Abstract `EventConverter` (`converter.py`) owns the shared compilation pipeline, destination caching, key resolution, tombstone handling, and `ChangeBatch` assembly via Template Method. Pure-Python Arrow type mapping and shared value normalizers live in `types.py` (with zero JSON, Base64, or JVM imports). Wire-specific parsing and decoding are strictly isolated into `connect.py` (`ConnectEventConverter`) and `json.py` (`JsonEventConverter`), compiling schemas directly into `pa.Schema` and lightweight single-format decoder closures without an intermediate AST tree or dual-format handler hierarchy.
3. **Pure PyArrow Domain Models & Ecosystem Interop**: `TableSchema`, `SchemaDiff`, `ChangeRecord`, `TableBatch`, and `ChangeBatch` (`models.py`) are immutable domain entities decoupled from wire-conversion internals. `TableBatch` materializes `pa.Table` via `to_arrow()` and implements the Arrow PyCapsule Interface (`__arrow_c_stream__`) for zero-glue interop with DuckDB, Polars, PyIceberg, and DataFusion.
4. **Strict Pass-Through Event Semantics**: Every `ChangeRecord` categorizes its payload via `EventKind` (`DATA`, `HEARTBEAT`, `SCHEMA_CHANGE`, `TOMBSTONE`) and passes converted values and keys straight through without implicit envelope unwrapping.
5. **Strict Module & Packaging Isolation**: `pyarrow>=19.0.0` is an optional extra (`pydbzengine[arrow]`). Importing `pydbzengine.engine` never imports `pydbzengine.schema` or `pyarrow`; importing `pydbzengine.schema` never loads `jpype`, `pyiceberg`, `dlt`, or `airflow`.

---

## Module Architecture & Public Surface

Consumer-facing contracts (`EventConverter`, `ConnectEventConverter`, `JsonEventConverter`, `EventKind`, `TableSchema`, `SchemaDiff`, `ChangeRecord`, `TableBatch`, `ChangeBatch`, `SchemaAwareChangeHandler`, and `EventFormat`) are re-exported directly from `pydbzengine.schema`.

| Module | Responsibility |
| :--- | :--- |
| `pydbzengine.schema.models` | Pure Arrow domain entities (`EventKind`, `TableSchema`, `SchemaDiff`, `ChangeRecord`, `TableBatch`, `ChangeBatch`), composable sub-batch slicing (`of_kind`, `group_by_kind`, `group_by_destination`, non-fragmenting `to_table_batches`), and Arrow PyCapsule interop (`__arrow_c_stream__`). |
| `pydbzengine.schema.types` | Shared Arrow type mapping (`PRIMITIVE_ARROW_TYPES`, `TEMPORAL_LOGICAL_TYPES`, `Decimal`, `VariableScaleDecimal`) and pure-Python value normalizers (decimal quantization, ISO-8601 date/time/timestamp parsing, int/time bounds). Zero JSON, Base64, or JVM imports. |
| `pydbzengine.schema.converter` | Abstract `EventConverter` base class owning one-pass schema compilation, per-destination caching, key compilation, tombstone resolution, and `ChangeBatch` construction. |
| `pydbzengine.schema.connect` | `ConnectEventConverter` and Connect-only Java `SourceRecord`/`Schema`/`Struct`/`ByteBuffer`/`BigDecimal`/`java.util.Date` decoders. Zero JSON or Base64 imports. |
| `pydbzengine.schema.json` | `JsonEventConverter` and JSON-only envelope parsing (`parse_float=Decimal`), Base64 decoding, numeric decimals, stringified map key coercion, and `[key, value]` map arrays. |
| `pydbzengine.schema.consumer` | `SchemaAwareChangeHandler` subclassing `BasePythonChangeHandler` with batch-first dispatch (`handle_change_batch`), atomic schema reconciliation (`reconcile_schema`), and table lifecycle hooks (`on_table_created`, `on_schema_evolved`, `handle_table_batch`). |

### Supported Data Types (`types.py`, `connect.py`, `json.py`)
- **Primitives**: `boolean` $\rightarrow$ `pa.bool_()`, `int8`/`int16`/`int32`/`int64` $\rightarrow$ matching signed Arrow integer with bit-width bounds validation, `float`/`float32` $\rightarrow$ `pa.float32()`, `double`/`float64` $\rightarrow$ `pa.float64()`, `string` $\rightarrow$ `pa.string()`, `bytes` (and `io.debezium.data.Bits`) $\rightarrow$ `pa.binary()`.
- **Decimals**: `org.apache.kafka.connect.data.Decimal` and `io.debezium.data.VariableScaleDecimal` $\rightarrow$ `pa.decimal128(precision, scale)` with validated bounds ($1 \le \text{precision} \le 38$, $0 \le \text{scale} \le \text{precision}$), `ROUND_HALF_UP` quantization, and precision overflow detection.
- **Temporals**: All 15 Debezium & Kafka Connect date, time (`ms`/`us`/`ns`), and timestamp (`ms`/`us`/`ns`, naive or `tz="UTC"` for `ZonedTimestamp`) logical types, strictly validating underlying Connect wire types and bounds, normalizing timezone offsets to UTC, and preserving nanosecond precision for `ns` units.
- **Containers**: `struct`, `array`, and `map` (automatically handling `Point`, `Geometry`, `Geography`, `FloatVector`, `DoubleVector`, `SparseDoubleVector`, `Hstore`) preserving child nullability, `doc`, and `debezium.logical_type` Arrow field metadata.

---

## Constraints & Invariants

1. **Fail-Fast & Deterministic**: Wire format and schema mode are fixed at converter instantiation. Schemaless configs (`schemas_enabled=False`), conflicting configuration properties, malformed schemas, missing required `schema`/`payload` envelope blocks, keyless tombstones, mismatched `kind`/`value`, non-nullable target violations, non-finite decimals, integer/decimal precision overflows, null map keys, or incompatible wire types raise `ValueError` or `TypeError` immediately.
2. **No Fallback Cascades or Silent Coercion**: Never guess formats at runtime, swallow exceptions, mutate enum invariants silently, or substitute sentinel defaults on invalid state.
3. **Single-Pass Value Conversion**: Traverse compiled format-specific field decoders in a single pass with one `try/except` frame per struct preserving nested `Field '<name>': ...` diagnostics.
4. **Tombstone-Safe Schema Caching**: Cache compiled value and key schemas per destination while retaining strong references to underlying Java schema objects; tombstone events never evict active table value schemas.
5. **Atomic Consumer Schema State**: `SchemaAwareChangeHandler` updates its tracked destination schema only after `on_table_created` or `on_schema_evolved` returns cleanly, preserving retry safety on downstream catalog errors.
6. **Strict Module Boundaries**: `types.py` and `models.py` have zero `json`, `base64`, or `jpype` imports; `connect.py` has zero `json`, `base64`, or `jpype` imports; `pydbzengine.schema` never imports `jpype`, `pyiceberg`, `dlt`, or `airflow`; `pydbzengine.engine` never imports `pydbzengine.schema` or `pyarrow`.

---

## Errors

| Error Category | Exception | Trigger Conditions |
| :--- | :--- | :--- |
| **Missing Extra** | `ImportError` | Importing `pydbzengine.schema` when `pyarrow` is not installed (guides users to `pip install pydbzengine[arrow]`). |
| **Unconfigured Handler** | `RuntimeError` | Calling `SchemaAwareChangeHandler.handle_batch` before providing a `converter`, `config`, or calling `configure(config)`. |
| **Invalid Type / Wire Payload** | `TypeError` | Passing a non-mapping to `DebeziumConfig`, a non-`EventKind` to `ChangeBatch.of_kind`, or a non-matching Python/Connect runtime type to a converter or `SchemaAwareChangeHandler`. |
| **Malformed Schema, Config, or Value** | `ValueError` | Passing `schemas_enabled=False` to `EventConverter.from_config`, conflicting format/schema/heartbeat settings, empty destination, keyless tombstone, mismatched `ChangeRecord` `kind`/`value`, null value for non-nullable field or `target_schema` column, missing JSON `schema`/`payload` block, unsupported Connect or mismatched logical wire type, invalid ISO temporal string, out-of-bounds integer or time offset, invalid Base64 binary/decimal, invalid bool/int/float JSON map key, or Decimal precision/scale out of bounds. |
