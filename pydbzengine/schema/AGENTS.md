# Schema Subsystem Specification & Agent Directives

This document is the authoritative specification, domain guide, and agent directive for the `pydbzengine.schema` subsystem. It defines the core responsibilities, user requirements, type mappings, and architectural invariants.

---

## 1. Domain Purpose & Philosophy: Pure Technical Conversion

The `pydbzengine.schema` subsystem provides a technical **Event Converter** (historically named `SchemaConverter` in the codebase). It is NOT an ETL or data-transformation pipeline.

Despite the historical name, it is a full-featured **Event / Record Converter** responsible for both:
1. **The Schema Plane**: Extracting schema definitions from Change Data Capture (CDC) events (Kafka Connect schemas or JSON schemas) and compiling them into analytical schemas (such as Apache Arrow table schemas).
2. **The Data Plane**: Extracting and coercing wire values into native Python and analytical columnar representations with strict type safety.
3. **Consumer Convenience Functionalities**: Providing direct helper functionalities (e.g. checking for heartbeats, schema-change events, and tombstones) directly as methods or functions—without creating separate utility classes or making unilateral decisions to alter, rewrite, or drop data.

### Core Principles & Boundaries

- **No Business Logic or Data Processing**:
  The `pydbzengine.schema` subsystem does not perform business transformations or ETL logic.
  - It does NOT unwrap, strip, or swap CDC envelopes (`after`/`before`).
  - It does NOT synthesize deleted rows or inject artificial columns (such as `__deleted`, `__source`, etc.).
  - It does NOT alter or enrich CDC event content produced by Debezium.
  - Its data output is strictly limited to **faithful technical type coercion** (e.g., decoding Base64 strings to decimals/bytes, temporal conversions, and JNI unwrapping) and optional caller-specified schema alignment (`target_schema`).

- **Uniform Event Mapping**:
  Whatever structure the CDC source emits—whether a full CDC envelope, a pre-flattened event, an insert, an update, or a delete—is converted faithfully and identically according to its declared schema.

- **Consumer Ownership**:
  Downstream consumers (such as lakehouse sinks, database handlers, or streaming pipelines) own data lifecycle policies (such as merge keys, soft vs hard deletes, and table upserts). The schema subsystem delivers faithfully typed data and schema definitions with zero overhead.

---

## 2. Core User Requirements

1. **Dual-Format Ingestion**:
   - **JSON Mode (`format="json"`, default)**: Ingests JSON-serialized events (with embedded schemas by default). Must also support data-only payloads when schema embedding is disabled (Schemaless JSON mode).
   - **Connect Mode (`format="connect"`)**: Ingests in-memory Kafka Connect change events across the Java Native Interface (JNI), avoiding JVM string serialization and Base64 encoding.

2. **Consumer Convenience Functionalities (Non-Intrusive, No Separate Util Classes)**:
   - Provide direct helper functionalities (such as checking `is_heartbeat()`, `is_schema_change()`, `is_tombstone()`) directly on the converter or module rather than bloated utility classes.
   - **Heartbeat Identification**: Expose standard heartbeat markers and predicate checks so consumers can easily identify heartbeat events to track offsets or skip them. The converter does not swallow them.
   - **DDL History Identification**: Expose markers and predicate checks so consumers can distinguish schema-history records from data records.
   - **Tombstone Detection**: Expose predicate checks to cleanly identify empty delete tombstones (null payload).

3. **High-Fidelity Analytical Type Compilation**:
   - Translates all standard Connect primitive types (`int8`..`int64`, `float32`, `float64`, `boolean`, `string`, `bytes`) into exact analytical columnar types.
   - Translates all database logical types across PostgreSQL, MySQL, SQL Server, and Oracle:
     - **Decimals**: Fixed-scale decimals and variable-scale decimals.
     - **Temporals**: Dates, times (millisecond, microsecond, nanosecond precision), timestamps (UTC, zoned, naive), and ISO-8601 formatted temporal strings.
     - **Binaries & Enums**: Bit sets, enums, UUIDs, JSON/JSONB text, XML, and key-value maps.
     - **Complex Types**: Structs, lists, and maps mapped to corresponding nested analytical types.

4. **Deterministic Init-Time Configuration (`ConverterConfig`)**:
   - The converter resolves its format and schema strategy **once during `__init__`** via `ConverterConfig` (aliased as `DebeziumConfig`), eliminating runtime format guessing and record-level sniffing in hot loops.
   - `ConverterConfig` inspects connector properties upfront to determine:
     - The event format (`EventFormat.JSON` vs `EventFormat.CONNECT`).
     - Whether schemas are enabled (`schemas.enable=true` vs `false`).
     - Upstream connector structure (e.g. if Debezium SMT flattening is configured).

5. **Batch Performance & Mid-Batch Schema Evolution**:
   - **O(1) Schema Change Detection**: Inspect each event via lightweight identity checks (e.g. pointer comparison in Connect mode or schema signature in JSON mode), avoiding full schema re-compilation unless a schema change actually occurs.
   - **Handling Mid-Batch Schema Evolution**: When a single batch contains events before and after a DDL change (e.g. TableX gains an `email` column mid-batch):
     - **Separate Batches by Schema (Recommended for Sinks)**: Slice the batch at the schema boundary and process distinct tables per schema version. This preserves exact database commit log boundaries and prevents type-conflict failures when columns are dropped or types are altered.
     - **Unified Table (`pa.unify_schemas`)**: If a single unified table is requested across purely additive schema changes, unify the schemas using `pa.unify_schemas()`, where earlier rows automatically receive typed `null`s for newly added columns.
   - **Schemaless Batch Inference**: In Schemaless JSON mode (`schemas_enabled=False`), schemas are inferred across the entire batch natively in C++ via `pa.Table.from_pylist(rows)` to eliminate single-row type ambiguity (e.g., initial null values).

---

## 3. Conceptual Data Flows & Behavioral Contracts

This section defines the input, processing behavior, and output contracts of the Event Converter subsystem without prescribing implementation syntax.

### Contract 1: Converter Initialization
- **Inputs**: Connector configuration properties (dictionary or configuration model) declaring the event format (`json` vs `connect`) and schema mode (`schemas_enabled`).
- **Processing**:
  - Evaluates configuration properties once at initialization time.
  - Binds the appropriate wire-decoding strategy and JNI boundary interop strategy.
  - Eliminates dynamic reflection and per-record runtime format guessing.
- **Output**: An initialized Event Converter ready to process homogeneous batches or streaming records.

### Contract 2: Event Materialization (Single & Batch Uniformity)
- **Inputs**: A single change event or a sequence of raw change events for a table destination.
- **Processing**:
  - Treats a single event and a batch of events through the exact same processing pipeline.
  - Allows consumers to check and filter or route special events (heartbeats, DDL history events, tombstones) using exposed predicate checks.
  - Inspects event schema identity to ensure schema stability across the batch.
  - Decodes wire values (e.g., unscaling Base64 decimals, parsing temporals, unwrapping JNI memory buffers).
  - Materializes the compiled analytical schema and coerced row values into an analytical columnar structure (such as an Apache Arrow `Table`).
  - Supports optional caller-specified schema alignment (projecting columns, filling missing target columns with `null`, and casting compatible types).
- **Output**: 
  - A materialized analytical Table, containing both the schema (`table.schema`) and the data:
    - Single event: A Table containing 1 row (`num_rows == 1`).
    - Batch of events: A Table containing N rows (`num_rows == N`).
  - Returns `None` if the input contained no data records.
- **Python Dictionary Access**: Consumers wanting Python row dictionaries use native `table.to_pylist()`, avoiding divergent return types (`tuple[TableSchema, dict]`) or separate single-row helper methods.

### Contract 3: Mid-Batch Schema Evolution Handling
When a database DDL operation occurs mid-batch (e.g., Events 1 & 2 have Schema v1 `[id, name]`; Events 3 & 4 have Schema v2 `[id, name, email]`):
- **Approach A: Slicing by Schema Version (Recommended for Sinks like Iceberg)**:
  - *Behavior*: The converter identifies the schema transition boundary and slices the input sequence into distinct homogeneous schema chunks.
  - *Output*: Emits distinct `(Analytical Schema, Table)` pairs for each schema epoch.
  - *Value*: Sinks can write Batch 1 under Schema v1, commit a transactional schema update to the storage catalog, and write Batch 2 under Schema v2. Safely handles breaking DDLs (dropped columns, renamed columns, type changes) without failure.
- **Approach B: Unified Table (Convenience for Additive Sinks)**:
  - *Behavior*: When a single table is requested across compatible additive schema changes, unifies the schemas across the batch.
  - *Output*: Emits a single analytical Table where earlier rows (Events 1 & 2) automatically receive typed `null` values for columns introduced in later rows.

### Contract 4: Analytical Schema Introspection
The compiled analytical schema descriptor provides table-level metadata to downstream consumers:
- Target destination and table name.
- Immutable sequence of primary key column names.
- Column names replicating the exact source database column order.
- Columnar data type declarations matching the analytical format.

---

## 4. Canonical Analytical Type Mappings

To guarantee predictable analytical schema compilation, types are mapped systematically by **Type Family**:

### 1. Primitives (The 9 Kafka Connect Base Types)
- **Integers**: `int8` → `pa.int8()`, `int16` → `pa.int16()`, `int32` → `pa.int32()`, `int64` → `pa.int64()`.
- **Floating-Point**: `float32` → `pa.float32()`, `float64` → `pa.float64()`.
- **Boolean**: `boolean` → `pa.bool_()`.
- **Text & Strings**: `string` → `pa.string()` (includes Enums, UUIDs, JSON/JSONB text, XML).
- **Binary**: `bytes` → `pa.binary()` (Base64-decoded in JSON; byte buffer unwrapped in Connect).

### 2. Numerics & Decimals (High-Precision)
- **Fixed-Scale Decimal** (`org.apache.kafka.connect.data.Decimal`): Maps to `pa.decimal128(precision, scale)` using two's-complement unscaling.
- **Variable-Scale Decimal** (`io.debezium.data.VariableScaleDecimal`): Maps to `pa.decimal128(38, scale)` with dynamic scale normalization.

### 3. Temporals (Strict Resolution Alignment)
- **Dates** (`Date`, `IsoDate`): Maps to `pa.date32()` (epoch days).
- **Times**:
  - Milliseconds (`Time`): `pa.time32("ms")`.
  - Microseconds (`MicroTime`, `IsoTime`): `pa.time64("us")`.
  - Nanoseconds (`NanoTime`): `pa.time64("ns")`.
- **Timestamps**:
  - Milliseconds (`Timestamp`): `pa.timestamp("ms")`.
  - Microseconds (`MicroTimestamp`, `IsoTimestamp`): `pa.timestamp("us")`.
  - Nanoseconds (`NanoTimestamp`): `pa.timestamp("ns")`.
  - Zoned Timestamps (`ZonedTimestamp`): `pa.timestamp("us", tz="UTC")`.

### 4. Complex & Nested Types
- **Structs**: Kafka Connect `struct` → `pa.struct(...)` (recursively mapping child field types and nullability).
- **Arrays**: Kafka Connect `array` → `pa.list_(element_type)`.
- **Maps**: Kafka Connect `map` or `Hstore` → `pa.map_(key_type, value_type)`.

---

## 5. Non-Negotiable Engineering Invariants & Agent Directives

AI agents and human developers modifying the schema subsystem must adhere to these directives:

### 1. Pure Technical Conversion Invariant (No ETL / Data Processing)
- **Zero Business Logic**: Never implement envelope unwrapping (`after`/`before`), delete row synthesis, or synthetic column injection (e.g. `__deleted`) in the schema subsystem. Convert the incoming payload structure directly according to its schema.
- **Consumer Ownership of Lifecycle**: Provide helper functionalities (`is_heartbeat`, `is_schema_change`, `is_tombstone`) directly on the converter or module—without creating separate utility classes—so consumers can inspect event categories without the converter silently discarding or altering records.

### 2. The JVM Boundary Invariant
- **Zero JVM Interop in Pure-Python Modules**: Pure-Python modules, schema compilers, and value coercers must never import JVM interop libraries or perform dynamic reflection on JVM proxies.
- **Strict Boundary Isolation**: All JVM object unwrapping (`BigDecimal`, dates, byte arrays, structs) belongs strictly at the interop transport boundary. Downstream compilation and coercion routines receive only native Python types.

### 3. Connect Mode Schema Caching Invariant
- **Strong-Reference Caching Before Traversal**: In Connect mode, change event schemas are immutable engine singletons. The interop boundary must check an identity/equality cache before converting schema representations.
- **Ban Unsafe JNI Identity Pitfalls**: Never use a JVM proxy as a dictionary key (which triggers recursive hash code computation across the JNI). Never rely on Python memory address identifiers alone without holding a strong reference to prevent garbage collection.

### 4. Fail-Fast & Actionable Errors
- **No Silent Fallbacks**: If an input payload is malformed or invalid, fail fast and raise a descriptive domain exception immediately.
- **Actionable Missing Schema Diagnostics**: When schema-enabled mode is active and an incoming record contains data without a schema block, raise an informative error explaining how to enable schemaless mode.

### 5. Single Source of Truth for Configuration
- Configuration resolution must be centralized in a dedicated configuration model. Converter initialization must delegate configuration normalization to this model rather than implementing cascading parameter branches.

### 6. Batch-First Processing Over Single-Record Loops
- Prioritize batch-level operations.
- Extract primary keys and resolve schemas once per table batch.
- Infer schemaless schemas across entire batches natively in C++ rather than row-by-row in Python.

### 7. Anti-Accretion & Lean Architecture
- Eliminate pass-through forwarding wrappers that only exist to delegate single calls.
- Avoid redundant type-promotion ladders or manual array reconstructions where analytical libraries provide native C++ functions.

---

## 6. Error Contracts & Conditions

| Exception Type | Condition Raised |
| :--- | :--- |
| `ValueError` | Record destination is empty or invalid. |
| `ValueError` | Record is missing required schema and payload blocks when schema-enabled mode is active. |
| `ValueError` | Record key is malformed or cannot be parsed. |
| `TypeError` | Wire payload cannot be decoded from the incoming representation. |

---

## 7. Verification & Quality Gates

Before submitting any modifications to the schema subsystem:

1. **Unit Tests**:
   ```bash
   uv run pytest tests/unit/ -v -k "schema"
   ```
2. **Integration Tests**:
   ```bash
   uv run pytest tests/integration/schema/ -v
   ```
3. **Type Checking & Linting**:
   ```bash
   uv run ruff check .
   uv run mypy pydbzengine
   ```
