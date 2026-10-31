# Schema Subsystem Specification

This document is the authoritative specification for the schema subsystem. It defines the overall vision, architectural design, current status vs. target enhancements, detailed functional requirements, and non-negotiable engineering invariants in plain English.

---

## 1. Vision, Goals, and Status Overview

### What We Want to Achieve
We need a **schema-aware event converter and consumer framework** inside the schema subsystem that transforms raw Debezium change events into a **globally reusable Apache Arrow representation** (backed by Apache Arrow table schemas, Arrow tables, and structured row batches) so any downstream consumer can ingest CDC streams without custom parsing, defensive probing, or runtime guesswork.

The subsystem is built around five foundational design decisions:
1. **Three Dedicated Converter Classes Known at Instantiation (Zero Runtime Tricks)**:
   At instantiation time, we already know from the engine and connector configuration whether the event wire format is **JSON** or **Connect**, and whether **schemas are enabled or disabled**. Because this is known upfront, we never need runtime tricks, payload peeking, or cross-mode fallback heuristics. Instantiation resolves directly to one of three dedicated converter classes:
   - **Connect Event Converter**: Strictly consumes in-memory Java Connect source records where Connect schemas are always present.
   - **JSON Event Converter**: Strictly consumes serialized JSON envelopes carrying both an explicit schema block and a payload block.
   - **Schemaless JSON Event Converter**: Strictly consumes raw JSON payload objects when schemas are disabled, owning all batch schema inference and null-column sanitization logic.
2. **Explicit, Separate Domain Handler Classes per Data Type Family**:
   Database change events contain mixed data types—scalar primitives (integers, floats, booleans, strings), temporal primitives (dates, times, timestamps, intervals/durations), fixed and variable-scale decimals, binary and bit-sets, maps, geospatial geometry types, vectors, and nested structures/arrays. Each family is modeled as its own dedicated handler class without dynamic post-initialization function-pointer swapping or cross-format fallback chains. Logical types are matched deterministically before generic struct or array compilation.
3. **Apache Arrow as the Global Reusable Format via Table Batch**:
   Apache Arrow is the universal schema and tabular representation across the subsystem. The per-destination **Table Batch** is the primary carrier pairing the compiled **Table Schema** with converted records/rows and direct Apache Arrow Table materialization.
4. **Minimalist Enum-Driven Event Semantics & Standard Tombstone Processing**:
   - Every converted record exposes its event category via a single **Event Kind** enum property (**Data**, **Heartbeat**, **Schema Change**, **Tombstone**) and its CDC operation via a single **Change Operation** enum property (**Insert**, **Update**, **Delete**, **Read**, **Truncate**, **Message**)—eliminating redundant boolean wrapper methods.
   - Tombstones (events with a null value/payload) follow standard processing without special tombstone-synthesis branches: they are flagged with the **Tombstone** event kind, extract their key and schema normally if present, and allow the record schema to be null when the value is null.
5. **Schema-Aware Consumer Handler Inside the Schema Subsystem**:
   The **Schema-Aware Change Handler** lives inside the schema subsystem (ensuring the core engine never depends on the schema package). It tracks active schemas per destination, computes structural diffs via **Schema Diff** utilities, and dispatches clean lifecycle hooks (**Table Created**, **Schema Evolved**, and **Handle Table Batch**).

### Implementation Status Summary
- **Implemented**:
  1. Three dedicated converter classes (**Connect Event Converter**, **JSON Event Converter**, **Schemaless JSON Event Converter**) inheriting from an abstract **Event Converter** base class taking explicit runtime arguments (`heartbeat_prefix`, `default_variable_scale`) and exposing class-level `format` and `schemas_enabled` attributes alongside a `@cached_property` `registry`.
  2. Explicit domain handler classes per data type family, deterministic logical-type matching before generic structs/arrays, and strict separation of JSON conversion from Connect conversion.
  3. Schemaless null-sanitization helpers encapsulated inside **Schemaless JSON Event Converter**, with no reverse Arrow-to-handler compilation on **Table Schema**.
  4. **Change Record** exposing single enum properties for **Event Kind** (including **Tombstone**) and **Change Operation**, with tombstones processed through the standard pipeline.
  5. **Table Batch** serving as the sole Arrow Table materializer, while **Change Batch** preserves all converted records in arrival order.
  6. **Schema-Aware Change Handler** located in the schema package, extracting `format`, `schemas_enabled`, and `heartbeat_prefix` from Debezium `properties`, instantiating the matching **Event Converter**, and dispatching **Table Created**, **Schema Evolved**, and **Handle Table Batch**.

---

## 2. Subsystem Architecture & Available Capabilities

### 1. Three Dedicated Event Converters
A factory or facade resolves the configuration once at instantiation time and returns the appropriate concrete converter:
1. **Connect Event Converter**:
   - **Input Contract**: Consumes engine change events whose value is a Java Connect source record carrying Java key and value schemas.
   - **Execution**: Extracts the Java value and key schemas, compiles them once per destination into a cached **Table Schema** (retaining strong references to the Java schema objects), and converts values and keys in a single pass through the compiled handler tree.
2. **JSON Event Converter**:
   - **Input Contract**: Consumes serialized JSON records formatted strictly with a schema block and a payload block.
   - **Execution**: Parses the JSON envelope, compiles the embedded value and key schema dictionaries once per destination into a cached **Table Schema**, and converts the JSON payload and key in a single pass through the compiled handler tree.
3. **Schemaless JSON Event Converter**:
   - **Input Contract**: Consumes serialized raw JSON payload objects when schema inclusion is disabled.
   - **Execution**: Groups non-null JSON payloads by destination across the batch, infers an Apache Arrow schema per destination, resolves all-null columns using envelope symmetry (mirroring before/after struct schemas), previously cached concrete field types for that destination, and nullable string fallback (tagged with inferred-null metadata for compatible future promotion), and produces a **Table Schema** without compiling unused field handler trees.

### 2. Dedicated Data Type Handler Families
Each data type family is modeled as a distinct, self-contained handler class with an explicit Apache Arrow data type, a strict JSON conversion method (accepting only JSON wire types), and a strict Connect conversion method (accepting only Java Connect types):
1. **Integer Handler**: 8-bit, 16-bit, 32-bit, and 64-bit signed integers. Rejects booleans and floats.
2. **Float Handler**: 32-bit single-precision and 64-bit double-precision floats. Accepts floats and ints; rejects booleans.
3. **Boolean Handler**: Booleans only.
4. **String & Logical Text Handler**: UTF-8 strings and string-backed logical types (UUID, JSON, XML, Enum, EnumSet, PostgreSQL Ltree, MongoDB ObjectId, Tsvector, ISO Interval).
5. **Temporal Duration & Interval Handler**: Integer-backed temporal logical types mapped to exact-width Arrow integers (32-bit integer for Year and Year-to-Month interval; 64-bit integer for MicroDuration, NanoDuration, and Day-to-Second interval).
6. **Binary & Bit-Set Handler**: Variable-length binary columns and Debezium bit-sets, decoding Base64 strings or hex strings in JSON mode and Java byte arrays or positioned byte buffers in Connect mode.
7. **Fixed-Scale & Variable-Scale Decimal Handlers**: 128-bit Arrow decimals with validated precision (1 to 38) and scale (0 to precision). Decodes Base64 two's-complement bytes or numeric strings in JSON mode, and Java arbitrary-precision decimals, byte arrays/buffers, or variable-scale decimal structs in Connect mode, quantizing with half-up rounding.
8. **Temporal Date, Time, & Timestamp Handlers**:
   - **Date Handler**: Maps epoch days, Java dates, and ISO-8601 date strings to 32-bit Arrow dates.
   - **Time Handler**: Maps millisecond, microsecond, and nanosecond time logical types to 32-bit or 64-bit Arrow times, preserving nanosecond precision when configured.
   - **Timestamp Handler**: Maps millisecond, microsecond, and nanosecond timestamps to naive Arrow timestamps, and zoned timestamps to UTC-zoned Arrow timestamps.
9. **Geospatial Geometry Handlers**: Dedicated handlers for Debezium Geometry, Geography (binary WKB and 32-bit integer SRID struct), and Point (64-bit float X/Y coordinates, binary WKB, and 32-bit integer SRID struct), matched by logical name before generic struct compilation.
10. **Vector Handlers**: Dedicated handlers for dense FloatVector (32-bit float list), DoubleVector (64-bit float list), and SparseDoubleVector (dimension count and index-to-value map struct), matched by logical name before generic struct or array compilation.
11. **Complex & Nested Handlers (Struct, Array, Map)**:
    - **Struct Handler**: Recursively converts nested record structures into Arrow struct types in a single pass.
    - **Array Handler**: Converts homogeneous sequences into Arrow list types.
    - **Map Handler**: Converts typed key-value maps and PostgreSQL Hstore fields into Arrow map types, enforcing non-null scalar keys and coercing stringified JSON keys back to their declared scalar types.

### 3. Domain Entities: Table Schema, Schema Diff, Change Record, Table Batch, and Change Batch
- **Table Schema**: Immutable descriptor for a destination's schema, holding the destination name, derived table name, **Event Kind**, full event Apache Arrow schema, unpacked row Apache Arrow schema, optional key Apache Arrow schema, validated scalar primary key names, and optional bound conversion handlers.
- **Schema Diff**: Structural comparison result between a previous **Table Schema** and a new **Table Schema**, exposing added fields, removed fields, changed field pairs, and boolean properties for whether changes exist, whether the evolution is strictly additive, and whether the evolution is backward-compatible.
- **Change Record**: Immutable converted event holding the destination, partition, converted value dictionary (null for tombstones), converted key dictionary, optional **Table Schema** (null when the event is a tombstone with no schema), **Event Kind** enum property (**Data**, **Heartbeat**, **Schema Change**, **Tombstone**), **Change Operation** enum property (**Insert**, **Update**, **Delete**, **Read**, **Truncate**, **Message**, or null), and envelope accessors (before state, after state, source metadata, and unpacked active row data).
- **Table Batch**: Homogeneous run of consecutive converted records for a single destination sharing the same **Table Schema**. Serves as the sole Apache Arrow Table materializer, exposing full envelope rows, unpacked table rows, and direct conversion to an Apache Arrow Table.
- **Change Batch**: Top-level container returned by batch conversion, holding the ordered tuple of converted **Change Record** items and slicing them by destination into homogeneous **Table Batch** runs.

---

## 3. Detailed Requirements

### Requirement 1: Three Dedicated Converter Classes & Deterministic Instantiation
- **Goal**: Resolve the wire format and schema mode once in the change handler or via automatic engine-to-handler configuration binding and bind to one of three dedicated converter classes with zero runtime format guessing or cross-mode fallbacks.
- **Status**: Implemented
- **Specification Details**:
  1. **Automatic Configuration Binding**: `DebeziumEngine` wraps connector `properties` and `format` into a centralized `DebeziumConfig` instance (which resolves wire format, `schemas_enabled`, and `heartbeat_prefix`) and automatically configures attached handlers via the `handler.configure(config=self.config)` lifecycle hook. **Schema-Aware Change Handler** initializes `self.converter` to `None` in `__init__` and delegates `configure(config)` to `EventConverter.from_config(config=config, ...)`, which instantiates the matching concrete **Event Converter** subclass. Neither **Schema-Aware Change Handler** nor **Event Converter** stores raw `properties`.
  2. Three dedicated converter classes are provided:
     - **Connect Event Converter**: Used when the format is Connect (`format = EventFormat.CONNECT`, `schemas_enabled = True`). Strictly requires incoming events to provide a Java Connect source record with a valid Java schema (unless the value is null).
     - **JSON Event Converter**: Used when the format is JSON and schemas are enabled (`format = EventFormat.JSON`, `schemas_enabled = True`). Strictly requires incoming non-null JSON values and keys to contain explicit schema and payload blocks, failing fast if either block is missing.
     - **Schemaless JSON Event Converter**: Used when the format is JSON and schemas are disabled (`format = EventFormat.JSON`, `schemas_enabled = False`). Treats parsed JSON objects directly as payloads without probing for schema/payload envelope keys.

### Requirement 2: Global Apache Arrow Schema Compilation & Destination Caching
- **Goal**: Compile JSON and Connect schemas into reusable Apache Arrow **Table Schema** entities cached per destination.
- **Status**: Implemented (requires removing reverse Arrow-to-handler synthesis when handlers are omitted).
- **Specification Details**:
  1. JSON schema blocks and Java Connect schemas must normalize into an immutable field specification tree capturing field name, base type, nullability, documentation, logical type name, parameters, and child specifications.
  2. Normalized specifications must compile into Apache Arrow fields (storing documentation and logical type names in Arrow field metadata) paired with their compiled data type handlers.
  3. **Table Schema** must not reverse-compile a handler tree from an Arrow schema when no value handler is provided (such as in Schemaless JSON mode).
  4. Compiled **Table Schema** instances must be cached per destination (retaining strong references to Java schema objects in Connect mode) so unchanged schemas are never recompiled.

### Requirement 3: Dedicated Scalar Primitive & Logical Scalar Handlers
- **Goal**: Model integer, float, boolean, string, and temporal duration/interval primitives as separate, strict handler classes without post-init function-pointer mutation.
- **Status**: Partially Implemented (currently lumped into a single primitive handler using dynamic function-pointer binding).
- **Specification Details**:
  1. Separate handler classes must be defined for **Integer** (8/16/32/64-bit), **Float** (32/64-bit), **Boolean**, **String** (including UUID, JSON, XML, Enum, EnumSet, Ltree, ObjectId, Tsvector, and ISO Interval), and **Temporal Duration/Interval** (32-bit Year and Year-to-Month; 64-bit MicroDuration, NanoDuration, and Day-to-Second).
  2. Each handler class must implement strict JSON conversion (accepting only JSON primitive types) and strict Connect conversion (accepting only Java Connect primitive/wrapper types), rejecting booleans for numeric fields and floats for integer fields.

### Requirement 4: Dedicated Temporal Date, Time, and Timestamp Handlers
- **Goal**: Convert date, time, and timestamp fields with strict isolation between JSON wire representations and Java Connect temporal objects.
- **Status**: Implemented (requires removing non-JSON Python object fallbacks from JSON conversion methods).
- **Specification Details**:
  1. **Date Handler**: Maps to 32-bit Arrow dates. JSON conversion must accept integer epoch days or ISO-8601 strings; Connect conversion must accept integer epoch days, Java date/temporal objects, or ISO strings.
  2. **Time Handler**: Maps to 32-bit millisecond or 64-bit microsecond/nanosecond Arrow times. JSON conversion must accept integer offsets or ISO-8601 time strings (preserving nanoseconds when target unit is nanoseconds); Connect conversion must accept integer offsets, Java time objects, or ISO strings.
  3. **Timestamp Handler**: Maps to naive or UTC-zoned Arrow timestamps (millisecond, microsecond, or nanosecond). JSON conversion must accept integer epoch offsets or ISO-8601 strings; Connect conversion must accept integer epoch offsets, Java instant/timestamp/date objects, or ISO strings.

### Requirement 5: Dedicated Fixed-Scale and Variable-Scale Decimal Handlers
- **Goal**: Convert fixed-scale and variable-scale decimals into 128-bit Apache Arrow decimals with separate, deterministic JSON and Connect decoding paths.
- **Status**: Implemented (requires separating JSON and Connect decoding paths instead of sharing a multi-type fallback chain).
- **Specification Details**:
  1. Precision must be validated between 1 and 38 (defaulting to 38) and scale between 0 and precision.
  2. In JSON mode, fixed-scale decimals must decode Base64 two's-complement strings, numeric strings, integers, or finite floats; variable-scale decimals must extract the integer scale and unscaled value from the JSON mapping.
  3. In Connect mode, fixed-scale decimals must decode Java arbitrary-precision decimals or byte arrays/buffers; variable-scale decimals must extract the scale and unscaled value from the Java Connect struct.
  4. Values with fractional digits exceeding the target scale must be quantized using half-up rounding.

### Requirement 6: Dedicated Binary and Bit-Set Handler
- **Goal**: Convert binary columns and bit-sets into Apache Arrow binary values without cross-mode fallback calls.
- **Status**: Implemented (requires removing Connect-to-JSON fallback calls).
- **Specification Details**:
  1. Maps raw bytes and Debezium bit-sets to Arrow variable-length binary types.
  2. JSON conversion must decode Base64-encoded strings or zero-x prefixed hexadecimal strings.
  3. Connect conversion must unwrap Java byte arrays, positioned Java byte buffers (respecting position, array offset, and remaining bytes), or byte sequences.

### Requirement 7: Dedicated Geospatial, Vector, Struct, Array, and Map Handlers
- **Goal**: Model geospatial types, dense/sparse vectors, nested structs, arrays, and maps in dedicated handler classes with deterministic logical-type precedence.
- **Status**: Partially Implemented (requires dedicated geospatial/vector handler classes and matching logical types before generic struct/array compilation).
- **Specification Details**:
  1. The type registry must check logical type names for **Geometry**, **Geography**, **Point**, **SparseDoubleVector**, **FloatVector**, and **DoubleVector** *before* generic struct or array compilation so their dedicated handlers are always selected even when wire schemas include sub-field definitions.
  2. **Struct Handler** must recursively convert JSON dicts in JSON mode and Java Connect structs in Connect mode in a single pass.
  3. **Array Handler** must convert JSON lists in JSON mode and Java iterables/lists in Connect mode.
  4. **Map Handler** (including PostgreSQL Hstore) must enforce non-null scalar keys, convert JSON dicts (coercing stringified keys to declared scalar types) or key-value pair lists in JSON mode, and convert Java maps in Connect mode.

### Requirement 8: Schemaless JSON Inference Encapsulated in Schemaless Converter
- **Goal**: Infer valid Apache Arrow schemas from schemaless JSON batches while keeping all schemaless inference and null-sanitization logic encapsulated inside the **Schemaless JSON Event Converter**.
- **Status**: Partially Implemented (currently lives on **Table Schema** instead of **Schemaless JSON Event Converter**).
- **Specification Details**:
  1. **Schemaless JSON Event Converter** must infer an Apache Arrow schema per destination from each batch of raw JSON payloads.
  2. For Debezium envelope payloads (containing operation code, before state, and after state), if either before or after is all-null across the batch, it must mirror the struct schema from the populated side; if both are structs with differing field types, it must unify them permissively.
  3. Any remaining all-null fields must reuse their known concrete type from the destination's previously cached schema if available, or fall back to a nullable string type tagged with inferred-null metadata so subsequent schema diffs recognize promotion to a concrete type as compatible.

### Requirement 9: Key Conversion and Scalar Primary Key Resolution
- **Goal**: Convert event keys using compiled key handlers and validate scalar primary key column names against the table schema.
- **Status**: Implemented (requires removing the dead schemaless-Connect-key unwrapper).
- **Specification Details**:
  1. In Connect and Schema-Enabled JSON modes, event keys must be converted using the compiled key handler (supporting struct keys, map keys, and single-field scalar keys). In Schemaless JSON mode, keys are parsed directly as JSON dictionaries.
  2. Primary key column names must be matched against the table's row fields (inside before/after structs for envelopes, or top-level fields for flattened records) and validated to reject nested struct, list, or map types.

### Requirement 10: Single Enum Properties for Event Kind & Operation Type, and Standard Tombstone Processing
- **Goal**: Keep **Change Record** simple and free of boolean wrapper bloat by exposing single enum properties for event category and operation type, and processing tombstones through the standard pipeline.
- **Status**: Pending Refactoring
- **Specification Details**:
  1. **Single Event Kind Enum Property**: **Change Record** must expose a single `kind` property returning an **Event Kind** enum with four values: **Data**, **Heartbeat**, **Schema Change**, and **Tombstone**. Redundant boolean wrapper properties (`is_data`, `is_heartbeat`, `is_schema_change`, `is_tombstone`) must not be defined.
  2. **Single Change Operation Enum Property**: **Change Record** must expose a single `op` property returning a **Change Operation** enum (**Insert** for `'c'`, **Update** for `'u'`, **Delete** for `'d'`, **Read** for `'r'`, **Truncate** for `'t'`, **Message** for `'m'`, or null when absent/tombstone). Redundant boolean wrapper properties (`is_insert`, `is_update`, `is_delete`, `is_read`) must not be defined.
  3. **Minimal, Standard Tombstone Processing**: When an event's value or payload is null, the converter must not synthesize a fake key-backed value **Table Schema** or branch into special tombstone builders. It must simply set the record's **Event Kind** to **Tombstone**, extract the schema normally if present (allowing `schema` on the record to be null when the value is null), and convert the key normally.
  4. **Row State Accessors**: **Change Record** must expose properties for `before`, `after`, `source`, and active row `data` (returning `after` when present, `before` on delete, or the flat value dictionary for unwrapped events), without a single-record `to_arrow` method.

### Requirement 11: Table Batch as the Primary Arrow + Rows Carrier & Schema Diff Utilities
- **Goal**: Group converted records into homogeneous per-destination **Table Batch** objects that own Apache Arrow Table materialization, paired with **Schema Diff** utilities.
- **Status**: Partially Implemented (requires removing `ChangeBatch.to_arrow`, `ChangeRecord.to_arrow`, and silent post-init record splitting).
- **Specification Details**:
  1. **Table Batch (Arrow Schema + Rows)**: Groups consecutive converted records sharing the same destination and **Table Schema**. Exposes full envelope `rows`, unpacked table `data_rows`, and `to_arrow(target_schema, extract_rows)` to materialize an Apache Arrow Table directly.
  2. **Change Batch**: Holds the tuple of converted **Change Record** items in arrival order without silent post-init mutation, provides filtering by **Event Kind**, and slices records by destination into homogeneous **Table Batch** runs whenever a destination's schema evolves. Does not expose a fragile batch-wide `to_arrow` method.
  3. **Schema Diff Utilities**: **Table Schema** must provide a `diff(previous)` method returning a **Schema Diff** with added fields, removed fields, changed field pairs, `has_changes`, `is_additive`, and `is_compatible`.

### Requirement 12: Schema-Aware Change Handler Inside the Schema Subsystem
- **Goal**: Provide a base consumer handler inside the schema package that automates conversion, per-destination schema tracking, and lifecycle dispatch.
- **Status**: Partially Implemented (currently located in the engine package with a separate tombstone hook).
- **Specification Details**:
  1. **Package Locality**: **Schema-Aware Change Handler** must reside in the schema subsystem (subclassing the engine's base change handler contract) so the core engine package has zero imports or dependencies on the schema subsystem.
  2. **Lifecycle Dispatch**: On each batch of raw engine events, the handler must convert the batch via its converter, iterate through the homogeneous **Table Batch** runs, track the active **Table Schema** per destination, and invoke:
     - **Table Created Hook**: Invoked the first time a **Table Schema** is observed for a destination.
     - **Schema Evolved Hook**: Invoked whenever a destination's **Table Schema** changes from its previously tracked version, passing the new schema, old schema, and computed **Schema Diff**.
     - **Handle Table Batch Hook**: Invoked for each homogeneous **Table Batch**.

---

## 4. Non-Negotiable Engineering Invariants

1. **Simple, Strict, Deterministic, and Fail-Fast**: Execution must be simple, strict, and completely deterministic. Resolve wire format and schema mode once at instantiation time with zero runtime format guessing, and immediately raise descriptive errors on malformed inputs, incompatible data types, missing schemas, or invalid configuration settings.
2. **No Defensive Coding or Dynamic Attribute Lookups**: Never use defensive coding patterns or dynamic attribute reflection (such as dynamic attribute getters or property existence checks) on domain entities. Trust and enforce strict static contracts with direct attribute access.
3. **No Fallback Logic or Hidden Issues**: Never introduce fallback chains, silent recovery paths, cross-mode fallback checks, or default-guessing logic that mask actual issues and create unseen, unpredictable runtime behaviors.
4. **Simple and Elegant — No Vibe Coding**: Keep all logic clean, direct, and elegant. Ban superficial helper wrappers (such as redundant `is_xxx` boolean methods over enums), single-consumer indirection, dead parameters, and speculative abstractions.
5. **Complete Domain-Driven Design over Arbitrary Size Constraints**: Model the subsystem around complete, cohesive domain entities that faithfully represent real-world concepts. Every entity class must have a well-defined purpose and fulfill its complete role; never split or fragment cohesive entities to satisfy arbitrary line-of-code (LOC) limits or "god class" metrics.
6. **Single-Pass Value Conversion**: Traverse the compiled field conversion tree in a single pass for both JSON and Connect wire payloads without intermediate untyped dictionary unwrapping.
7. **JVM Boundary Isolation**: Loading or using the schema subsystem must never import the Java bridge or start a Java Virtual Machine.
8. **Connect Schema Caching**: Cache compiled table schemas per destination while retaining strong references to the underlying Java schema objects so identity and equality checks remain valid across batches.
