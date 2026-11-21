# Domain Models & Data Types

This page documents the public domain entities in `pydbzengine.schema.models` (re-exported from `pydbzengine.schema`) and the Debezium-to-Arrow type mappings in `pydbzengine.schema.types`.

## Domain Model Relationships

```mermaid
classDiagram
    class ChangeBatch {
        +tuple~ChangeRecord~ records
        +of_kind(kind: EventKind, *more: EventKind) ChangeBatch
        +group_by_kind() dict~EventKind, ChangeBatch~
        +group_by_destination() dict~str, ChangeBatch~
        +to_table_batches() tuple~TableBatch~
    }

    class TableBatch {
        +TableSchema schema
        +tuple~ChangeRecord~ records
        +str destination
        +list~dict~ rows
        +to_arrow(target_schema: pa.Schema | None) pa.Table
    }

    class ChangeRecord {
        +str destination
        +dict | None value
        +dict | None key
        +TableSchema | None schema
        +int | None partition
        +EventKind kind
    }

    class TableSchema {
        +str destination
        +pa.Schema arrow_schema
        +tuple~str~ key_fields
        +pa.Schema | None key_schema
        +tuple~str~ namespace
        +str table_name
        +diff_from(previous: TableSchema | pa.Schema) SchemaDiff
    }

    class SchemaDiff {
        +tuple~pa.Field~ added_fields
        +tuple~pa.Field~ removed_fields
        +tuple~tuple~ changed_fields
        +bool reordered
        +bool key_fields_changed
        +bool key_schema_changed
        +bool has_changes
        +bool is_additive
        +bool is_compatible
    }

    ChangeBatch *-- ChangeRecord : contains
    ChangeBatch ..> TableBatch : segments into
    TableBatch *-- ChangeRecord : groups
    TableBatch --> TableSchema : bound to
    ChangeRecord --> TableSchema : references
    TableSchema ..> SchemaDiff : computes via diff_from()
```

---

## Domain Entities (`pydbzengine.schema.models`)

### `TableSchema`
Immutable compiled schema descriptor for a CDC destination:

| Attribute / Method | Type | Description |
| :--- | :--- | :--- |
| `destination` | `str` | Full destination identifier (e.g., `"cdc_prod.inventory.orders"`). Whitespace-trimmed and non-empty. |
| `arrow_schema` | `pa.Schema` | Compiled PyArrow schema matching the exact top-level event value emitted by Debezium (envelope struct by default, or flat table columns when using `ExtractNewRecordState`). |
| `key_fields` | `tuple[str, ...]` | Field names extracted from the event key schema (e.g., `("id",)` for struct keys, `("key",)` for scalar keys, or `()` for keyless / map-keyed tables). |
| `key_schema` | `pa.Schema \| None` | Compiled PyArrow schema of the event key, if present (non-map keys). |
| `namespace` | `tuple[str, ...]` | Dot-split destination prefix excluding `table_name` (e.g., `("cdc_prod", "inventory")`). |
| `table_name` | `str` | Table name extracted from the final dot-separated segment of `destination` (e.g., `"orders"`). |
| `diff_from(previous)` | `SchemaDiff` | Computes the structural difference from `previous` (`TableSchema` or `pa.Schema`) to `self`. |

### `SchemaDiff`
Structural comparison result between a previous and new `TableSchema` (or `pa.Schema`):

| Attribute / Property | Type | Description |
| :--- | :--- | :--- |
| `added_fields` | `tuple[pa.Field, ...]` | Top-level fields added in the new schema. |
| `removed_fields` | `tuple[pa.Field, ...]` | Top-level fields present in the previous schema that were removed. |
| `changed_fields` | `tuple[tuple[pa.Field, pa.Field], ...]` | `(old_field, new_field)` pairs whose `pa.DataType` or `nullable` flag changed. |
| `reordered` | `bool` | `True` if the relative order of surviving top-level fields changed. |
| `key_fields_changed` | `bool` | `True` if `TableSchema.key_fields` changed (always `False` when diffing against a raw `pa.Schema`). |
| `key_schema_changed` | `bool` | `True` if `TableSchema.key_schema` changed (always `False` when diffing against a raw `pa.Schema`). |
| `has_changes` | `bool` | `True` if any fields were added, removed, changed, reordered, or if key metadata changed. |
| `is_additive` | `bool` | `True` when changes consist solely of new nullable fields (top-level or nested inside structs such as `before`/`after`), with no removals, reordering, or key changes. |
| `is_compatible` | `bool` | `True` when no fields were removed, key metadata is unchanged, all new fields (top-level or nested) are nullable, no lossy promotions occur (`int64`/`uint64`/`decimal` to float), and PyArrow permissive schema unification matches the new schema (supports lossless widenings such as `int32 -> int64`, `float32 -> float64`, decimal/temporal widening, nullability widening, and reordering). |

### `ChangeRecord` & `EventKind`
`ChangeRecord` represents a single converted CDC event:

| Attribute | Type | Description |
| :--- | :--- | :--- |
| `destination` | `str` | Destination topic or table identifier. |
| `value` | `dict[str, Any] \| None` | Converted payload dictionary matching `schema.arrow_schema`, or `None` exclusively for `EventKind.TOMBSTONE`. |
| `key` | `dict[str, Any] \| None` | Converted key dictionary (required and non-empty for `EventKind.TOMBSTONE`). |
| `schema` | `TableSchema \| None` | Compiled `TableSchema` for the event, if available. |
| `partition` | `int \| None` | Source partition number, if present. |
| `kind` | `EventKind` | Event classification: `DATA`, `HEARTBEAT`, `SCHEMA_CHANGE`, or `TOMBSTONE`. |

Each event is classified into one of four `EventKind` values:

- **`EventKind.TOMBSTONE`** (`"tombstone"`): `value` is `None` (delete tombstone carrying only `key`).
- **`EventKind.HEARTBEAT`** (`"heartbeat"`): Destination starts with `heartbeat_prefix` or wire schema ends with `io.debezium.connector.common.Heartbeat`.
- **`EventKind.SCHEMA_CHANGE`** (`"schema_change"`): Wire schema ends with `.SchemaChangeValue` / `.SchemaChangeKey`, or payload contains Debezium DDL marker fields (`{"ddl", "databaseName", "tableChanges"}`).
- **`EventKind.DATA`** (`"data"`): Standard table change record.

In default Debezium envelope mode, access fields directly on `record.value` (for example, `record.value["op"]`, `record.value["before"]`, `record.value["after"]`, `record.value["source"]`). When `ExtractNewRecordState` is configured, `record.value` contains the unwrapped table columns (plus any configured metadata fields like `record.value["__op"]`) directly at the top level.

### `TableBatch` & `ChangeBatch`
- **`ChangeBatch`**: Holds `records: tuple[ChangeRecord, ...]` in arrival order (supports `len()`, `bool()`, and iteration).
  - `batch.of_kind(kind: EventKind, *more: EventKind) -> ChangeBatch`: Returns a sub-`ChangeBatch` containing only records matching the given `EventKind`(s) in arrival order.
  - `batch.group_by_kind() -> dict[EventKind, ChangeBatch]`: Partitions records in a single pass into non-empty sub-batches keyed by `EventKind`.
  - `batch.group_by_destination() -> dict[str, ChangeBatch]`: Groups records in a single pass into sub-batches keyed by `destination`, preserving arrival order (chains naturally with `of_kind()`, e.g., `batch.of_kind(EventKind.TOMBSTONE).group_by_destination()`).
  - `batch.to_table_batches() -> tuple[TableBatch, ...]`: Groups `EventKind.DATA` records per destination into homogeneous `TableBatch` runs, splitting a destination's batch only when its `TableSchema` changes within the batch (non-data records such as tombstones, DDLs, and heartbeats do not fragment `DATA` runs).
- **`TableBatch`**: Groups consecutive `EventKind.DATA` records for one destination sharing a single `TableSchema` (supports `len()` and iteration).
  - `batch.destination -> str`: Shortcut for `batch.schema.destination`.
  - `batch.rows -> list[dict[str, Any]]`: Converted `record.value` dictionaries matching `schema.arrow_schema`.
  - `batch.to_arrow(target_schema: pa.Schema | None = None) -> pa.Table`: Materializes a `pyarrow.Table` from `batch.rows` using `schema.arrow_schema`. When `target_schema` is provided and differs from `schema.arrow_schema`, aligns/reorders columns, casts existing columns via PyArrow (including temporal unit scaling and nested struct evolution on `before`/`after` fields; subfield reordering requires `pyarrow>=21.0.0`), fills missing nullable target columns with nulls, and recursively validates target nullability.
  - `batch.__arrow_c_stream__(requested_schema=None)`: Implements the Arrow PyCapsule Interface for direct interop with Arrow-native engines without manual conversion:

```python
import pyarrow as pa

# Pass TableBatch directly to any Arrow PyCapsule consumer:
arrow_table = pa.table(batch)
# duckdb.sql("SELECT * FROM batch")
# polars.from_arrow(batch)
```

---

## Debezium-to-Arrow Type Mappings (`pydbzengine.schema.types`)

`EventConverter` resolves Debezium and Kafka Connect schemas through `pydbzengine.schema.types` into `pyarrow.Field` descriptors (preserving `doc` and `debezium.logical_type` in field metadata) and binds format-specific decoders in `connect.py` or `json.py`:

| Category | Debezium / Connect Types & Logical Names | Target PyArrow Type (`pa.DataType`) | Python Runtime Value |
| :--- | :--- | :--- | :--- |
| **Boolean** | `boolean` | `pa.bool_()` | `bool` |
| **Integers** | `int8`, `int16`, `int32`, `int64` (and int-backed logical types such as `Year`, `MicroDuration`, `NanoDuration`) | `pa.int8()`, `pa.int16()`, `pa.int32()`, `pa.int64()` | `int` |
| **Floats** | `float` / `float32`, `double` / `float64` | `pa.float32()`, `pa.float64()` | `float` |
| **Strings** | `string` (and string-backed logical types such as `Uuid`, `Json`, `Xml`, `Enum`, `EnumSet`, `Ltree`, `Interval`) | `pa.string()` | `str` |
| **Binary** | `bytes` (and `io.debezium.data.Bits`) | `pa.binary()` | `bytes` |
| **Fixed Decimal** | `org.apache.kafka.connect.data.Decimal` | `pa.decimal128(precision, scale)` | `decimal.Decimal` |
| **Variable Decimal** | `io.debezium.data.VariableScaleDecimal` | `pa.decimal128(precision, scale)` | `decimal.Decimal \| None` |
| **Date** | `org.apache.kafka.connect.data.Date`, `io.debezium.time.Date`, `io.debezium.time.IsoDate` | `pa.date32()` | `datetime.date \| int` |
| **Time** | `org.apache.kafka.connect.data.Time`, `io.debezium.time.Time` (`ms`), `MicroTime` / `IsoTime` / `ZonedTime` (`us`), `NanoTime` (`ns`) | `pa.time32("ms")`, `pa.time64("us")`, `pa.time64("ns")` | `datetime.time \| int` |
| **Timestamp** | `org.apache.kafka.connect.data.Timestamp`, `io.debezium.time.Timestamp` (`ms`), `MicroTimestamp` / `IsoTimestamp` (`us`), `NanoTimestamp` (`ns`), `ZonedTimestamp` (`us`, `tz="UTC"`) | `pa.timestamp(unit, tz=None \| "UTC")` | `datetime.datetime \| int` |
| **Struct** | `struct` (envelopes, nested records, and struct-backed logical types such as `Geometry`, `Geography`, `Point`, `SparseDoubleVector`) | `pa.struct([...])` | `dict[str, Any]` |
| **Array** | `array` (lists and array-backed logical types such as `FloatVector`, `DoubleVector`) | `pa.list_(item_field)` | `list[Any]` |
| **Map** | `map` (maps and `io.debezium.data.Hstore`) | `pa.map_(key_field, value_field)` | `dict[Any, Any]` |

!!! tip "Preserving Debezium Logical Type Metadata"
    Logical types whose wire representation already matches a standard primitive or nested container (such as `io.debezium.data.Uuid`, `io.debezium.data.Json`, `io.debezium.data.geometry.Geometry`, `io.debezium.data.geometry.Point`, `io.debezium.data.vector.FloatVector`, and `io.debezium.data.Hstore`) compile cleanly through the corresponding standard handler while preserving their full Debezium logical type name in `pa.Field.metadata[b"debezium.logical_type"]`.

!!! note "Decimal Precision & JSON Null-Default Parity"
    - Fixed and variable decimals compile to `pa.decimal128(precision, scale)` (`1 <= precision <= 38`, `0 <= scale <= precision`), matching Arrow/Iceberg lakehouse conventions. Higher-precision source columns (>38 digits) fail fast unless configured with `decimal.handling.mode=string` or `double`.
    - In `"connect"` mode, `ConnectEventConverter` reads struct fields via `getWithoutDefault()` so explicit `NULL`s on nullable columns with SQL `DEFAULT` clauses stay `None`. In `"json"` mode, Kafka Connect's `JsonConverter` defaults to `replace.null.with.default=true`; set `"converter.replace.null.with.default": "false"` if you want identical `NULL` preservation in JSON mode.
