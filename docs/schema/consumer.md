# Schema-Aware Consumer & Examples

`SchemaAwareChangeHandler` (`pydbzengine.schema.handler`, re-exported from `pydbzengine.schema`) is the base class for schema-driven CDC consumers. It converts incoming `ChangeEvent` batches via `EventConverter`, tracks the active `TableSchema` per destination, and dispatches causally ordered lifecycle hooks.

## Lifecycle Hooks & Dispatch Sequence

When `DebeziumEngine` passes a batch of `ChangeEvent` records to `SchemaAwareChangeHandler.handle_batch()`, the handler converts the batch into a `ChangeBatch`, iterates `batch.destination_events()` in causal order, and invokes:

1. **`on_table_created(self, schema: TableSchema) -> None`**
   Called the first time a `TableSchema` is observed for a destination.
2. **`on_schema_evolved(self, new_schema: TableSchema, old_schema: TableSchema, diff: SchemaDiff) -> None`**
   Called whenever a destination's `TableSchema` differs from its previously tracked version (`diff.has_changes`), passing the computed `SchemaDiff`.
3. **`handle_table_batch(self, batch: TableBatch) -> None`**
   Called for each homogeneous `TableBatch` after `on_table_created` or `on_schema_evolved` has completed.
4. **`on_tombstone(self, record: ChangeRecord) -> None`**
   Called for each delete tombstone (`EventKind.TOMBSTONE`) at its causal position between `TableBatch` runs for that destination.
5. **`on_schema_change(self, record: ChangeRecord) -> None`**
   Called for each DDL schema-change event (`EventKind.SCHEMA_CHANGE`) after flushing all open destination runs.
6. **`on_heartbeat(self, record: ChangeRecord) -> None`**
   Called for each heartbeat event (`EventKind.HEARTBEAT`).

You can inspect currently tracked schemas at any time via `self.schemas` (returns a copy of `dict[str, TableSchema]`) or `self.get_schema(destination)`. Pass `default_variable_scale` to `super().__init__(default_variable_scale=...)` if you need a custom scale for `VariableScaleDecimal` fields (defaults to `10`).

```mermaid
sequenceDiagram
    autonumber
    participant Engine as DebeziumEngine
    participant Handler as SchemaAwareChangeHandler
    participant Conv as EventConverter
    participant Sink as Custom Consumer (e.g. Snowflake)

    Note over Engine,Sink: Batch 1: Initial Records (v1 Schema: id, amount)
    Engine->>Handler: handle_batch([event_1, event_2])
    Handler->>Conv: converter.convert(records)
    Conv-->>Handler: ChangeBatch -> destination_events() = [TableBatch(dest="public.orders", schema=v1)]
    Handler->>Sink: on_table_created(schema=v1)
    Sink-->>Handler: CREATE TABLE orders (id INT, amount DECIMAL(10,2))
    Handler->>Sink: handle_table_batch(TableBatch[v1])
    Sink-->>Handler: Load 2 rows via batch.to_arrow()

    Note over Engine,Sink: Batch 2: Upstream ALTER TABLE ADD COLUMN status VARCHAR
    Engine->>Handler: handle_batch([event_3_v1, event_4_v2])
    Handler->>Conv: converter.convert(records)
    Note right of Conv: Intra-batch evolution sliced into 2 homogeneous TableBatches
    Conv-->>Handler: ChangeBatch -> destination_events() = [TableBatch(v1), TableBatch(v2)]

    Handler->>Sink: handle_table_batch(TableBatch[v1])
    Sink-->>Handler: Load event_3 (v1 schema)

    Handler->>Handler: diff = v2.diff(v1)
    Handler->>Sink: on_schema_evolved(new_schema=v2, old_schema=v1, diff=diff)
    Sink-->>Handler: ALTER TABLE orders ADD COLUMN status VARCHAR
    Handler->>Sink: handle_table_batch(TableBatch[v2])
    Sink-->>Handler: Load event_4 (v2 schema)
```

---

## PyArrow Consumer Example (Default Debezium Envelope)

Without an unwrapping SMT, Debezium emits standard CDC envelopes (`before`, `after`, `source`, `op`, `ts_ms`). `TableSchema.arrow_schema` compiles the envelope directly, and `SchemaDiff.is_additive` detects nullable column additions inside the nested `before`/`after` structs:

```python
from pydbzengine import DebeziumEngine
from pydbzengine.schema import (
    SchemaAwareChangeHandler,
    SchemaDiff,
    TableBatch,
    TableSchema,
)


class ArrowBatchHandler(SchemaAwareChangeHandler):
    def on_table_created(self, schema: TableSchema) -> None:
        self.logger.info(
            "Table created: %s (key_fields=%s, columns=%s)",
            schema.destination,
            schema.key_fields,
            schema.arrow_schema.names,
        )

    def on_schema_evolved(
        self,
        new_schema: TableSchema,
        old_schema: TableSchema,
        diff: SchemaDiff,
    ) -> None:
        self.logger.info(
            "Schema evolved for %s: additive=%s, compatible=%s, added=%s, changed=%s",
            new_schema.destination,
            diff.is_additive,
            diff.is_compatible,
            [f.name for f in diff.added_fields],
            [old_f.name for old_f, _ in diff.changed_fields],
        )

    def handle_table_batch(self, batch: TableBatch) -> None:
        arrow_table = batch.to_arrow()
        self.logger.info(
            "Consumed %d records for %s (columns=%s)",
            arrow_table.num_rows,
            batch.destination,
            arrow_table.column_names,
        )


if __name__ == "__main__":
    props = {
        "name": "arrow-cdc-consumer",
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "localhost",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "inventory",
        "topic.prefix": "cdc",
        "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
        "offset.storage.file.filename": "/tmp/offsets.dat",
    }

    handler = ArrowBatchHandler()
    engine = DebeziumEngine(properties=props, handler=handler)
    engine.run()
```

---

## Custom Snowflake CDC Consumer Example (`ExtractNewRecordState`)

When loading into a relational warehouse or lakehouse table, configure Debezium's `ExtractNewRecordState` SMT (`transforms.unwrap.type = io.debezium.transforms.ExtractNewRecordState`) with `add.fields = op` and `delete.tombstone.handling.mode = rewrite`. Debezium then emits flat table rows with `__op` and `__deleted` metadata columns so `schema.arrow_schema` maps directly to table columns:

- **`arrow_type_to_snowflake_sql`**: Maps `schema.arrow_schema` field types to Snowflake DDL types.
- **`on_table_created`**: Executes `CREATE TABLE IF NOT EXISTS` using `schema.arrow_schema` (excluding `__`-prefixed SMT metadata columns) and `schema.key_fields` for `PRIMARY KEY` constraints.
- **`on_schema_evolved`**: Verifies `diff.is_compatible` and executes `ALTER TABLE ... ADD COLUMN` for newly added data columns in `diff.added_fields`.
- **`handle_table_batch`**: Appends keyless tables directly via `batch.to_arrow(target_schema=data_schema)`, or stages keyed CDC records with an explicit `__CDC_OP` marker (`"D"` for deletes, `"U"` for inserts/updates) and executes an atomic `MERGE INTO`.

```python
from __future__ import annotations

from typing import Any

import pyarrow as pa

from pydbzengine import DebeziumEngine
from pydbzengine.schema import (
    SchemaAwareChangeHandler,
    SchemaDiff,
    TableBatch,
    TableSchema,
)


def arrow_type_to_snowflake_sql(dtype: pa.DataType) -> str:
    if pa.types.is_boolean(dtype):
        return "BOOLEAN"
    if pa.types.is_integer(dtype):
        return "NUMBER(38, 0)"
    if pa.types.is_floating(dtype):
        return "FLOAT"
    if pa.types.is_decimal(dtype):
        return f"NUMBER({dtype.precision}, {dtype.scale})"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "VARCHAR"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return "BINARY"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_time(dtype):
        return "TIME"
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP_TZ" if dtype.tz is not None else "TIMESTAMP_NTZ"
    if pa.types.is_struct(dtype) or pa.types.is_list(dtype) or pa.types.is_map(dtype):
        return "VARIANT"
    raise ValueError(f"Unsupported PyArrow type for Snowflake DDL: {dtype}")


def data_arrow_schema(schema: TableSchema) -> pa.Schema:
    return pa.schema(
        [field for field in schema.arrow_schema if not field.name.startswith("__")]
    )


class SnowflakeChangeHandler(SchemaAwareChangeHandler):
    def __init__(
        self,
        snowflake_conn: Any,
        target_schema: str,
    ) -> None:
        super().__init__()
        self.conn = snowflake_conn
        self.target_schema = target_schema

    def on_table_created(self, schema: TableSchema) -> None:
        table_fqn = f'"{self.target_schema}"."{schema.table_name.upper()}"'
        col_defs: list[str] = []

        for field in data_arrow_schema(schema):
            sf_type = arrow_type_to_snowflake_sql(field.type)
            null_clause = "" if field.nullable else " NOT NULL"
            col_defs.append(f'"{field.name.upper()}" {sf_type}{null_clause}')

        if schema.key_fields:
            pk_cols = ", ".join(f'"{pk.upper()}"' for pk in schema.key_fields)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        ddl = f"CREATE TABLE IF NOT EXISTS {table_fqn} ({', '.join(col_defs)})"
        self.logger.info("Executing DDL: %s", ddl)
        with self.conn.cursor() as cur:
            cur.execute(ddl)

    def on_schema_evolved(
        self,
        new_schema: TableSchema,
        old_schema: TableSchema,
        diff: SchemaDiff,
    ) -> None:
        if not diff.is_compatible:
            raise RuntimeError(
                f"Incompatible schema evolution for '{new_schema.destination}': "
                f"removed={[f.name for f in diff.removed_fields]}, "
                f"changed={[(o.name, o.type, n.type) for o, n in diff.changed_fields]}"
            )

        added_data_fields = [
            f for f in diff.added_fields if not f.name.startswith("__")
        ]
        if not added_data_fields:
            return

        table_fqn = f'"{self.target_schema}"."{new_schema.table_name.upper()}"'
        add_clauses = [
            f'"{field.name.upper()}" {arrow_type_to_snowflake_sql(field.type)}'
            for field in added_data_fields
        ]
        alter_sql = f"ALTER TABLE {table_fqn} ADD COLUMN {', '.join(add_clauses)}"
        self.logger.info("Executing DDL: %s", alter_sql)
        with self.conn.cursor() as cur:
            cur.execute(alter_sql)

    def handle_table_batch(self, batch: TableBatch) -> None:
        from snowflake.connector.pandas_tools import write_pandas

        schema = batch.schema
        data_schema = data_arrow_schema(schema)
        target_table = schema.table_name.upper()
        table_fqn = f'"{self.target_schema}"."{target_table}"'

        if not schema.key_fields:
            df = batch.to_arrow(target_schema=data_schema).to_pandas()
            df.columns = [col.upper() for col in df.columns]
            write_pandas(
                conn=self.conn,
                df=df,
                table_name=target_table,
                schema=self.target_schema,
                quote_identifiers=True,
            )
            return

        data_cols = data_schema.names
        staged_rows: list[dict[str, Any]] = []
        for record in batch:
            row = record.value
            if row is None:
                continue
            op = row.get("__op")
            if op in ("t", "m"):
                continue
            is_delete = op == "d" or row.get("__deleted") == "true"
            staged_row = {col: row.get(col) for col in data_cols}
            staged_row["__CDC_OP"] = "D" if is_delete else "U"
            staged_rows.append(staged_row)

        if not staged_rows:
            return

        stage_table = f"STAGE_{target_table}"
        stage_fqn = f'"{self.target_schema}"."{stage_table}"'
        stage_schema = data_schema.append(pa.field("__CDC_OP", pa.string()))
        stage_df = pa.Table.from_pylist(staged_rows, schema=stage_schema).to_pandas()
        stage_df.columns = [col.upper() for col in stage_df.columns]

        with self.conn.cursor() as cur:
            cur.execute(
                f"CREATE TEMPORARY TABLE IF NOT EXISTS {stage_fqn} LIKE {table_fqn}"
            )
            cur.execute(
                f'ALTER TABLE {stage_fqn} ADD COLUMN IF NOT EXISTS "__CDC_OP" VARCHAR'
            )
            cur.execute(f"TRUNCATE TABLE {stage_fqn}")

        write_pandas(
            conn=self.conn,
            df=stage_df,
            table_name=stage_table,
            schema=self.target_schema,
            quote_identifiers=True,
        )

        join_cond = " AND ".join(
            f't."{pk.upper()}" = s."{pk.upper()}"' for pk in schema.key_fields
        )
        col_names = [f.name.upper() for f in data_schema]
        update_set = ", ".join(f't."{c}" = s."{c}"' for c in col_names)
        insert_cols = ", ".join(f'"{c}"' for c in col_names)
        insert_vals = ", ".join(f's."{c}"' for c in col_names)

        merge_sql = f"""
        MERGE INTO {table_fqn} AS t
        USING {stage_fqn} AS s
        ON {join_cond}
        WHEN MATCHED AND s."__CDC_OP" = 'D' THEN DELETE
        WHEN MATCHED AND s."__CDC_OP" = 'U' THEN UPDATE SET {update_set}
        WHEN NOT MATCHED AND s."__CDC_OP" = 'U' THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        with self.conn.cursor() as cur:
            cur.execute(merge_sql)


if __name__ == "__main__":
    import snowflake.connector

    sf_conn = snowflake.connector.connect(
        user="cdc_user",
        password="cdc_password",
        account="xy12345.us-east-1",
        warehouse="CDC_WH",
        database="ANALYTICS",
        schema="PUBLIC",
    )

    dbz_props = {
        "name": "snowflake-cdc-engine",
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "localhost",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "inventory",
        "topic.prefix": "cdc",
        "table.include.list": "public.customers,public.orders",
        "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
        "offset.storage.file.filename": "/tmp/snowflake_offsets.dat",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.add.fields": "op",
        "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
    }

    handler = SnowflakeChangeHandler(
        snowflake_conn=sf_conn,
        target_schema="PUBLIC",
    )
    engine = DebeziumEngine(properties=dbz_props, handler=handler, format="connect")
    engine.run()
```

---

## Lifecycle Events, Schema Alignment & Standalone Conversion

### Handling Tombstones, DDL Events, and Heartbeats
`SchemaAwareChangeHandler` exposes dedicated hooks for non-data lifecycle events, invoked in causal order alongside `handle_table_batch`:

```python
from pydbzengine.schema import (
    ChangeRecord,
    SchemaAwareChangeHandler,
    TableBatch,
)


class LifecycleAwareHandler(SchemaAwareChangeHandler):
    def handle_table_batch(self, batch: TableBatch) -> None:
        table = batch.to_arrow()
        self.logger.info("Batch for %s: %d rows", batch.destination, table.num_rows)

    def on_tombstone(self, record: ChangeRecord) -> None:
        self.logger.info("Tombstone on %s: key=%s", record.destination, record.key)

    def on_schema_change(self, record: ChangeRecord) -> None:
        ddl = record.value.get("ddl") if record.value is not None else None
        self.logger.info("DDL event on %s: %s", record.destination, ddl)

    def on_heartbeat(self, record: ChangeRecord) -> None:
        self.logger.debug("Heartbeat on %s: %s", record.destination, record.value)
```

### Aligning Batches to an Evolved `target_schema`
When writing heterogeneous historical batches into a single target table schema (including nested `before`/`after` envelope structs on `pyarrow>=19.0.0`), pass `target_schema` to `TableBatch.to_arrow()`:

```python
aligned_table = table_batch.to_arrow(target_schema=latest_table_schema.arrow_schema)
```

`to_arrow(target_schema=...)` projects and reorders columns, casts widened types (including temporal units and nested struct fields), fills missing nullable columns with nulls, and enforces target nullability constraints recursively.

### Standalone `EventConverter` Usage
`EventConverter` can also be used directly without subclassing `SchemaAwareChangeHandler`:

```python
from pydbzengine import DebeziumConfig
from pydbzengine.schema import EventConverter, JsonEventConverter

# Direct instantiation for schema-enabled JSON records
converter = JsonEventConverter()
change_batch = converter.convert(raw_events)

for table_batch in change_batch.table_batches():
    arrow_table = table_batch.to_arrow()

# Or resolve deterministically from DebeziumConfig (CONNECT or JSON with schemas_enabled=True)
config = DebeziumConfig(properties=dbz_props, format="connect")
config_converter = EventConverter.from_config(config)
```
