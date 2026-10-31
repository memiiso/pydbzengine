from __future__ import annotations

import unittest
from decimal import Decimal

import pyarrow as pa

from pydbzengine.schema import (
    ChangeBatch,
    ChangeOp,
    ChangeRecord,
    DecimalTypeHandler,
    EventKind,
    IntTypeHandler,
    SchemaDiff,
    SchemaField,
    StructTypeHandler,
    TableBatch,
    TableSchema,
)


class TestSchemaModels(unittest.TestCase):
    def test_event_kind_classification(self) -> None:
        self.assertEqual(
            EventKind.classify(
                "__debezium-heartbeat.server", ["ts_ms"], "__debezium-heartbeat"
            ),
            EventKind.HEARTBEAT,
        )
        self.assertEqual(
            EventKind.classify("custom-hb.server", ["ts_ms"], "custom-hb"),
            EventKind.HEARTBEAT,
        )
        self.assertEqual(
            EventKind.classify(
                "server.schema_changes",
                ["ddl", "databaseName", "tableChanges"],
                "__debezium-heartbeat",
            ),
            EventKind.SCHEMA_CHANGE,
        )
        self.assertEqual(
            EventKind.classify(
                "inventory.customers", ["id", "name"], "__debezium-heartbeat"
            ),
            EventKind.DATA,
        )

    def test_table_schema_equality_row_schema_and_diff(self) -> None:
        pa_s1 = pa.schema(
            [
                pa.field(
                    "id",
                    pa.int32(),
                    nullable=False,
                    metadata={b"doc": b"Primary key"},
                ),
                pa.field("name", pa.string(), nullable=True),
            ]
        )
        s1 = TableSchema(
            destination="inventory.customers",
            kind=EventKind.DATA,
            arrow_schema=pa_s1,
            primary_keys=("id",),
            key_schema=pa.schema([pa.field("id", pa.int32(), nullable=False)]),
        )
        s2 = TableSchema(
            destination="inventory.customers",
            kind=EventKind.DATA,
            arrow_schema=pa_s1,
            primary_keys=("id",),
            key_schema=pa.schema([pa.field("id", pa.int32(), nullable=False)]),
        )
        self.assertEqual(s1, s2)
        self.assertEqual(hash(s1), hash(s2))
        self.assertEqual(len({s1, s2}), 1)
        self.assertEqual(s1.names, ["id", "name"])
        self.assertEqual(s1.table_name, "customers")
        self.assertEqual(s1.value_schema, pa_s1)
        self.assertFalse(s1.is_envelope)
        self.assertEqual(s1.row_schema, pa_s1)
        self.assertEqual(s1.fields, ())

        # Additive evolution diff
        pa_s3 = pa.schema(
            [
                pa.field(
                    "id",
                    pa.int32(),
                    nullable=False,
                    metadata={b"doc": b"Primary key"},
                ),
                pa.field("name", pa.string(), nullable=True),
                pa.field("email", pa.string(), nullable=True),
            ]
        )
        s3 = TableSchema(
            destination="inventory.customers",
            kind=EventKind.DATA,
            arrow_schema=pa_s3,
            primary_keys=("id",),
        )
        diff_add: SchemaDiff = s3.diff(s1)
        self.assertTrue(diff_add.has_changes)
        self.assertTrue(diff_add.is_additive)
        self.assertTrue(diff_add.is_compatible)
        self.assertEqual(len(diff_add.added_fields), 1)
        self.assertEqual(diff_add.added_fields[0].name, "email")
        self.assertEqual(diff_add.removed_fields, ())
        self.assertEqual(diff_add.changed_fields, ())

        # Breaking evolution diff (type change + column drop)
        pa_s4 = pa.schema([pa.field("id", pa.int64(), nullable=False)])
        s4 = TableSchema(
            destination="inventory.customers",
            kind=EventKind.DATA,
            arrow_schema=pa_s4,
            primary_keys=("id",),
        )
        diff_break = s4.diff(s1)
        self.assertTrue(diff_break.has_changes)
        self.assertFalse(diff_break.is_additive)
        self.assertFalse(diff_break.is_compatible)
        self.assertEqual(len(diff_break.removed_fields), 1)
        self.assertEqual(diff_break.removed_fields[0].name, "name")
        self.assertEqual(len(diff_break.changed_fields), 1)
        self.assertEqual(diff_break.changed_fields[0][0].type, pa.int32())
        self.assertEqual(diff_break.changed_fields[0][1].type, pa.int64())

    def test_envelope_row_schema_and_record_data(self) -> None:
        row_fields = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
        env_pa = pa.schema(
            [
                pa.field("before", pa.struct(row_fields)),
                pa.field("after", pa.struct(row_fields)),
                pa.field("source", pa.struct([pa.field("db", pa.string())])),
                pa.field("op", pa.string()),
            ]
        )
        schema = TableSchema(
            destination="inventory.customers",
            kind=EventKind.DATA,
            arrow_schema=env_pa,
            primary_keys=("id",),
        )
        self.assertTrue(schema.is_envelope)
        self.assertEqual(schema.row_schema, pa.schema(row_fields))

        create_rec = ChangeRecord(
            schema=schema,
            value={
                "before": None,
                "after": {"id": 1, "name": "Alice"},
                "source": {"db": "inventory"},
                "op": "c",
            },
            key={"id": 1},
            partition=0,
        )
        self.assertEqual(create_rec.destination, "inventory.customers")
        self.assertEqual(create_rec.kind, EventKind.DATA)
        self.assertEqual(create_rec.op, ChangeOp.INSERT)
        self.assertIsNone(create_rec.before)
        self.assertEqual(create_rec.after, {"id": 1, "name": "Alice"})
        self.assertEqual(create_rec.source, {"db": "inventory"})
        self.assertEqual(create_rec.data, {"id": 1, "name": "Alice"})
        self.assertEqual(create_rec.key, {"id": 1})
        self.assertEqual(create_rec.partition, 0)

        delete_rec = ChangeRecord(
            schema=schema,
            value={
                "before": {"id": 1, "name": "Alice"},
                "after": None,
                "source": {"db": "inventory"},
                "op": "d",
            },
            key={"id": 1},
        )
        self.assertEqual(delete_rec.op, ChangeOp.DELETE)
        self.assertEqual(delete_rec.data, {"id": 1, "name": "Alice"})

        # Extract unpacked row table via TableBatch.to_arrow(extract_rows=True)
        row_tbl = TableBatch(schema=schema, records=(create_rec,)).to_arrow(
            extract_rows=True
        )
        self.assertEqual(row_tbl.schema, pa.schema(row_fields))
        self.assertEqual(row_tbl.to_pylist(), [{"id": 1, "name": "Alice"}])

    def test_tombstone_change_record(self) -> None:
        schema = TableSchema(
            destination="inventory.customers",
            kind=EventKind.DATA,
            arrow_schema=pa.schema([pa.field("id", pa.int32())]),
            primary_keys=("id",),
        )
        ts_with_schema = ChangeRecord(schema=schema, value=None, key={"id": 42})
        self.assertEqual(ts_with_schema.kind, EventKind.TOMBSTONE)
        self.assertIsNone(ts_with_schema.value)
        self.assertIsNone(ts_with_schema.row)
        self.assertIsNone(ts_with_schema.data)
        self.assertIsNone(ts_with_schema.op)
        self.assertIsNone(ts_with_schema.before)
        self.assertIsNone(ts_with_schema.after)
        self.assertEqual(ts_with_schema.destination, "inventory.customers")
        self.assertEqual(ts_with_schema.key, {"id": 42})

        ts_without_schema = ChangeRecord(
            schema=None,
            value=None,
            key={"id": 42},
            destination="inventory.customers",
        )
        self.assertEqual(ts_without_schema.kind, EventKind.TOMBSTONE)
        self.assertIsNone(ts_without_schema.schema)
        self.assertEqual(ts_without_schema.destination, "inventory.customers")

    def test_table_batch_to_arrow(self) -> None:
        val_handler = StructTypeHandler(
            (
                SchemaField("id", IntTypeHandler(pa.int32())),
                SchemaField("price", DecimalTypeHandler(precision=10, scale=2)),
            )
        )
        schema = TableSchema(
            destination="inventory.orders",
            kind=EventKind.DATA,
            arrow_schema=val_handler.arrow_schema,
            primary_keys=("id",),
            value_handler=val_handler,
        )
        row = schema.convert_json_value({"id": 1, "price": "19.99"})
        rec = ChangeRecord(schema=schema, value=row, key={"id": 1})
        tb = TableBatch(schema=schema, records=(rec,))
        self.assertEqual(tb.destination, "inventory.orders")
        self.assertEqual(len(tb), 1)
        self.assertEqual(list(tb), [rec])

        tbl = tb.to_arrow()
        self.assertEqual(tbl.num_rows, 1)
        self.assertEqual(tbl.to_pylist(), [{"id": 1, "price": Decimal("19.99")}])

    def test_change_batch_retains_arrival_order_and_filters_by_kind(self) -> None:
        s_data = TableSchema(
            destination="t1",
            kind=EventKind.DATA,
            arrow_schema=pa.schema([pa.field("id", pa.int32())]),
        )
        s_hb = TableSchema(
            destination="__debezium-heartbeat.server",
            kind=EventKind.HEARTBEAT,
            arrow_schema=pa.schema([pa.field("ts_ms", pa.int64())]),
        )
        r1 = ChangeRecord(schema=s_data, value={"id": 1}, key={"id": 1})
        r2 = ChangeRecord(schema=s_hb, value={"ts_ms": 100})
        r3 = ChangeRecord(schema=s_data, value={"id": 2}, key={"id": 2})
        ts = ChangeRecord(schema=None, value=None, key={"id": 99}, destination="t1")

        batch = ChangeBatch(records=(r1, r2, r3, ts))
        self.assertEqual(len(batch), 4)
        self.assertEqual(list(batch), [r1, r2, r3, ts])
        self.assertEqual(batch.data_records, (r1, r3))
        self.assertEqual(batch.of_kind(EventKind.HEARTBEAT), (r2,))
        self.assertEqual(batch.of_kind(EventKind.TOMBSTONE), (ts,))

        table_batches = batch.table_batches()
        self.assertEqual(len(table_batches), 1)
        self.assertEqual(table_batches[0].destination, "t1")
        self.assertEqual(len(table_batches[0].rows), 2)

        tbl = table_batches[0].to_arrow()
        self.assertEqual(tbl.num_rows, 2)
        self.assertEqual(tbl.to_pylist(), [{"id": 1}, {"id": 2}])

    def test_envelope_schema_diff(self) -> None:
        v1_fields = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
        v2_fields = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("email", pa.string(), nullable=True),
        ]
        env_v1 = TableSchema(
            destination="inventory.customers",
            kind=EventKind.DATA,
            arrow_schema=pa.schema(
                [
                    pa.field("before", pa.struct(v1_fields)),
                    pa.field("after", pa.struct(v1_fields)),
                    pa.field("op", pa.string()),
                ]
            ),
            primary_keys=("id",),
        )
        env_v2 = TableSchema(
            destination="inventory.customers",
            kind=EventKind.DATA,
            arrow_schema=pa.schema(
                [
                    pa.field("before", pa.struct(v2_fields)),
                    pa.field("after", pa.struct(v2_fields)),
                    pa.field("op", pa.string()),
                ]
            ),
            primary_keys=("id",),
        )
        diff = env_v2.diff(env_v1)
        self.assertTrue(diff.has_changes)
        self.assertTrue(diff.is_additive)
        self.assertTrue(diff.is_compatible)
        self.assertEqual([f.name for f in diff.added_fields], ["email"])
        self.assertEqual(diff.changed_fields, ())

        # Explicit use_row_schema=False diffs top-level envelope wrapper fields
        env_diff = env_v2.diff(env_v1, use_row_schema=False)
        self.assertEqual(
            [f[0].name for f in env_diff.changed_fields], ["before", "after"]
        )

    def test_strict_model_validation_raises(self) -> None:
        valid_pa = pa.schema([pa.field("id", pa.int32())])
        with self.assertRaises(ValueError):
            TableSchema(destination="   ", kind=EventKind.DATA, arrow_schema=valid_pa)
        with self.assertRaises(ValueError):
            TableSchema(
                destination="t1", kind=EventKind.DATA, arrow_schema=pa.schema([])
            )
        with self.assertRaises(ValueError):
            TableSchema(
                destination="t1",
                kind=EventKind.DATA,
                arrow_schema=valid_pa,
                key_schema=pa.schema([]),
            )
        with self.assertRaises(ValueError):
            TableSchema.select_primary_keys(
                ("meta",),
                [pa.field("meta", pa.struct([pa.field("k", pa.string())]))],
            )

        s1 = TableSchema(destination="t1", kind=EventKind.DATA, arrow_schema=valid_pa)
        s2 = TableSchema(destination="t2", kind=EventKind.DATA, arrow_schema=valid_pa)

        with self.assertRaises(TypeError):
            _ = ChangeRecord(s1, {"op": 123}).op
        with self.assertRaises(ValueError):
            _ = ChangeRecord(s1, {"op": "invalid_op"}).op
        with self.assertRaises(TypeError):
            _ = ChangeRecord(s1, {"before": "not_a_dict"}).before
        with self.assertRaises(TypeError):
            _ = ChangeRecord(s1, {"after": [1, 2]}).after
        with self.assertRaises(TypeError):
            _ = ChangeRecord(s1, {"source": 42}).source

        ts = ChangeRecord(s1, None, key={"id": 1})
        r1 = ChangeRecord(s1, {"id": 1})
        r2 = ChangeRecord(s2, {"id": 2})

        with self.assertRaises(ValueError):
            TableBatch(schema=s1, records=(ts,))
        with self.assertRaises(ValueError):
            TableBatch(schema=s1, records=(r1, r2))
        with self.assertRaises(ValueError):
            ChangeBatch(records=(ts,)).table_batches(kind=EventKind.TOMBSTONE)

        env_schema = TableSchema(
            destination="t1",
            kind=EventKind.DATA,
            arrow_schema=pa.schema(
                [
                    pa.field("before", pa.struct([pa.field("id", pa.int32())])),
                    pa.field("after", pa.struct([pa.field("id", pa.int32())])),
                    pa.field("op", pa.string()),
                ]
            ),
        )
        empty_env_batch = TableBatch(
            schema=env_schema,
            records=(
                ChangeRecord(env_schema, {"before": None, "after": None, "op": "m"}),
            ),
        )
        with self.assertRaises(ValueError):
            _ = empty_env_batch.data_rows

        with self.assertRaises(TypeError):
            TableSchema(destination="t1", kind="data", arrow_schema=valid_pa)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            TableSchema(
                destination="t1", kind=EventKind.DATA, arrow_schema="not_schema"
            )  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            TableSchema(
                destination="t1",
                kind=EventKind.DATA,
                arrow_schema=valid_pa,
                key_schema="not_schema",  # type: ignore[arg-type]
            )
        with self.assertRaises(TypeError):
            ChangeRecord("not_schema", {"id": 1})  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            ChangeRecord(None, {"id": 1})
        with self.assertRaises(ValueError):
            ChangeRecord(None, None, key={"id": 1})
        with self.assertRaises(ValueError):
            ChangeRecord(s1, {"id": 1}, destination="wrong_dest")
        with self.assertRaises(TypeError):
            ChangeRecord(s1, "not_a_dict")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            ChangeRecord(s1, {"id": 1}, key="not_a_dict")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            ChangeRecord(s1, {"id": 1}, partition=True)

        s1_evolved = TableSchema(
            destination="t1",
            kind=EventKind.DATA,
            arrow_schema=pa.schema([pa.field("id", pa.int64())]),
        )
        with self.assertRaises(ValueError):
            TableBatch(
                schema=s1,
                records=(r1, ChangeRecord(s1_evolved, {"id": 2})),
            )


if __name__ == "__main__":
    unittest.main()
