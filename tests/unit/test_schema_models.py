from __future__ import annotations

import base64
import datetime
import unittest
from decimal import Decimal

import pyarrow as pa

from pydbzengine.schema import (
    ChangeBatch,
    ChangeRecord,
    EventKind,
    SchemaDiff,
    TableBatch,
    TableSchema,
)
from pydbzengine.schema.types import (
    DecimalTypeHandler,
    IntTypeHandler,
    SchemaField,
    StructTypeHandler,
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
            arrow_schema=pa_s1,
            primary_keys=("id",),
            key_schema=pa.schema([pa.field("id", pa.int32(), nullable=False)]),
        )
        s2 = TableSchema(
            destination="inventory.customers",
            arrow_schema=pa_s1,
            primary_keys=("id",),
            key_schema=pa.schema([pa.field("id", pa.int32(), nullable=False)]),
        )
        self.assertEqual(s1, s2)
        self.assertEqual(hash(s1), hash(s2))
        self.assertEqual(len({s1, s2}), 1)
        self.assertEqual(s1.arrow_schema.names, ["id", "name"])
        self.assertEqual(s1.table_name, "customers")
        self.assertEqual(s1.arrow_schema, pa_s1)

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

    def test_envelope_record_and_batch_pass_through(self) -> None:
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
            arrow_schema=env_pa,
            primary_keys=("id",),
        )

        create_rec = ChangeRecord(
            destination="inventory.customers",
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
        self.assertEqual(
            create_rec.value,
            {
                "before": None,
                "after": {"id": 1, "name": "Alice"},
                "source": {"db": "inventory"},
                "op": "c",
            },
        )
        self.assertEqual(create_rec.key, {"id": 1})
        self.assertEqual(create_rec.partition, 0)

        env_tbl = TableBatch(schema=schema, records=(create_rec,)).to_arrow()
        self.assertEqual(env_tbl.schema, env_pa)
        self.assertEqual(
            env_tbl.to_pylist(),
            [
                {
                    "before": None,
                    "after": {"id": 1, "name": "Alice"},
                    "source": {"db": "inventory"},
                    "op": "c",
                }
            ],
        )

    def test_tombstone_change_record(self) -> None:
        schema = TableSchema(
            destination="inventory.customers",
            arrow_schema=pa.schema([pa.field("id", pa.int32())]),
            primary_keys=("id",),
        )
        ts_with_schema = ChangeRecord(
            destination="inventory.customers",
            schema=schema,
            value=None,
            key={"id": 42},
        )
        self.assertEqual(ts_with_schema.kind, EventKind.TOMBSTONE)
        self.assertIsNone(ts_with_schema.value)
        self.assertEqual(ts_with_schema.destination, "inventory.customers")
        self.assertEqual(ts_with_schema.key, {"id": 42})

        ts_without_schema = ChangeRecord(
            destination="inventory.customers",
            schema=None,
            value=None,
            key={"id": 42},
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
            arrow_schema=val_handler.arrow_schema,
            primary_keys=("id",),
        )
        self.assertEqual(schema.arrow_schema, val_handler.arrow_schema)
        price_b64 = base64.b64encode((1999).to_bytes(2, "big", signed=True)).decode(
            "ascii"
        )
        row = val_handler.convert_json({"id": 1, "price": price_b64})
        rec = ChangeRecord(
            destination="inventory.orders",
            schema=schema,
            value=row,
            key={"id": 1},
        )
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
            arrow_schema=pa.schema([pa.field("id", pa.int32())]),
        )
        s_hb = TableSchema(
            destination="__debezium-heartbeat.server",
            arrow_schema=pa.schema([pa.field("ts_ms", pa.int64())]),
        )
        r1 = ChangeRecord(
            destination="t1", schema=s_data, value={"id": 1}, key={"id": 1}
        )
        r2 = ChangeRecord(
            destination="__debezium-heartbeat.server",
            schema=s_hb,
            value={"ts_ms": 100},
            kind=EventKind.HEARTBEAT,
        )
        r3 = ChangeRecord(
            destination="t1", schema=s_data, value={"id": 2}, key={"id": 2}
        )
        ts = ChangeRecord(destination="t1", schema=None, value=None, key={"id": 99})

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
        self.assertTrue(diff.is_compatible)
        self.assertEqual([f[0].name for f in diff.changed_fields], ["before", "after"])

    def test_strict_model_validation_raises(self) -> None:
        with self.assertRaises(ValueError):
            ChangeRecord(destination="", value={"id": 1})
        with self.assertRaises(ValueError):
            ChangeRecord(destination="   ", value={"id": 1})

        # Recursive SchemaDiff.is_compatible via native PyArrow schema unification
        s_old = TableSchema(
            destination="t1",
            arrow_schema=pa.schema(
                [
                    pa.field(
                        "meta",
                        pa.struct(
                            [
                                pa.field("a", pa.null()),
                                pa.field("b", pa.int32()),
                            ]
                        ),
                    )
                ]
            ),
        )
        # Promoting nested null -> string and adding nested field "c" is compatible
        s_compat = TableSchema(
            destination="t1",
            arrow_schema=pa.schema(
                [
                    pa.field(
                        "meta",
                        pa.struct(
                            [
                                pa.field("a", pa.string()),
                                pa.field("b", pa.int32()),
                                pa.field("c", pa.bool_()),
                            ]
                        ),
                    )
                ]
            ),
        )
        self.assertTrue(s_compat.diff(s_old).is_compatible)

        # Incompatible nested sibling type change (int32 -> string) is rejected
        s_bad_type = TableSchema(
            destination="t1",
            arrow_schema=pa.schema(
                [
                    pa.field(
                        "meta",
                        pa.struct(
                            [
                                pa.field("a", pa.string()),
                                pa.field("b", pa.string()),
                            ]
                        ),
                    )
                ]
            ),
        )
        self.assertFalse(s_bad_type.diff(s_old).is_compatible)

        # Dropped nested struct field ("b" removed) is rejected
        s_dropped_nested = TableSchema(
            destination="t1",
            arrow_schema=pa.schema(
                [pa.field("meta", pa.struct([pa.field("a", pa.string())]))]
            ),
        )
        self.assertFalse(s_dropped_nested.diff(s_old).is_compatible)

        # Top-level nullability widening (False -> True) is detected and compatible;
        # tightening (True -> False) is detected and incompatible.
        s_non_null = TableSchema(
            destination="t1",
            arrow_schema=pa.schema([pa.field("id", pa.int32(), nullable=False)]),
        )
        s_nullable = TableSchema(
            destination="t1",
            arrow_schema=pa.schema([pa.field("id", pa.int32(), nullable=True)]),
        )
        diff_widen = s_nullable.diff(s_non_null)
        self.assertTrue(diff_widen.has_changes)
        self.assertFalse(diff_widen.is_additive)
        self.assertTrue(diff_widen.is_compatible)
        self.assertEqual(len(diff_widen.changed_fields), 1)

        diff_tighten = s_non_null.diff(s_nullable)
        self.assertTrue(diff_tighten.has_changes)
        self.assertFalse(diff_tighten.is_compatible)
        self.assertEqual(len(diff_tighten.changed_fields), 1)

        # Destination normalization & tombstone / kind invariants (SCH-10)
        trimmed = ChangeRecord(destination="  orders  ", value={"id": 1}, key={"id": 1})
        self.assertEqual(trimmed.destination, "orders")
        with self.assertRaises(ValueError):
            ChangeRecord(destination="orders", value=None, key=None)
        with self.assertRaises(ValueError):
            ChangeRecord(destination="orders", value=None, key={})
        with self.assertRaises(ValueError):
            ChangeRecord(
                destination="__debezium-heartbeat.srv",
                value=None,
                key={"id": 1},
                kind=EventKind.HEARTBEAT,
            )
        with self.assertRaises(ValueError):
            ChangeRecord(
                destination="srv",
                value=None,
                key={"id": 1},
                kind=EventKind.SCHEMA_CHANGE,
            )

        # TableBatch enforces DATA records and matching destination
        ts_rec = ChangeRecord(destination="t1", value=None, key={"id": 1})
        with self.assertRaises(ValueError):
            TableBatch(schema=s_non_null, records=(ts_rec,))
        wrong_dest = ChangeRecord(
            destination="other", value={"id": 1}, schema=s_non_null
        )
        with self.assertRaises(ValueError):
            TableBatch(schema=s_non_null, records=(wrong_dest,))

        # SchemaDiff detects column reordering and primary_keys changes
        s_ab = TableSchema(
            destination="t1",
            arrow_schema=pa.schema([("a", pa.int32()), ("b", pa.string())]),
            primary_keys=("a",),
        )
        s_ba = TableSchema(
            destination="t1",
            arrow_schema=pa.schema([("b", pa.string()), ("a", pa.int32())]),
            primary_keys=("a",),
        )
        diff_reorder = s_ba.diff(s_ab)
        self.assertTrue(diff_reorder.reordered)
        self.assertTrue(diff_reorder.has_changes)
        self.assertFalse(diff_reorder.is_additive)
        self.assertTrue(diff_reorder.is_compatible)

        s_pk_changed = TableSchema(
            destination="t1",
            arrow_schema=pa.schema([("a", pa.int32()), ("b", pa.string())]),
            primary_keys=("a", "b"),
        )
        diff_pk = s_pk_changed.diff(s_ab)
        self.assertTrue(diff_pk.primary_keys_changed)
        self.assertTrue(diff_pk.has_changes)
        self.assertFalse(diff_pk.is_additive)
        self.assertFalse(diff_pk.is_compatible)

    def test_change_batch_splits_runs_on_tombstone_and_schema_change_barriers(
        self,
    ) -> None:
        s = TableSchema(
            destination="orders",
            arrow_schema=pa.schema([("id", pa.int32()), ("name", pa.string())]),
            primary_keys=("id",),
        )
        r_alice = ChangeRecord(
            destination="orders",
            schema=s,
            value={"id": 1, "name": "Alice"},
            key={"id": 1},
        )
        ts_1 = ChangeRecord(destination="orders", value=None, key={"id": 1})
        r_bob = ChangeRecord(
            destination="orders",
            schema=s,
            value={"id": 1, "name": "Bob"},
            key={"id": 1},
        )
        ddl = ChangeRecord(
            destination="inventory",
            value={"ddl": "ALTER TABLE orders ADD COLUMN c INT"},
            kind=EventKind.SCHEMA_CHANGE,
        )
        r_carol = ChangeRecord(
            destination="orders",
            schema=s,
            value={"id": 2, "name": "Carol"},
            key={"id": 2},
        )
        hb = ChangeRecord(
            destination="__debezium-heartbeat.srv",
            value={"ts_ms": 999},
            kind=EventKind.HEARTBEAT,
        )

        batch = ChangeBatch(records=(r_alice, ts_1, r_bob, ddl, r_carol, hb))
        events = batch.destination_events()
        self.assertEqual(len(events), 6)
        self.assertIsInstance(events[0], TableBatch)
        assert isinstance(events[0], TableBatch)
        self.assertEqual(events[0].rows, [{"id": 1, "name": "Alice"}])
        self.assertEqual(events[1], ts_1)
        self.assertIsInstance(events[2], TableBatch)
        assert isinstance(events[2], TableBatch)
        self.assertEqual(events[2].rows, [{"id": 1, "name": "Bob"}])
        self.assertEqual(events[3], ddl)
        self.assertIsInstance(events[4], TableBatch)
        assert isinstance(events[4], TableBatch)
        self.assertEqual(events[4].rows, [{"id": 2, "name": "Carol"}])
        self.assertEqual(events[5], hb)

        t_batches = batch.table_batches()
        self.assertEqual(len(t_batches), 3)
        self.assertEqual(
            [tb.rows for tb in t_batches],
            [
                [{"id": 1, "name": "Alice"}],
                [{"id": 1, "name": "Bob"}],
                [{"id": 2, "name": "Carol"}],
            ],
        )

    def test_table_schema_namespace_and_destination_validation(self) -> None:
        pa_s = pa.schema([("id", pa.int32())])
        s_3part = TableSchema(
            destination="  prod_pg.inventory.orders  ", arrow_schema=pa_s
        )
        self.assertEqual(s_3part.destination, "prod_pg.inventory.orders")
        self.assertEqual(s_3part.namespace, ("prod_pg", "inventory"))
        self.assertEqual(s_3part.table_name, "orders")

        s_2part = TableSchema(destination="inventory.orders", arrow_schema=pa_s)
        self.assertEqual(s_2part.namespace, ("inventory",))
        self.assertEqual(s_2part.table_name, "orders")

        s_1part = TableSchema(destination="orders", arrow_schema=pa_s)
        self.assertEqual(s_1part.namespace, ())
        self.assertEqual(s_1part.table_name, "orders")

        for bad_dest in ("", "   ", None, 123):
            with self.assertRaises(ValueError):
                TableSchema(destination=bad_dest, arrow_schema=pa_s)  # type: ignore[arg-type]

    def test_event_kind_classify_with_debezium_schema_names(self) -> None:
        self.assertEqual(
            EventKind.classify(
                "routed.heartbeat_topic",
                ["ts_ms"],
                "__debezium-heartbeat",
                schema_name="io.debezium.connector.common.Heartbeat",
            ),
            EventKind.HEARTBEAT,
        )
        self.assertEqual(
            EventKind.classify(
                "routed.ddl_history",
                ["source", "ts_ms"],
                "__debezium-heartbeat",
                schema_name="io.debezium.connector.mysql.SchemaChangeValue",
            ),
            EventKind.SCHEMA_CHANGE,
        )
        self.assertEqual(
            EventKind.classify(
                "routed.ddl_history",
                None,
                "__debezium-heartbeat",
                schema_name="io.debezium.connector.mysql.SchemaChangeValue",
            ),
            EventKind.TOMBSTONE,
        )

    def test_schema_diff_lossless_widening_vs_narrowing_and_lossy_promotions(
        self,
    ) -> None:
        def is_compat(old_t: pa.DataType, new_t: pa.DataType) -> bool:
            s_old = TableSchema("t", pa.schema([("col", old_t)]))
            s_new = TableSchema("t", pa.schema([("col", new_t)]))
            diff = s_new.diff(s_old)
            self.assertTrue(diff.has_changes)
            return diff.is_compatible

        # Lossless SQL / Iceberg widenings
        for old_t, new_t in (
            (pa.int8(), pa.int16()),
            (pa.int16(), pa.int32()),
            (pa.int32(), pa.int64()),
            (pa.float32(), pa.float64()),
            (pa.int32(), pa.float64()),
            (pa.decimal128(10, 2), pa.decimal128(18, 2)),
            (pa.decimal128(10, 2), pa.decimal128(12, 4)),
            (pa.timestamp("ms"), pa.timestamp("us")),
            (pa.timestamp("us"), pa.timestamp("ns")),
            (pa.time32("ms"), pa.time64("us")),
            (pa.struct([("a", pa.int32())]), pa.struct([("a", pa.int64())])),
            (pa.list_(pa.int32()), pa.list_(pa.int64())),
        ):
            self.assertTrue(
                is_compat(old_t, new_t),
                msg=f"Expected lossless widening {old_t} -> {new_t} to be compatible",
            )

        # Narrowings, incompatible types, and lossy 64-bit integer / decimal -> floating promotions
        for old_t, new_t in (
            (pa.int64(), pa.int32()),
            (pa.int32(), pa.int16()),
            (pa.float64(), pa.float32()),
            (pa.int32(), pa.float32()),
            (pa.int64(), pa.float64()),
            (pa.uint64(), pa.float64()),
            (pa.decimal128(10, 2), pa.float64()),
            (pa.decimal128(10, 2), pa.float32()),
            (pa.decimal128(18, 2), pa.decimal128(10, 2)),
            (pa.decimal128(10, 2), pa.decimal128(10, 4)),
            (pa.timestamp("us"), pa.timestamp("ms")),
            (pa.time64("us"), pa.time32("ms")),
            (pa.int32(), pa.string()),
            (pa.struct([("a", pa.int64())]), pa.struct([("a", pa.float64())])),
            (
                pa.struct([("a", pa.decimal128(10, 2))]),
                pa.struct([("a", pa.float64())]),
            ),
            (pa.list_(pa.int64()), pa.list_(pa.float64())),
            (
                pa.map_(pa.string(), pa.int64()),
                pa.map_(pa.string(), pa.float64()),
            ),
        ):
            self.assertFalse(
                is_compat(old_t, new_t),
                msg=f"Expected narrowing/lossy {old_t} -> {new_t} to be incompatible",
            )

    def test_table_batch_to_arrow_scales_temporal_units_on_target_schema(self) -> None:
        source_pa = pa.schema(
            [
                pa.field("dropped", pa.string()),
                pa.field("created_at", pa.timestamp("ms")),
                pa.field("event_time", pa.time32("ms")),
            ]
        )
        target_pa = pa.schema(
            [
                pa.field("created_at", pa.timestamp("us")),
                pa.field("event_time", pa.time64("us")),
                pa.field("added", pa.string()),
            ]
        )
        schema = TableSchema(destination="inventory.orders", arrow_schema=source_pa)
        rec = ChangeRecord(
            destination="inventory.orders",
            schema=schema,
            value={
                "dropped": "old",
                "created_at": 1620000000000,
                "event_time": 14_711_000,
            },
            key={"id": 1},
        )
        aligned = TableBatch(schema=schema, records=(rec,)).to_arrow(
            target_schema=target_pa
        )
        self.assertEqual(aligned.schema, target_pa)
        self.assertEqual(
            aligned.to_pylist(),
            [
                {
                    "created_at": datetime.datetime(2021, 5, 3, 0, 0),
                    "event_time": datetime.time(4, 5, 11),
                    "added": None,
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
