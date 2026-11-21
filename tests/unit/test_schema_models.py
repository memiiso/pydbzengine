from __future__ import annotations

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

    def test_table_schema_equality_and_diff(self) -> None:
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
        key_pa = pa.schema([pa.field("id", pa.int32(), nullable=False)])
        s1 = TableSchema(
            destination="inventory.customers",
            arrow_schema=pa_s1,
            key_fields=("id",),
            key_schema=key_pa,
        )
        s2 = TableSchema(
            destination="inventory.customers",
            arrow_schema=pa_s1,
            key_fields=("id",),
            key_schema=key_pa,
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
            key_fields=("id",),
            key_schema=key_pa,
        )
        diff_add: SchemaDiff = s3.diff_from(s1)
        self.assertTrue(diff_add.has_changes)
        self.assertTrue(diff_add.is_additive)
        self.assertTrue(diff_add.is_compatible)
        self.assertEqual(len(diff_add.added_fields), 1)
        self.assertEqual(diff_add.added_fields[0].name, "email")
        self.assertEqual(diff_add.removed_fields, ())
        self.assertEqual(diff_add.changed_fields, ())
        self.assertFalse(diff_add.key_fields_changed)
        self.assertFalse(diff_add.key_schema_changed)

        # Breaking evolution diff (type change + column drop)
        pa_s4 = pa.schema([pa.field("id", pa.int64(), nullable=False)])
        s4 = TableSchema(
            destination="inventory.customers",
            arrow_schema=pa_s4,
            key_fields=("id",),
            key_schema=key_pa,
        )
        diff_break = s4.diff_from(s1)
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
            key_fields=("id",),
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

    def test_tombstone_change_record_requires_explicit_kind(self) -> None:
        schema = TableSchema(
            destination="inventory.customers",
            arrow_schema=pa.schema([pa.field("id", pa.int32())]),
            key_fields=("id",),
        )
        ts_with_schema = ChangeRecord(
            destination="inventory.customers",
            schema=schema,
            value=None,
            key={"id": 42},
            kind=EventKind.TOMBSTONE,
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
            kind=EventKind.TOMBSTONE,
        )
        self.assertEqual(ts_without_schema.kind, EventKind.TOMBSTONE)
        self.assertIsNone(ts_without_schema.schema)
        self.assertEqual(ts_without_schema.destination, "inventory.customers")

        # Default kind=EventKind.DATA with value=None fails fast instead of mutating kind
        with self.assertRaisesRegex(ValueError, "kind 'data' cannot have a null value"):
            ChangeRecord(
                destination="inventory.customers",
                value=None,
                key={"id": 42},
            )
        with self.assertRaisesRegex(ValueError, "kind 'data' cannot have a null value"):
            ChangeRecord(
                destination="inventory.customers",
                value=None,
                key={"id": 42},
                kind=EventKind.DATA,
            )

    def test_table_batch_to_arrow(self) -> None:
        pa_schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("price", pa.decimal128(10, 2)),
            ]
        )
        schema = TableSchema(
            destination="inventory.orders",
            arrow_schema=pa_schema,
            key_fields=("id",),
        )
        self.assertEqual(schema.arrow_schema, pa_schema)
        rec = ChangeRecord(
            destination="inventory.orders",
            schema=schema,
            value={"id": 1, "price": Decimal("19.99")},
            key={"id": 1},
        )
        tb = TableBatch(schema=schema, records=(rec,))
        self.assertEqual(tb.destination, "inventory.orders")
        self.assertEqual(len(tb), 1)
        self.assertEqual(list(tb), [rec])

        tbl = tb.to_arrow()
        self.assertEqual(tbl.num_rows, 1)
        # Arrow PyCapsule Interface (__arrow_c_stream__)
        via_capsule = pa.table(tb)
        self.assertEqual(via_capsule, tbl)
        self.assertEqual(tbl.to_pylist(), [{"id": 1, "price": Decimal("19.99")}])

    def test_table_batch_to_arrow_enforces_target_schema_nullability(self) -> None:
        source_pa = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("name", pa.string(), nullable=True),
            ]
        )
        schema = TableSchema(destination="inventory.users", arrow_schema=source_pa)
        rec_with_null = ChangeRecord(
            destination="inventory.users",
            schema=schema,
            value={"id": 1, "name": None},
            key={"id": 1},
        )
        rec_no_null = ChangeRecord(
            destination="inventory.users",
            schema=schema,
            value={"id": 2, "name": "Alice"},
            key={"id": 2},
        )

        # Missing non-nullable column in target_schema fails fast for non-empty batch
        target_missing_req = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("required_col", pa.string(), nullable=False),
            ]
        )
        with self.assertRaisesRegex(ValueError, "missing non-nullable target field"):
            TableBatch(schema=schema, records=(rec_no_null,)).to_arrow(
                target_schema=target_missing_req
            )

        # Empty batch with missing non-nullable column succeeds (0 nulls)
        empty_tbl = TableBatch(schema=schema, records=()).to_arrow(
            target_schema=target_missing_req
        )
        self.assertEqual(empty_tbl.num_rows, 0)
        self.assertEqual(empty_tbl.schema, target_missing_req)

        # Tightening existing nullable column to nullable=False fails when nulls are present
        target_tight = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("name", pa.string(), nullable=False),
            ]
        )
        with self.assertRaisesRegex(ValueError, "Non-nullable target field 'name'"):
            TableBatch(schema=schema, records=(rec_with_null,)).to_arrow(
                target_schema=target_tight
            )

        # Tightening succeeds when the batch contains no nulls in that column
        aligned = TableBatch(schema=schema, records=(rec_no_null,)).to_arrow(
            target_schema=target_tight
        )
        self.assertEqual(aligned.schema, target_tight)
        self.assertEqual(aligned.to_pylist(), [{"id": 2, "name": "Alice"}])

        # Nested list and map non-nullable target fields reject null elements while permitting null parents
        nested_src = TableSchema(
            destination="inventory.nested",
            arrow_schema=pa.schema(
                [
                    pa.field(
                        "xs", pa.list_(pa.field("item", pa.int32(), nullable=True))
                    ),
                    pa.field(
                        "m",
                        pa.map_(
                            pa.field("k", pa.string(), nullable=False),
                            pa.field("v", pa.int32(), nullable=True),
                        ),
                    ),
                ]
            ),
        )
        nested_target = pa.schema(
            [
                pa.field("xs", pa.list_(pa.field("item", pa.int32(), nullable=False))),
                pa.field(
                    "m",
                    pa.map_(
                        pa.field("k", pa.string(), nullable=False),
                        pa.field("v", pa.int32(), nullable=False),
                    ),
                ),
            ]
        )
        rec_null_list_elem = ChangeRecord(
            destination="inventory.nested",
            schema=nested_src,
            value={"xs": [1, None], "m": {"a": 1}},
        )
        with self.assertRaisesRegex(
            ValueError, r"Non-nullable target field 'xs\.item'"
        ):
            TableBatch(schema=nested_src, records=(rec_null_list_elem,)).to_arrow(
                target_schema=nested_target
            )

        rec_null_map_val = ChangeRecord(
            destination="inventory.nested",
            schema=nested_src,
            value={"xs": [1, 2], "m": {"a": None}},
        )
        with self.assertRaisesRegex(
            ValueError, r"Non-nullable target field 'm\.value'"
        ):
            TableBatch(schema=nested_src, records=(rec_null_map_val,)).to_arrow(
                target_schema=nested_target
            )

        rec_null_parents = ChangeRecord(
            destination="inventory.nested",
            schema=nested_src,
            value={"xs": None, "m": None},
        )
        rec_valid_nested = ChangeRecord(
            destination="inventory.nested",
            schema=nested_src,
            value={"xs": [1, 2], "m": {"a": 10}},
        )
        nested_aligned = TableBatch(
            schema=nested_src, records=(rec_null_parents, rec_valid_nested)
        ).to_arrow(target_schema=nested_target)
        self.assertEqual(nested_aligned.schema, nested_target)
        self.assertEqual(
            nested_aligned.to_pylist(),
            [{"xs": None, "m": None}, {"xs": [1, 2], "m": [("a", 10)]}],
        )

    def test_change_batch_retains_arrival_order_and_slices_composable_sub_batches(
        self,
    ) -> None:
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
        ts1 = ChangeRecord(
            destination="t1",
            schema=None,
            value=None,
            key={"id": 99},
            kind=EventKind.TOMBSTONE,
        )
        ts2 = ChangeRecord(
            destination="t2",
            schema=None,
            value=None,
            key={"id": 100},
            kind=EventKind.TOMBSTONE,
        )

        batch = ChangeBatch(records=(r1, r2, r3, ts1, ts2))
        self.assertEqual(len(batch), 5)
        self.assertEqual(list(batch), [r1, r2, r3, ts1, ts2])

        # of_kind returns composable ChangeBatch instances
        data_sub = batch.of_kind(EventKind.DATA)
        hb_sub = batch.of_kind(EventKind.HEARTBEAT)
        ts_sub = batch.of_kind(EventKind.TOMBSTONE)
        ddl_sub = batch.of_kind(EventKind.SCHEMA_CHANGE)
        self.assertIsInstance(data_sub, ChangeBatch)
        self.assertEqual(data_sub.records, (r1, r3))
        self.assertEqual(hb_sub.records, (r2,))
        self.assertEqual(ts_sub.records, (ts1, ts2))
        self.assertEqual(len(ddl_sub), 0)
        self.assertFalse(bool(ddl_sub))

        # Multi-kind of_kind preserves exact arrival order across selected kinds
        mutations = batch.of_kind(EventKind.DATA, EventKind.TOMBSTONE)
        self.assertEqual(mutations.records, (r1, r3, ts1, ts2))

        # Non-EventKind arguments fail fast (even valid string values)
        with self.assertRaises(TypeError):
            batch.of_kind("tombstone")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            batch.of_kind(EventKind.DATA, "heartbeat")  # type: ignore[arg-type]

        # group_by_kind partitions in a single pass with non-empty sub-batches only
        by_kind = batch.group_by_kind()
        self.assertEqual(
            list(by_kind.keys()),
            [EventKind.DATA, EventKind.HEARTBEAT, EventKind.TOMBSTONE],
        )
        self.assertEqual(by_kind[EventKind.DATA], data_sub)
        self.assertEqual(by_kind[EventKind.HEARTBEAT], hb_sub)
        self.assertEqual(by_kind[EventKind.TOMBSTONE], ts_sub)
        self.assertNotIn(EventKind.SCHEMA_CHANGE, by_kind)

        # Chaining of_kind -> group_by_destination
        ts_by_dest = ts_sub.group_by_destination()
        self.assertEqual(
            ts_by_dest,
            {
                "t1": ChangeBatch(records=(ts1,)),
                "t2": ChangeBatch(records=(ts2,)),
            },
        )

        # to_table_batches() on full batch matches data sub-batch
        table_batches = batch.to_table_batches()
        self.assertEqual(table_batches, data_sub.to_table_batches())
        self.assertEqual(len(table_batches), 1)
        self.assertEqual(table_batches[0].destination, "t1")
        self.assertEqual(len(table_batches[0].rows), 2)

        tbl = table_batches[0].to_arrow()
        self.assertEqual(tbl.num_rows, 2)
        self.assertEqual(tbl.to_pylist(), [{"id": 1}, {"id": 2}])

    def test_envelope_schema_diff_is_additive_for_nullable_struct_additions(
        self,
    ) -> None:
        v1_fields = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
        v2_nullable_add = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("email", pa.string(), nullable=True),
        ]
        v2_non_null_add = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("required_flag", pa.bool_(), nullable=False),
        ]
        v2_widened = [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("email", pa.string(), nullable=True),
        ]
        v2_reordered = [
            pa.field("name", pa.string(), nullable=True),
            pa.field("id", pa.int32(), nullable=False),
            pa.field("email", pa.string(), nullable=True),
        ]

        def make_env(fields: list[pa.Field]) -> TableSchema:
            return TableSchema(
                destination="inventory.customers",
                arrow_schema=pa.schema(
                    [
                        pa.field("before", pa.struct(fields)),
                        pa.field("after", pa.struct(fields)),
                        pa.field("op", pa.string()),
                    ]
                ),
                key_fields=("id",),
            )

        env_v1 = make_env(v1_fields)
        diff_add = make_env(v2_nullable_add).diff_from(env_v1)
        self.assertTrue(diff_add.has_changes)
        self.assertTrue(diff_add.is_additive)
        self.assertTrue(diff_add.is_compatible)
        self.assertEqual(
            [f[0].name for f in diff_add.changed_fields], ["before", "after"]
        )

        # Adding nullable=False subfield inside before/after is neither additive nor compatible
        diff_non_null = make_env(v2_non_null_add).diff_from(env_v1)
        self.assertTrue(diff_non_null.has_changes)
        self.assertFalse(diff_non_null.is_additive)
        self.assertFalse(diff_non_null.is_compatible)

        # Widening + adding subfield is compatible, not purely additive
        diff_widen = make_env(v2_widened).diff_from(env_v1)
        self.assertFalse(diff_widen.is_additive)
        self.assertTrue(diff_widen.is_compatible)

        # Reordering subfields inside struct is not additive
        diff_reorder = make_env(v2_reordered).diff_from(env_v1)
        self.assertFalse(diff_reorder.is_additive)

    def test_schema_diff_rejects_non_nullable_additions_and_tracks_key_schema(
        self,
    ) -> None:
        s1 = TableSchema(
            destination="t1",
            arrow_schema=pa.schema([pa.field("id", pa.int32(), nullable=False)]),
            key_fields=("id",),
            key_schema=pa.schema([pa.field("id", pa.int32(), nullable=False)]),
        )
        # Top-level non-nullable addition is neither additive nor compatible
        s_top_req = TableSchema(
            destination="t1",
            arrow_schema=pa.schema(
                [
                    pa.field("id", pa.int32(), nullable=False),
                    pa.field("tenant", pa.string(), nullable=False),
                ]
            ),
            key_fields=("id",),
            key_schema=s1.key_schema,
        )
        diff_top_req = s_top_req.diff_from(s1)
        self.assertTrue(diff_top_req.has_changes)
        self.assertFalse(diff_top_req.is_additive)
        self.assertFalse(diff_top_req.is_compatible)

        # key_schema type change with unchanged key_fields is tracked
        s_key_widened = TableSchema(
            destination="t1",
            arrow_schema=s1.arrow_schema,
            key_fields=("id",),
            key_schema=pa.schema([pa.field("id", pa.int64(), nullable=False)]),
        )
        diff_key_schema = s_key_widened.diff_from(s1)
        self.assertFalse(diff_key_schema.key_fields_changed)
        self.assertTrue(diff_key_schema.key_schema_changed)
        self.assertTrue(diff_key_schema.has_changes)
        self.assertFalse(diff_key_schema.is_additive)
        self.assertFalse(diff_key_schema.is_compatible)

        # Nested struct inside list adding non-nullable field is incompatible
        s_list_old = TableSchema(
            destination="t1",
            arrow_schema=pa.schema(
                [pa.field("items", pa.list_(pa.struct([pa.field("a", pa.int32())])))]
            ),
        )
        s_list_new_req = TableSchema(
            destination="t1",
            arrow_schema=pa.schema(
                [
                    pa.field(
                        "items",
                        pa.list_(
                            pa.struct(
                                [
                                    pa.field("a", pa.int32()),
                                    pa.field("b", pa.string(), nullable=False),
                                ]
                            )
                        ),
                    )
                ]
            ),
        )
        self.assertFalse(s_list_new_req.diff_from(s_list_old).is_compatible)

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
        # Promoting nested null -> string and adding nullable nested field "c" is compatible
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
        self.assertTrue(s_compat.diff_from(s_old).is_compatible)

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
        self.assertFalse(s_bad_type.diff_from(s_old).is_compatible)

        # Dropped nested struct field ("b" removed) is rejected
        s_dropped_nested = TableSchema(
            destination="t1",
            arrow_schema=pa.schema(
                [pa.field("meta", pa.struct([pa.field("a", pa.string())]))]
            ),
        )
        self.assertFalse(s_dropped_nested.diff_from(s_old).is_compatible)

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
        diff_widen = s_nullable.diff_from(s_non_null)
        self.assertTrue(diff_widen.has_changes)
        self.assertFalse(diff_widen.is_additive)
        self.assertTrue(diff_widen.is_compatible)
        self.assertEqual(len(diff_widen.changed_fields), 1)

        diff_tighten = s_non_null.diff_from(s_nullable)
        self.assertTrue(diff_tighten.has_changes)
        self.assertFalse(diff_tighten.is_compatible)
        self.assertEqual(len(diff_tighten.changed_fields), 1)

        # Destination normalization & tombstone / kind invariants
        trimmed = ChangeRecord(destination="  orders  ", value={"id": 1}, key={"id": 1})
        self.assertEqual(trimmed.destination, "orders")
        with self.assertRaises(ValueError):
            ChangeRecord(
                destination="orders",
                value=None,
                key=None,
                kind=EventKind.TOMBSTONE,
            )
        with self.assertRaises(ValueError):
            ChangeRecord(
                destination="orders",
                value=None,
                key={},
                kind=EventKind.TOMBSTONE,
            )
        with self.assertRaises(ValueError):
            ChangeRecord(
                destination="orders",
                value={"id": 1},
                key={"id": 1},
                kind=EventKind.TOMBSTONE,
            )
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
        ts_rec = ChangeRecord(
            destination="t1", value=None, key={"id": 1}, kind=EventKind.TOMBSTONE
        )
        with self.assertRaises(ValueError):
            TableBatch(schema=s_non_null, records=(ts_rec,))
        wrong_dest = ChangeRecord(
            destination="other", value={"id": 1}, schema=s_non_null
        )
        with self.assertRaises(ValueError):
            TableBatch(schema=s_non_null, records=(wrong_dest,))

        # SchemaDiff detects column reordering and key_fields changes
        s_ab = TableSchema(
            destination="t1",
            arrow_schema=pa.schema([("a", pa.int32()), ("b", pa.string())]),
            key_fields=("a",),
        )
        s_ba = TableSchema(
            destination="t1",
            arrow_schema=pa.schema([("b", pa.string()), ("a", pa.int32())]),
            key_fields=("a",),
        )
        diff_reorder = s_ba.diff_from(s_ab)
        self.assertTrue(diff_reorder.reordered)
        self.assertTrue(diff_reorder.has_changes)
        self.assertFalse(diff_reorder.is_additive)
        self.assertTrue(diff_reorder.is_compatible)

        s_pk_changed = TableSchema(
            destination="t1",
            arrow_schema=pa.schema([("a", pa.int32()), ("b", pa.string())]),
            key_fields=("a", "b"),
        )
        diff_pk = s_pk_changed.diff_from(s_ab)
        self.assertTrue(diff_pk.key_fields_changed)
        self.assertTrue(diff_pk.has_changes)
        self.assertFalse(diff_pk.is_additive)
        self.assertFalse(diff_pk.is_compatible)

    def test_change_batch_table_batches_does_not_fragment_on_tombstones_and_splits_on_schema_evolution(
        self,
    ) -> None:
        s_orders_v1 = TableSchema(
            destination="orders",
            arrow_schema=pa.schema([("id", pa.int32()), ("name", pa.string())]),
            key_fields=("id",),
        )
        s_orders_v2 = TableSchema(
            destination="orders",
            arrow_schema=pa.schema(
                [("id", pa.int32()), ("name", pa.string()), ("status", pa.string())]
            ),
            key_fields=("id",),
        )
        s_customers = TableSchema(
            destination="customers",
            arrow_schema=pa.schema([("id", pa.int32())]),
            key_fields=("id",),
        )

        r_o1 = ChangeRecord(
            destination="orders",
            schema=s_orders_v1,
            value={"id": 1, "name": "Alice"},
            key={"id": 1},
        )
        ts_1 = ChangeRecord(
            destination="orders",
            value=None,
            key={"id": 1},
            kind=EventKind.TOMBSTONE,
        )
        r_c1 = ChangeRecord(
            destination="customers",
            schema=s_customers,
            value={"id": 10},
            key={"id": 10},
        )
        r_o2 = ChangeRecord(
            destination="orders",
            schema=s_orders_v1,
            value={"id": 2, "name": "Bob"},
            key={"id": 2},
        )
        ts_2 = ChangeRecord(
            destination="orders",
            value=None,
            key={"id": 2},
            kind=EventKind.TOMBSTONE,
        )
        ddl = ChangeRecord(
            destination="inventory",
            value={"ddl": "ALTER TABLE orders ADD COLUMN status VARCHAR"},
            kind=EventKind.SCHEMA_CHANGE,
        )
        r_o3 = ChangeRecord(
            destination="orders",
            schema=s_orders_v2,
            value={"id": 3, "name": "Carol", "status": "open"},
            key={"id": 3},
        )
        r_c2 = ChangeRecord(
            destination="customers",
            schema=s_customers,
            value={"id": 20},
            key={"id": 20},
        )
        hb = ChangeRecord(
            destination="__debezium-heartbeat.srv",
            value={"ts_ms": 999},
            kind=EventKind.HEARTBEAT,
        )

        batch = ChangeBatch(records=(r_o1, ts_1, r_c1, r_o2, ts_2, ddl, r_o3, r_c2, hb))
        t_batches = batch.to_table_batches()
        self.assertEqual(len(t_batches), 3)
        self.assertEqual(t_batches[0].schema, s_orders_v1)
        self.assertEqual(
            t_batches[0].rows,
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        )
        self.assertEqual(t_batches[1].schema, s_orders_v2)
        self.assertEqual(
            t_batches[1].rows, [{"id": 3, "name": "Carol", "status": "open"}]
        )
        self.assertEqual(t_batches[2].schema, s_customers)
        self.assertEqual(t_batches[2].rows, [{"id": 10}, {"id": 20}])

        # Non-DATA-only batch yields empty tuple
        self.assertEqual(ChangeBatch(records=(ts_1, ddl, hb)).to_table_batches(), ())

        # DATA record without TableSchema fails fast
        missing_schema = ChangeRecord(
            destination="orders", schema=None, value={"id": 1}
        )
        with self.assertRaisesRegex(ValueError, "missing a TableSchema"):
            ChangeBatch(records=(missing_schema,)).to_table_batches()

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
            diff = s_new.diff_from(s_old)
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

    def test_table_batch_to_arrow_evolves_nested_structs_on_target_schema(
        self,
    ) -> None:
        v1_row = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("legacy", pa.string(), nullable=True),
        ]
        v2_row = [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("email", pa.string(), nullable=True),
        ]
        source_pa = pa.schema(
            [
                pa.field("before", pa.struct(v1_row), nullable=True),
                pa.field("after", pa.struct(v1_row), nullable=True),
                pa.field("op", pa.string(), nullable=False),
            ]
        )
        target_pa = pa.schema(
            [
                pa.field("before", pa.struct(v2_row), nullable=True),
                pa.field("after", pa.struct(v2_row), nullable=True),
                pa.field("op", pa.string(), nullable=False),
            ]
        )
        schema = TableSchema(
            destination="inventory.customers",
            arrow_schema=source_pa,
            key_fields=("id",),
        )
        rec_insert = ChangeRecord(
            destination="inventory.customers",
            schema=schema,
            value={
                "before": None,
                "after": {"id": 1, "name": "Alice", "legacy": "x"},
                "op": "c",
            },
            key={"id": 1},
        )
        rec_delete = ChangeRecord(
            destination="inventory.customers",
            schema=schema,
            value={
                "before": {"id": 2, "name": "Bob", "legacy": "y"},
                "after": None,
                "op": "d",
            },
            key={"id": 2},
        )

        aligned = TableBatch(schema=schema, records=(rec_insert, rec_delete)).to_arrow(
            target_schema=target_pa
        )
        self.assertEqual(aligned.schema, target_pa)
        self.assertEqual(
            aligned.to_pylist(),
            [
                {
                    "before": None,
                    "after": {"id": 1, "name": "Alice", "email": None},
                    "op": "c",
                },
                {
                    "before": {"id": 2, "name": "Bob", "email": None},
                    "after": None,
                    "op": "d",
                },
            ],
        )

        # Adding a missing non-nullable subfield to a struct fails fast in PyArrow cast
        target_req_sub = pa.schema(
            [
                pa.field(
                    "after",
                    pa.struct(
                        [
                            pa.field("id", pa.int32(), nullable=False),
                            pa.field("required_flag", pa.bool_(), nullable=False),
                        ]
                    ),
                    nullable=True,
                ),
            ]
        )
        with self.assertRaises(pa.ArrowTypeError):
            TableBatch(schema=schema, records=(rec_insert,)).to_arrow(
                target_schema=target_req_sub
            )

        # Tightening an existing nullable subfield fails when null values exist
        # (ArrowTypeError on pyarrow 19.x, ArrowInvalid on pyarrow 20.x+)
        target_tight_sub = pa.schema(
            [
                pa.field(
                    "after",
                    pa.struct(
                        [
                            pa.field("id", pa.int32(), nullable=False),
                            pa.field("name", pa.string(), nullable=False),
                        ]
                    ),
                    nullable=True,
                ),
            ]
        )
        rec_null_name = ChangeRecord(
            destination="inventory.customers",
            schema=schema,
            value={
                "before": None,
                "after": {"id": 3, "name": None, "legacy": None},
                "op": "c",
            },
            key={"id": 3},
        )
        with self.assertRaises(pa.ArrowException):
            TableBatch(schema=schema, records=(rec_null_name,)).to_arrow(
                target_schema=target_tight_sub
            )


if __name__ == "__main__":
    unittest.main()
