from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pydbzengine.schema import (
    SchemaDiff,
    TableSchema,
    make_decimal_coercer,
)


class TestSchemaModels(unittest.TestCase):
    def test_table_schema_fingerprint_and_hashability(self) -> None:
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
        schema1 = TableSchema(
            destination="inventory.customers",
            arrow_schema=pa_s1,
            primary_keys=("id",),
        )

        pa_s2 = pa.schema(
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
        schema2 = TableSchema(
            destination="inventory.customers",
            arrow_schema=pa_s2,
            primary_keys=("id",),
        )

        self.assertTrue(schema1.same_schema(schema2))
        self.assertTrue(schema1.same_schema(pa_s2))
        self.assertEqual(schema1, schema2)
        self.assertEqual(hash(schema1), hash(schema2))
        self.assertEqual(len({schema1, schema2}), 1)
        self.assertEqual(schema1.fingerprint, schema2.fingerprint)
        self.assertGreater(len(schema1.fingerprint), 20)
        self.assertEqual(schema1.column_names, ["id", "name"])
        self.assertEqual(schema1.table_name, "customers")
        self.assertEqual(schema1.schema_name, "inventory")
        self.assertEqual(schema1.namespace, "inventory.customers")
        self.assertIsNotNone(schema1.field("id"))
        self.assertIsNone(schema1.field("unknown"))

    def test_table_schema_fingerprint_changes_on_field_or_metadata_diff(
        self,
    ) -> None:
        s1 = TableSchema(
            destination="docs",
            arrow_schema=pa.schema(
                [
                    pa.field(
                        "content",
                        pa.string(),
                        metadata={b"debezium.logical_type": b"io.debezium.data.Xml"},
                    )
                ]
            ),
        )
        s2 = TableSchema(
            destination="docs",
            arrow_schema=pa.schema([pa.field("content", pa.string())]),
        )
        self.assertNotEqual(s1.fingerprint, s2.fingerprint)
        self.assertNotEqual(s1, s2)
        self.assertFalse(s1.same_schema(s2))

    def test_schema_diff_added_removed_widened_nullability(self) -> None:
        target = TableSchema(
            destination="orders",
            arrow_schema=pa.schema(
                [
                    pa.field("id", pa.int32(), nullable=False),
                    pa.field("legacy_code", pa.string(), nullable=True),
                    pa.field("amount", pa.int32(), nullable=False),
                ]
            ),
            primary_keys=("id",),
        )
        incoming = TableSchema(
            destination="orders",
            arrow_schema=pa.schema(
                [
                    pa.field("id", pa.int32(), nullable=False),
                    pa.field("amount", pa.int64(), nullable=True),
                    pa.field("discount", pa.decimal128(10, 2), nullable=True),
                ]
            ),
            primary_keys=("id",),
        )

        diff: SchemaDiff = incoming.diff_from(target)
        self.assertEqual(diff, incoming.diff_from(target.arrow_schema))
        self.assertTrue(diff.has_changes)
        self.assertTrue(diff.has_expansion)
        self.assertEqual([f.name for f in diff.added_fields], ["discount"])
        self.assertEqual([f.name for f in diff.removed_fields], ["legacy_code"])
        self.assertEqual(len(diff.type_changes), 1)
        self.assertEqual(diff.type_changes[0][0].type, pa.int32())
        self.assertEqual(diff.type_changes[0][1].type, pa.int64())
        self.assertEqual(len(diff.nullability_changes), 1)
        self.assertFalse(diff.nullability_changes[0][0].nullable)
        self.assertTrue(diff.nullability_changes[0][1].nullable)

    def test_expand_target_schema_union_by_name_and_promotion(self) -> None:
        target_pa = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("dropped_col", pa.string(), nullable=False),
                pa.field("count", pa.int32(), nullable=False),
                pa.field("ratio", pa.float32(), nullable=True),
                pa.field("price", pa.decimal128(8, 2), nullable=True),
                pa.field(
                    "owner",
                    pa.struct([pa.field("name", pa.string(), nullable=False)]),
                ),
            ]
        )
        target_ts = TableSchema(
            destination="orders",
            arrow_schema=target_pa,
            primary_keys=("id",),
            row_coercers=(("price", make_decimal_coercer(2)),),
        )
        incoming = TableSchema(
            destination="orders",
            arrow_schema=pa.schema(
                [
                    pa.field("id", pa.int32(), nullable=False),
                    pa.field("count", pa.int64(), nullable=True),
                    pa.field("ratio", pa.float64(), nullable=True),
                    pa.field("price", pa.decimal128(12, 4), nullable=True),
                    pa.field(
                        "owner",
                        pa.struct(
                            [
                                pa.field("name", pa.string(), nullable=False),
                                pa.field("email", pa.string(), nullable=False),
                            ]
                        ),
                    ),
                    pa.field("new_col", pa.string(), nullable=False),
                ]
            ),
            row_coercers=(("price", make_decimal_coercer(4)),),
        )

        expanded = incoming.expand_target_schema(target_pa)
        evolved = target_ts.evolve(incoming)
        self.assertEqual(evolved.arrow_schema, expanded)
        self.assertEqual(evolved.primary_keys, ("id",))
        self.assertEqual([c for c, _ in evolved.row_coercers], ["price"])
        self.assertEqual(
            expanded.names,
            ["id", "dropped_col", "count", "ratio", "price", "owner", "new_col"],
        )
        # Dropped column retained as nullable
        self.assertTrue(expanded.field("dropped_col").nullable)
        # Promoted types
        self.assertEqual(expanded.field("count").type, pa.int64())
        self.assertTrue(expanded.field("count").nullable)
        self.assertEqual(expanded.field("ratio").type, pa.float64())
        self.assertEqual(expanded.field("price").type, pa.decimal128(12, 4))
        # Nested struct expanded with email as nullable
        owner_struct = expanded.field("owner").type
        self.assertTrue(pa.types.is_struct(owner_struct))
        self.assertEqual([f.name for f in owner_struct], ["name", "email"])
        self.assertTrue(owner_struct.field("email").nullable)
        # New top-level column appended as nullable
        self.assertTrue(expanded.field("new_col").nullable)

    def test_table_schema_to_arrow_align_table_and_parquet_roundtrip(self) -> None:
        incoming_schema = TableSchema(
            destination="dbserver1.inventory.orders",
            arrow_schema=pa.schema(
                [
                    pa.field("id", pa.int32(), nullable=False),
                    pa.field("price", pa.decimal128(10, 2), nullable=True),
                    pa.field("new_col", pa.string(), nullable=True),
                ]
            ),
            primary_keys=("id",),
            row_coercers=(("price", make_decimal_coercer(2)),),
        )
        row = {"id": 1, "price": "19.99", "new_col": "v2"}
        tbl = incoming_schema.to_arrow([row])
        self.assertEqual(tbl.num_rows, 1)

        target_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("legacy_col", pa.string(), nullable=True),
                pa.field("price", pa.decimal128(10, 2), nullable=True),
                pa.field("new_col", pa.string(), nullable=True),
            ]
        )

        aligned = incoming_schema.align_table(tbl, target_schema)
        self.assertEqual(aligned.schema, target_schema)
        self.assertEqual(
            aligned.to_pylist(),
            [
                {
                    "id": 1,
                    "legacy_col": None,
                    "price": Decimal("19.99"),
                    "new_col": "v2",
                }
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            pq_path = Path(tmpdir) / "part-0.parquet"
            pq.write_table(aligned, pq_path)
            read_back = pq.read_table(pq_path)
            self.assertEqual(
                read_back.to_pylist(),
                [
                    {
                        "id": 1,
                        "legacy_col": None,
                        "price": Decimal("19.99"),
                        "new_col": "v2",
                    }
                ],
            )

    def test_incompatible_type_promotion_raises(self) -> None:
        target = pa.schema([pa.field("col", pa.int64())])
        incoming_incompatible = TableSchema(
            destination="orders",
            arrow_schema=pa.schema([pa.field("col", pa.string())]),
        )
        with self.assertRaises(TypeError):
            incoming_incompatible.expand_target_schema(target)

        target_dec = pa.schema([pa.field("amount", pa.decimal128(35, 2))])
        incoming_dec = TableSchema(
            destination="orders",
            arrow_schema=pa.schema([pa.field("amount", pa.decimal128(35, 10))]),
        )
        with self.assertRaises(ValueError):
            incoming_dec.expand_target_schema(target_dec)

    def test_nested_struct_alignment_and_nullability_expansion(self) -> None:
        # Target already has nullable=True; incoming has nullable=False -> has_changes=True, has_expansion=False
        target = pa.schema([pa.field("id", pa.int32(), nullable=True)])
        incoming = TableSchema(
            destination="orders",
            arrow_schema=pa.schema([pa.field("id", pa.int32(), nullable=False)]),
        )
        diff = incoming.diff_from(target)
        self.assertTrue(diff.has_changes)
        self.assertFalse(diff.has_expansion)

        # Nested struct alignment: incoming struct has {"name": str}, target has {"name": str, "age": int64}
        src_schema = TableSchema(
            destination="users",
            arrow_schema=pa.schema(
                [
                    pa.field("id", pa.int32()),
                    pa.field("profile", pa.struct([pa.field("name", pa.string())])),
                ]
            ),
        )
        tbl = src_schema.to_arrow(
            [{"id": 1, "profile": {"name": "Alice"}}, {"id": 2, "profile": None}]
        )
        target_struct_schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field(
                    "profile",
                    pa.struct(
                        [
                            pa.field("name", pa.string()),
                            pa.field("age", pa.int64()),
                        ]
                    ),
                ),
            ]
        )
        aligned = src_schema.align_table(tbl, target_struct_schema)
        self.assertEqual(aligned.schema, target_struct_schema)
        self.assertEqual(
            aligned.to_pylist(),
            [
                {"id": 1, "profile": {"name": "Alice", "age": None}},
                {"id": 2, "profile": None},
            ],
        )

    def test_null_promotion_temporal_widening_and_list_struct_alignment(self) -> None:
        # 1. pa.null() promotes to concrete type and vice versa
        target_null = pa.schema(
            [
                pa.field("col_null", pa.null()),
                pa.field("ts", pa.timestamp("ms")),
            ]
        )
        incoming = TableSchema(
            destination="events",
            arrow_schema=pa.schema(
                [
                    pa.field("col_null", pa.int64()),
                    pa.field("ts", pa.timestamp("us")),
                ]
            ),
        )
        expanded = incoming.expand_target_schema(target_null)
        self.assertEqual(expanded.field("col_null").type, pa.int64())
        self.assertEqual(expanded.field("ts").type, pa.timestamp("us"))

        # 2. List of structs alignment when struct gains a new field
        src_list_schema = TableSchema(
            destination="events",
            arrow_schema=pa.schema(
                [
                    pa.field(
                        "items",
                        pa.list_(pa.struct([pa.field("name", pa.string())])),
                    )
                ]
            ),
        )
        tbl = src_list_schema.to_arrow(
            [{"items": [{"name": "a"}, {"name": "b"}]}, {"items": None}]
        )
        target_list_schema = pa.schema(
            [
                pa.field(
                    "items",
                    pa.list_(
                        pa.struct(
                            [
                                pa.field("name", pa.string()),
                                pa.field("age", pa.int64()),
                            ]
                        )
                    ),
                )
            ]
        )
        aligned = src_list_schema.align_table(tbl, target_list_schema)
        self.assertEqual(
            aligned.to_pylist(),
            [
                {
                    "items": [
                        {"name": "a", "age": None},
                        {"name": "b", "age": None},
                    ]
                },
                {"items": None},
            ],
        )


if __name__ == "__main__":
    unittest.main()
