from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pydbzengine.schema import TableSchema, unify_arrow_schemas
from pydbzengine.schema.coercers import make_decimal_coercer


class TestSchemaModels(unittest.TestCase):
    def test_table_schema_equality_and_hashability(self) -> None:
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
        self.assertEqual(schema1.names, ["id", "name"])
        self.assertEqual(schema1.field_names, ["id", "name"])
        self.assertEqual(schema1.column_names, ["id", "name"])
        self.assertIn("id", schema1)
        self.assertNotIn("unknown", schema1)
        self.assertEqual(schema1.table_name, "customers")
        self.assertEqual(schema1.schema_name, "inventory")
        self.assertEqual(schema1.namespace, "inventory.customers")
        self.assertIsNotNone(schema1.field("id"))
        self.assertIsNone(schema1.field("unknown"))

    def test_table_schema_inequality_on_field_or_metadata_diff(self) -> None:
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
        self.assertNotEqual(s1, s2)
        self.assertFalse(s1.same_schema(s2))

    def test_table_schema_evolve_and_unify_arrow_schemas(self) -> None:
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
                                pa.field("email", pa.string(), nullable=True),
                            ]
                        ),
                    ),
                    pa.field("new_col", pa.string(), nullable=False),
                ]
            ),
            row_coercers=(("price", make_decimal_coercer(4)),),
        )

        evolved = target_ts.evolve(incoming)
        expanded = evolved.arrow_schema
        self.assertEqual(evolved.primary_keys, ("id",))
        self.assertEqual([c for c, _ in evolved.row_coercers], ["price"])
        self.assertEqual(
            expanded.names,
            ["id", "dropped_col", "count", "ratio", "price", "owner", "new_col"],
        )
        self.assertTrue(expanded.field("dropped_col").nullable)
        self.assertEqual(expanded.field("count").type, pa.int64())
        self.assertTrue(expanded.field("count").nullable)
        self.assertEqual(expanded.field("ratio").type, pa.float64())
        self.assertEqual(expanded.field("price").type, pa.decimal128(12, 4))
        owner_struct = expanded.field("owner").type
        self.assertTrue(pa.types.is_struct(owner_struct))
        self.assertEqual([f.name for f in owner_struct], ["name", "email"])
        self.assertTrue(expanded.field("new_col").nullable)

    def test_table_schema_to_arrow_with_target_schema_and_parquet_roundtrip(
        self,
    ) -> None:
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
        target_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("legacy_col", pa.string(), nullable=True),
                pa.field("price", pa.decimal128(10, 2), nullable=True),
                pa.field("new_col", pa.string(), nullable=True),
            ]
        )
        row = {"id": 1, "price": "19.99", "new_col": "v2"}
        aligned = incoming_schema.to_arrow([row], target_schema=target_schema)
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

    def test_incompatible_type_unification_raises(self) -> None:
        target = TableSchema(
            destination="orders",
            arrow_schema=pa.schema([pa.field("col", pa.int64())]),
        )
        incoming_incompatible = TableSchema(
            destination="orders",
            arrow_schema=pa.schema([pa.field("col", pa.string())]),
        )
        with self.assertRaises(TypeError):
            target.evolve(incoming_incompatible)

    def test_nested_struct_and_list_alignment_via_pyarrow_cpp(self) -> None:
        target_null = pa.schema(
            [
                pa.field("col_null", pa.null()),
                pa.field("ts", pa.timestamp("ms")),
            ]
        )
        incoming_pa = pa.schema(
            [
                pa.field("col_null", pa.int64()),
                pa.field("ts", pa.timestamp("us")),
            ]
        )
        expanded = unify_arrow_schemas([target_null, incoming_pa])
        self.assertEqual(expanded.field("col_null").type, pa.int64())
        self.assertEqual(expanded.field("ts").type, pa.timestamp("us"))

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
        aligned = src_list_schema.to_arrow(
            [{"items": [{"name": "a"}, {"name": "b"}]}, {"items": None}],
            target_schema=target_list_schema,
        )
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
