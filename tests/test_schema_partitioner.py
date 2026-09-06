from __future__ import annotations

import json
import unittest
from typing import Any

from mock_events import MockChangeEvent

from pydbzengine.schema.base import StreamPartitioner
from pydbzengine.schema.readers import DebeziumSchemaReader


def make_event(
    fields_dict: dict[str, tuple[str, Any]],
    op: str = "c",
    dest: str = "test.customers",
) -> MockChangeEvent:
    schema_fields: list[dict[str, Any]] = []
    payload_after: dict[str, Any] = {}
    for name, (type_str, val) in fields_dict.items():
        schema_fields.append({"type": type_str, "field": name, "optional": True})
        payload_after[name] = val

    val_json = {
        "schema": {
            "type": "struct",
            "name": f"{dest}.Envelope",
            "fields": [
                {"type": "string", "field": "op", "optional": False},
                {
                    "type": "struct",
                    "field": "after",
                    "fields": schema_fields,
                    "optional": True,
                },
            ],
        },
        "payload": {
            "op": op,
            "after": payload_after,
        },
    }
    return MockChangeEvent(
        key=json.dumps({"id": payload_after.get("id", 1)}),
        value=json.dumps(val_json),
        destination=dest,
    )


class TestSchemaPartitioner(unittest.TestCase):
    def setUp(self) -> None:
        self.reader = DebeziumSchemaReader(include_metadata=False)

    def test_empty_records(self) -> None:
        chunks_direct = list(StreamPartitioner.chunk_by_schema([], self.reader))
        self.assertEqual(len(chunks_direct), 0)

    def test_single_schema_stream(self) -> None:
        records = [
            make_event({"id": ("int64", 1), "name": ("string", "Alice")}),
            make_event({"id": ("int64", 2), "name": ("string", "Bob")}),
            make_event({"id": ("int64", 3), "name": ("string", "Charlie")}),
        ]
        chunks = list(StreamPartitioner.chunk_by_schema(records, self.reader))
        self.assertEqual(len(chunks), 1)
        schema, chunk_records = chunks[0]
        self.assertEqual(len(chunk_records), 3)
        self.assertEqual(schema.field_names, ["id", "name"])

    def test_mid_batch_schema_evolution(self) -> None:
        # 2 records of V1 (id, name), then 2 records of V2 (id, name, email), then 1 of V3 (id, name, email, age)
        records = [
            make_event({"id": ("int64", 1), "name": ("string", "Alice")}),
            make_event({"id": ("int64", 2), "name": ("string", "Bob")}),
            make_event(
                {
                    "id": ("int64", 3),
                    "name": ("string", "Charlie"),
                    "email": ("string", "c@test.com"),
                }
            ),
            make_event(
                {
                    "id": ("int64", 4),
                    "name": ("string", "David"),
                    "email": ("string", "d@test.com"),
                }
            ),
            make_event(
                {
                    "id": ("int64", 5),
                    "name": ("string", "Eve"),
                    "email": ("string", "e@test.com"),
                    "age": ("int32", 30),
                }
            ),
        ]
        chunks = list(StreamPartitioner.chunk_by_schema(records, self.reader))
        self.assertEqual(len(chunks), 3)

        # Chunk 1: V1
        schema1, chunk1 = chunks[0]
        self.assertEqual(len(chunk1), 2)
        self.assertEqual(schema1.field_names, ["id", "name"])

        # Chunk 2: V2
        schema2, chunk2 = chunks[1]
        self.assertEqual(len(chunk2), 2)
        self.assertEqual(schema2.field_names, ["id", "name", "email"])

        # Chunk 3: V3
        schema3, chunk3 = chunks[2]
        self.assertEqual(len(chunk3), 1)
        self.assertEqual(schema3.field_names, ["id", "name", "email", "age"])

    def test_alternating_schema_boundaries(self) -> None:
        # V1 -> V2 -> V1
        v1_1 = make_event({"id": ("int64", 1)})
        v2 = make_event({"id": ("int64", 2), "extra": ("string", "foo")})
        v1_2 = make_event({"id": ("int64", 3)})

        chunks = list(StreamPartitioner.chunk_by_schema([v1_1, v2, v1_2], self.reader))
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0][0].field_names, ["id"])
        self.assertEqual(chunks[1][0].field_names, ["id", "extra"])
        self.assertEqual(chunks[2][0].field_names, ["id"])

    def test_skips_empty_whitespace_records(self) -> None:
        r1 = make_event({"id": ("int64", 1)})
        empty = MockChangeEvent(key="", value="   ", destination="test.customers")
        none_rec = MockChangeEvent(key="", value=None, destination="test.customers")
        r2 = make_event({"id": ("int64", 2)})

        chunks = list(
            StreamPartitioner.chunk_by_schema([r1, empty, none_rec, r2], self.reader)
        )
        self.assertEqual(len(chunks), 1)
        self.assertEqual(len(chunks[0][1]), 2)


if __name__ == "__main__":
    unittest.main()
