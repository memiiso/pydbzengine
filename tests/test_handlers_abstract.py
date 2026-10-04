from __future__ import annotations

import unittest
from collections.abc import Sequence
from typing import Any

from mock_events import MockChangeEvent

from pydbzengine.engine import BasePythonChangeHandler, ChangeEvent
from pydbzengine.schema.models import (
    CanonicalField,
    CanonicalPrimitiveType,
    CanonicalSchema,
    CanonicalType,
)
from pydbzengine.schema.partitioner import StreamChunk, StreamPartitioner
from pydbzengine.schema.readers import DebeziumSchemaReader


class MockTable:
    """In-memory mock table for testing handler coordination without external engines."""

    def __init__(self, name: str, schema: CanonicalSchema) -> None:
        self.name = name
        self.schema = schema
        self.rows: list[dict[str, Any]] = []


def make_test_event(dest: str, field_names: list[str]) -> MockChangeEvent:
    schema_fields = [
        {"type": "string", "field": f, "optional": True} for f in field_names
    ]
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
            "op": "c",
            "after": {f: "val" for f in field_names},
        },
    }
    import json

    return MockChangeEvent(
        key=json.dumps({"id": 1}),
        value=json.dumps(val_json),
        destination=dest,
    )


class ConcreteMockSink(BasePythonChangeHandler):
    """A realistic concrete sink implementing BasePythonChangeHandler directly."""

    def __init__(self, schema_reader: DebeziumSchemaReader) -> None:
        self.schema_reader = schema_reader
        self.tables: dict[str, MockTable] = {}
        self.evolution_count = 0
        self.total_processed = 0

    def handle_batch(self, records: list[ChangeEvent]) -> None:
        self.handleJsonBatch(records)

    def handleJsonBatch(self, records: list[ChangeEvent]) -> None:
        readable = [r for r in records if self.schema_reader.can_read(r)]
        if not readable:
            return

        for dest, dest_records in self.group_by_destination(readable).items():
            for chunk in StreamPartitioner.chunk_by_schema(
                dest_records, self.schema_reader
            ):
                if dest not in self.tables:
                    self.tables[dest] = MockTable(dest, chunk.schema)
                elif chunk.schema.fingerprint != self.tables[dest].schema.fingerprint:
                    self.tables[dest].schema = chunk.schema
                    self.evolution_count += 1

                self.tables[dest].rows.extend(
                    [{"dest": r.destination(), "val": r.value()} for r in chunk.records]
                )
                self.total_processed += len(chunk.records)


class TestHandlersAbstractDesign(unittest.TestCase):
    def setUp(self) -> None:
        self.reader = DebeziumSchemaReader()

    def test_stream_chunk_named_and_tuple_access(self) -> None:
        """Verifies StreamChunk works with both attribute access and tuple unpacking."""
        dummy_schema = CanonicalSchema(
            identifier="test",
            fields=(
                CanonicalField(
                    name="id",
                    field_type=CanonicalType(primitive=CanonicalPrimitiveType.INT64),
                ),
            ),
        )
        rec = make_test_event("test", ["id"])
        chunk = StreamChunk(schema=dummy_schema, records=[rec])

        # Attribute access
        self.assertEqual(chunk.schema, dummy_schema)
        self.assertEqual(len(chunk.records), 1)

        # Tuple unpacking
        schema, records = chunk
        self.assertEqual(schema, dummy_schema)
        self.assertEqual(len(records), 1)

        # Index access & len
        self.assertEqual(chunk[0], dummy_schema)
        self.assertEqual(chunk[1], [rec])
        self.assertEqual(len(chunk), 2)

    def test_stream_partitioner_chunk_by_schema(self) -> None:
        """Verifies StreamPartitioner.chunk_by_schema slices runs correctly."""
        records = [
            make_test_event("orders", ["id", "amount"]),
            make_test_event("orders", ["id", "amount"]),
            make_test_event(
                "orders", ["id", "amount", "discount"]
            ),  # Schema evolution!
        ]
        chunks = list(StreamPartitioner.chunk_by_schema(records, self.reader))
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0].schema.field_names, ["id", "amount"])
        self.assertEqual(len(chunks[0].records), 2)
        self.assertEqual(chunks[1].schema.field_names, ["id", "amount", "discount"])
        self.assertEqual(len(chunks[1].records), 1)

    def test_base_python_change_handler_destination_grouping(self) -> None:
        """Verifies BasePythonChangeHandler.group_by_destination cleanly separates interleaved tables."""
        records: list[ChangeEvent] = [
            make_test_event("table_a", ["id"]),
            make_test_event("table_b", ["id"]),
            make_test_event("table_a", ["id", "extra"]),
            make_test_event("table_b", ["id"]),
        ]
        sink = ConcreteMockSink(self.reader)
        grouped = sink.group_by_destination(records)
        self.assertEqual(set(grouped.keys()), {"table_a", "table_b"})
        self.assertEqual(len(grouped["table_a"]), 2)
        self.assertEqual(len(grouped["table_b"]), 2)

    def test_base_python_change_handler_empty_destination_raises(self) -> None:
        """Verifies group_by_destination fails fast on records with missing/empty destination."""
        empty_dest_record = make_test_event("", ["id"])
        sink = ConcreteMockSink(self.reader)
        with self.assertRaisesRegex(ValueError, "empty or missing destination"):
            sink.group_by_destination([empty_dest_record])

    def test_base_python_change_handler_custom_routing_override(self) -> None:
        """Verifies a subclass can override group_by_destination to customize table routing."""

        class CustomRoutingSink(ConcreteMockSink):
            def group_by_destination(
                self, records: Sequence[ChangeEvent]
            ) -> dict[str, list[ChangeEvent]]:
                res: dict[str, list[ChangeEvent]] = {}
                for r in records:
                    routed = f"tenant_{r.destination()}"
                    res.setdefault(routed, []).append(r)
                return res

        sink = CustomRoutingSink(self.reader)
        records = [make_test_event("orders", ["id"])]
        grouped = sink.group_by_destination(records)
        self.assertIn("tenant_orders", grouped)

    def test_base_python_change_handler_pythonic_handle_batch(self) -> None:
        """Verifies handler implementing pythonic handle_batch is invoked via handleJsonBatch."""
        called: list[int] = []

        class PythonicSink(BasePythonChangeHandler):
            def handle_batch(self, records: list[ChangeEvent]) -> int | None:
                called.append(len(records))
                return len(records)

        handler = PythonicSink()
        rec = make_test_event("test", ["id"])
        handler.handleJsonBatch([rec])
        self.assertEqual(called, [1])

    def test_concrete_sink_orchestration_and_evolution(self) -> None:
        """Verifies concrete sink handles batches, groups by table, and evolves schema mid-batch."""
        sink = ConcreteMockSink(self.reader)
        records: list[ChangeEvent] = [
            # customers batch: V1 -> V2
            make_test_event("customers", ["id", "name"]),
            make_test_event("customers", ["id", "name"]),
            make_test_event("customers", ["id", "name", "email"]),  # V2
            # orders batch: V1
            make_test_event("orders", ["order_id", "total"]),
            make_test_event("orders", ["order_id", "total"]),
        ]

        sink.handleJsonBatch(records)
        self.assertEqual(sink.total_processed, 5)

        # Verify tables created
        self.assertIn("customers", sink.tables)
        self.assertIn("orders", sink.tables)
        self.assertEqual(len(sink.tables["customers"].rows), 3)
        self.assertEqual(len(sink.tables["orders"].rows), 2)

        # Verify evolution occurred
        self.assertEqual(sink.evolution_count, 1)
        self.assertEqual(
            sink.tables["customers"].schema.field_names, ["id", "name", "email"]
        )
        self.assertEqual(
            sink.tables["orders"].schema.field_names, ["order_id", "total"]
        )

    def test_base_python_change_handler_requires_implementation(self) -> None:
        """Verifies handler raises NotImplementedError when neither batch method is implemented."""

        class IncompleteHandler(BasePythonChangeHandler):
            pass

        handler = IncompleteHandler()
        with self.assertRaises(NotImplementedError):
            handler.handle_batch([])
        with self.assertRaises(NotImplementedError):
            handler.handleJsonBatch([])


if __name__ == "__main__":
    unittest.main()
