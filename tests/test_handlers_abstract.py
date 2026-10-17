from __future__ import annotations

import unittest
from collections.abc import Sequence

from mock_events import MockChangeEvent

from pydbzengine.engine import BasePythonChangeHandler, ChangeEvent


def make_test_event(dest: str, payload: str = "test") -> MockChangeEvent:
    return MockChangeEvent(
        key='{"id": 1}',
        value=payload,
        destination=dest,
    )


class ConcreteMockSink(BasePythonChangeHandler):
    """A realistic concrete sink implementing BasePythonChangeHandler directly."""

    def __init__(self) -> None:
        self.tables: dict[str, list[ChangeEvent]] = {}
        self.total_processed = 0

    def handle_batch(self, records: list[ChangeEvent]) -> None:
        for dest, dest_records in self.group_by_destination(records).items():
            self.tables.setdefault(dest, []).extend(dest_records)
            self.total_processed += len(dest_records)


class TestHandlersAbstractDesign(unittest.TestCase):
    def test_base_python_change_handler_destination_grouping(self) -> None:
        """Verifies BasePythonChangeHandler.group_by_destination cleanly separates interleaved tables."""
        records: list[ChangeEvent] = [
            make_test_event("table_a", "val_a1"),
            make_test_event("table_b", "val_b1"),
            make_test_event("table_a", "val_a2"),
            make_test_event("table_b", "val_b2"),
        ]
        sink = ConcreteMockSink()
        grouped = sink.group_by_destination(records)
        self.assertEqual(set(grouped.keys()), {"table_a", "table_b"})
        self.assertEqual(len(grouped["table_a"]), 2)
        self.assertEqual(len(grouped["table_b"]), 2)

    def test_base_python_change_handler_empty_destination_raises(self) -> None:
        """Verifies group_by_destination fails fast on records with missing/empty destination."""
        empty_dest_record = make_test_event("", "val")
        sink = ConcreteMockSink()
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

        sink = CustomRoutingSink()
        records = [make_test_event("orders", "val")]
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
        rec = make_test_event("test", "val")
        handler.handleJsonBatch([rec])
        self.assertEqual(called, [1])

    def test_base_python_change_handler_handle_json_batch_override(self) -> None:
        """Verifies handler implementing handleJsonBatch directly works and handle_batch delegates."""
        called: list[int] = []

        class JavaBridgeSink(BasePythonChangeHandler):
            def handleJsonBatch(self, records: list[ChangeEvent]) -> None:
                called.append(len(records))

        handler = JavaBridgeSink()
        rec = make_test_event("test", "val")
        handler.handle_batch([rec])
        self.assertEqual(called, [1])

    def test_base_python_change_handler_requires_implementation(self) -> None:
        """Verifies handler raises NotImplementedError when neither batch method is implemented."""

        class IncompleteHandler(BasePythonChangeHandler):
            pass

        handler = IncompleteHandler()
        with self.assertRaises(NotImplementedError):
            handler.handle_batch([])
        with self.assertRaises(NotImplementedError):
            handler.handleJsonBatch([])

    def test_change_event_protocol_structural_conformance(self) -> None:
        """Verifies that any object exposing key, value, destination, partition satisfies ChangeEvent."""

        class CustomRecord:
            def key(self) -> str:
                return "k"

            def value(self) -> str:
                return "v"

            def destination(self) -> str:
                return "d"

            def partition(self) -> int:
                return 0

        rec = CustomRecord()
        self.assertTrue(isinstance(rec, ChangeEvent))


if __name__ == "__main__":
    unittest.main()
