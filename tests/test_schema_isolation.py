from __future__ import annotations

import subprocess
import sys
import unittest

from pydbzengine.schema import BaseBatchChangeHandler


class TestSchemaIsolationAndABCs(unittest.TestCase):
    def test_schema_does_not_load_jvm_or_pyiceberg(self) -> None:
        """Verifies in an isolated Python process that importing pydbzengine.schema does not load jpype or pyiceberg."""
        code = """
import sys
import pydbzengine.schema
assert "jpype" not in sys.modules, "jpype was imported by pydbzengine.schema!"
assert "pyiceberg" not in sys.modules, "pyiceberg was imported by pydbzengine.schema!"
"""
        res = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        self.assertEqual(res.returncode, 0, msg=f"Process failed: {res.stderr}")

    def test_change_event_does_not_load_jvm_or_jpype(self) -> None:
        """Verifies in an isolated Python process that importing and using ChangeEvent does not load jpype or start JVM."""
        code = """
import sys
from pydbzengine.engine.base import ChangeEvent

assert "jpype" not in sys.modules, "jpype was loaded by importing ChangeEvent!"

class SampleRecord(ChangeEvent):
    def key(self): return "k"
    def value(self): return "v"
    def destination(self): return "dest"
    def partition(self): return 0

rec = SampleRecord()
assert isinstance(rec, ChangeEvent), "SampleRecord must satisfy ChangeEvent contract!"
assert "jpype" not in sys.modules, "jpype was loaded during isinstance check!"
"""
        res = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        self.assertEqual(res.returncode, 0, msg=f"Process failed: {res.stderr}")

    def test_abc_runtime_enforcement(self) -> None:
        """Verifies that BaseBatchChangeHandler cannot be instantiated without implementing handle_table_batch."""

        class IncompleteBatchHandler(BaseBatchChangeHandler):
            pass

        with self.assertRaises(TypeError):
            IncompleteBatchHandler()


if __name__ == "__main__":
    unittest.main()
