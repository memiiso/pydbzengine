from __future__ import annotations

import subprocess
import sys
import unittest

from pydbzengine.engine.base import BasePythonChangeHandler


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

    def test_base_handler_runtime_enforcement(self) -> None:
        """Verifies that BasePythonChangeHandler raises NotImplementedError if neither handle_batch nor handleJsonBatch is overridden."""

        class IncompleteHandler(BasePythonChangeHandler):
            pass

        handler = IncompleteHandler()
        with self.assertRaises(NotImplementedError):
            handler.handleJsonBatch([])


if __name__ == "__main__":
    unittest.main()
