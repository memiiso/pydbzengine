from __future__ import annotations

import subprocess
import sys
import unittest

from pydbzengine.schema.base import (
    BaseSchemaConverter,
    BaseSchemaEvolver,
    BaseSchemaReader,
    BaseStreamSynchronizer,
    BaseTableWriter,
)


class TestSchemaIsolationAndABCs(unittest.TestCase):
    def test_core_schema_has_zero_third_party_dependencies(self) -> None:
        """Verifies in an isolated Python process that importing pydbzengine.schema does not load pyiceberg or pyarrow."""
        code = """
import sys
import pydbzengine.schema
assert "pyiceberg" not in sys.modules, "pyiceberg was imported by pydbzengine.schema!"
assert "pyarrow" not in sys.modules, "pyarrow was imported by pydbzengine.schema!"
"""
        res = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        self.assertEqual(res.returncode, 0, msg=f"Process failed: {res.stderr}")

    def test_arrow_submodule_does_not_import_pyiceberg(self) -> None:
        """Verifies that importing sinks.arrow does not load pyiceberg."""
        code = """
import sys
try:
    import pydbzengine.sinks.arrow
    if getattr(pydbzengine.sinks.arrow, "__file__", None) is None:
        sys.exit(77)
except (ImportError, ModuleNotFoundError):
    sys.exit(77)
assert "pyarrow" in sys.modules, "pyarrow should be loaded!"
assert "pyiceberg" not in sys.modules, "pyiceberg should NOT be imported by sinks.arrow!"
"""
        res = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        if res.returncode == 77:
            self.skipTest("pydbzengine.sinks.arrow is not yet available in this branch")
        self.assertEqual(res.returncode, 0, msg=f"Process failed: {res.stderr}")

    def test_abc_runtime_enforcement(self) -> None:
        """Verifies that ABCs cannot be instantiated without implementing all abstract methods."""

        class IncompleteReader(BaseSchemaReader):
            pass

        with self.assertRaises(TypeError):
            IncompleteReader()

        class IncompleteConverter(BaseSchemaConverter):
            pass

        with self.assertRaises(TypeError):
            IncompleteConverter()

        class IncompleteEvolver(BaseSchemaEvolver):
            pass

        with self.assertRaises(TypeError):
            IncompleteEvolver()

        class IncompleteWriter(BaseTableWriter):
            pass

        with self.assertRaises(TypeError):
            IncompleteWriter()

        class IncompleteSync(BaseStreamSynchronizer):
            pass

        with self.assertRaises(TypeError):
            IncompleteSync()


if __name__ == "__main__":
    unittest.main()
