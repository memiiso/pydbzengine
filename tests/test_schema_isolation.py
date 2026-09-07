from __future__ import annotations

import subprocess
import sys
import unittest

from pydbzengine.schema.base import BaseSchemaReader


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

    def test_abc_runtime_enforcement(self) -> None:
        """Verifies that BaseSchemaReader cannot be instantiated without implementing abstract methods."""

        class IncompleteReader(BaseSchemaReader):
            pass

        with self.assertRaises(TypeError):
            IncompleteReader()


if __name__ == "__main__":
    unittest.main()
