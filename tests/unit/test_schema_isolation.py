from __future__ import annotations

import ast
import subprocess
import sys
import unittest
from pathlib import Path

from pydbzengine.engine.base import BasePythonChangeHandler


def _top_level_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    return imported


class TestSchemaIsolationAndABCs(unittest.TestCase):
    def test_schema_does_not_load_jvm_or_pyiceberg(self) -> None:
        """Verifies importing pydbzengine.schema never loads jpype or pyiceberg."""
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

    def test_schema_module_boundaries_are_strictly_separated(self) -> None:
        """Verifies strict format and dependency isolation across pydbzengine.schema modules."""
        schema_dir = Path(__file__).resolve().parents[2] / "pydbzengine" / "schema"

        # Shared pure-Python Arrow types module must have zero JSON, Base64, or JVM imports
        types_imports = _top_level_imports(schema_dir / "types.py")
        self.assertNotIn("json", types_imports)
        self.assertNotIn("base64", types_imports)
        self.assertNotIn("jpype", types_imports)

        # Connect module must have zero JSON or Base64 imports
        connect_imports = _top_level_imports(schema_dir / "connect.py")
        self.assertNotIn("json", connect_imports)
        self.assertNotIn("base64", connect_imports)
        self.assertNotIn("jpype", connect_imports)

        # JSON module must have zero JVM imports
        json_imports = _top_level_imports(schema_dir / "json.py")
        self.assertNotIn("jpype", json_imports)

        # Pure domain models module must have zero JSON, Base64, or JVM imports
        models_imports = _top_level_imports(schema_dir / "models.py")
        self.assertNotIn("json", models_imports)
        self.assertNotIn("base64", models_imports)
        self.assertNotIn("jpype", models_imports)

    def test_engine_does_not_import_schema_or_pyarrow(self) -> None:
        """Verifies importing pydbzengine.engine does not load pydbzengine.schema or pyarrow."""
        code = """
import sys
import pydbzengine.engine
assert "pydbzengine.schema" not in sys.modules, "pydbzengine.schema was imported by pydbzengine.engine!"
assert "pyarrow" not in sys.modules, "pyarrow was imported by pydbzengine.engine!"
"""
        res = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        self.assertEqual(res.returncode, 0, msg=f"Process failed: {res.stderr}")

    def test_change_event_does_not_load_jvm_or_jpype(self) -> None:
        """Verifies importing and using ChangeEvent does not load jpype or start JVM."""
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
        """Verifies BasePythonChangeHandler raises NotImplementedError if neither method is overridden."""

        class IncompleteHandler(BasePythonChangeHandler):
            pass

        handler = IncompleteHandler()
        with self.assertRaises(NotImplementedError):
            handler.handleJsonBatch([])


if __name__ == "__main__":
    unittest.main()
