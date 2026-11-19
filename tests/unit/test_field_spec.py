from __future__ import annotations

import base64
import unittest
from decimal import Decimal
from typing import Any

import pyarrow as pa

from pydbzengine.schema.handlers import (
    ArrayTypeHandler,
    DecimalTypeHandler,
    IntTypeHandler,
    MapTypeHandler,
    StructTypeHandler,
    VariableScaleDecimalHandler,
)
from pydbzengine.schema.registry import TypeRegistry
from pydbzengine.schema.spec import FieldSpec
from pydbzengine.schema.temporal import (
    DateTypeHandler,
    TimestampTypeHandler,
    TimeTypeHandler,
)


class TestFieldSpecAndTypeRegistry(unittest.TestCase):
    def test_field_spec_immutability_hashing_and_validation(self) -> None:
        fs1 = FieldSpec(
            "  x  ", "  BYTES ", parameters={"scale": "2", "precision": "10"}
        )
        fs2 = FieldSpec("x", "bytes", parameters={"precision": "10", "scale": "2"})
        self.assertEqual(fs1.name, "x")
        self.assertEqual(fs1.type_name, "bytes")
        self.assertEqual(fs1, fs2)
        self.assertEqual(hash(fs1), hash(fs2))
        with self.assertRaises(TypeError):
            fs1.parameters["scale"] = "4"  # type: ignore[index]

        for bad_name, bad_type in (
            ("", "int32"),
            ("   ", "int32"),
            ("x", ""),
            ("x", "  "),
        ):
            with self.assertRaises(ValueError):
                FieldSpec(name=bad_name, type_name=bad_type)

    def test_field_spec_from_json_rejects_malformed_definitions(self) -> None:
        for malformed in (
            {"type": "struct", "field": "st", "fields": "not_a_list"},
            {"type": "string", "field": "s", "parameters": "not_a_dict"},
            {"type": "string", "field": "s", "optional": "false"},
            {"type": "array", "field": "arr", "items": 123},
            {"type": "map", "field": "mp", "keys": 123, "values": {"type": "int32"}},
            {"type": "map", "field": "mp", "keys": {"type": "string"}, "values": 123},
            {"field": "x", "type": "string", "name": 123},
            {"field": "x", "type": "string", "doc": 456},
        ):
            with self.assertRaises(ValueError):
                FieldSpec.from_json(malformed)

    def test_field_spec_from_connect_with_real_debezium_schemas(self) -> None:
        from pydbzengine.engine.jvm import ensure_jvm_started

        ensure_jvm_started()
        import jpype

        Point = jpype.JClass("io.debezium.data.geometry.Point")
        Geometry = jpype.JClass("io.debezium.data.geometry.Geometry")
        FloatVector = jpype.JClass("io.debezium.data.vector.FloatVector")
        SparseDoubleVector = jpype.JClass("io.debezium.data.vector.SparseDoubleVector")

        reg = TypeRegistry()

        point_field = reg.compile_field(
            FieldSpec.from_connect(Point.builder().build(), field_name="loc")
        )
        self.assertIsInstance(point_field.handler, StructTypeHandler)
        self.assertEqual(
            point_field.arrow_field.metadata[b"debezium.logical_type"],
            b"io.debezium.data.geometry.Point",
        )
        self.assertEqual(point_field.arrow_type.field("x").type, pa.float64())
        self.assertEqual(point_field.arrow_type.field("y").type, pa.float64())
        self.assertEqual(point_field.arrow_type.field("wkb").type, pa.binary())
        self.assertEqual(point_field.arrow_type.field("srid").type, pa.int32())

        geom_field = reg.compile_field(
            FieldSpec.from_connect(Geometry.schema(), field_name="geom")
        )
        self.assertIsInstance(geom_field.handler, StructTypeHandler)
        self.assertFalse(geom_field.arrow_type.field("wkb").nullable)
        self.assertTrue(geom_field.arrow_type.field("srid").nullable)

        fvec_field = reg.compile_field(
            FieldSpec.from_connect(FloatVector.schema(), field_name="emb")
        )
        self.assertIsInstance(fvec_field.handler, ArrayTypeHandler)
        self.assertEqual(fvec_field.arrow_type.value_type, pa.float32())

        svec_field = reg.compile_field(
            FieldSpec.from_connect(SparseDoubleVector.schema(), field_name="sp")
        )
        self.assertIsInstance(svec_field.handler, StructTypeHandler)
        self.assertEqual(svec_field.arrow_type.field("dimensions").type, pa.int16())
        self.assertTrue(pa.types.is_map(svec_field.arrow_type.field("vector").type))

    def test_registry_compiles_semantic_logical_types_and_validates_wire_types(
        self,
    ) -> None:
        reg = TypeRegistry()
        for logical_name, (
            wire_type,
            expected_handler,
        ) in TypeRegistry.SEMANTIC_LOGICAL_HANDLERS.items():
            compiled = reg.compile_handler(
                FieldSpec("col", wire_type, logical_name=logical_name)
            )
            self.assertIs(compiled, expected_handler)
            self.assertIsInstance(
                compiled, (DateTypeHandler, TimeTypeHandler, TimestampTypeHandler)
            )

            wrong_wire = "boolean"
            with self.assertRaisesRegex(ValueError, "Expected wire type"):
                reg.compile_handler(
                    FieldSpec("col", wrong_wire, logical_name=logical_name)
                )

        # Decimal & VariableScaleDecimal wire validation
        with self.assertRaisesRegex(ValueError, "Expected wire type 'bytes'"):
            reg.compile_handler(
                FieldSpec(
                    "d",
                    "struct",
                    logical_name="org.apache.kafka.connect.data.Decimal",
                    parameters={"scale": "2"},
                )
            )
        with self.assertRaisesRegex(ValueError, "Expected wire type 'struct'"):
            reg.compile_handler(
                FieldSpec(
                    "vd",
                    "bytes",
                    logical_name="io.debezium.data.VariableScaleDecimal",
                )
            )
        with self.assertRaisesRegex(ValueError, "Missing required 'scale'"):
            reg.compile_handler(
                FieldSpec(
                    "d",
                    "bytes",
                    logical_name="org.apache.kafka.connect.data.Decimal",
                    parameters={"connect.decimal.precision": "10"},
                )
            )

    def test_registry_preserves_metadata_on_pass_through_logical_types(self) -> None:
        reg = TypeRegistry()
        for wire_type, logical_name, expected_pa in (
            ("string", "io.debezium.data.Uuid", pa.string()),
            ("string", "io.debezium.data.Json", pa.string()),
            ("string", "io.debezium.data.TsVector", pa.string()),
            ("string", "io.debezium.connector.postgresql.data.Ltree", pa.string()),
            ("int32", "io.debezium.time.Year", pa.int32()),
            ("int64", "io.debezium.time.MicroDuration", pa.int64()),
            ("bytes", "io.debezium.data.Bits", pa.binary()),
        ):
            sf = reg.compile_field(
                FieldSpec("col", wire_type, logical_name=logical_name)
            )
            self.assertEqual(sf.arrow_type, expected_pa)
            self.assertEqual(
                sf.arrow_field.metadata[b"debezium.logical_type"],
                logical_name.encode("utf-8"),
            )

    def test_registry_variable_scale_clamping_and_complex_errors(self) -> None:
        reg = TypeRegistry(default_variable_scale=10)
        small_vardec = reg.compile_handler(
            FieldSpec(
                name="rate",
                type_name="struct",
                logical_name="io.debezium.data.VariableScaleDecimal",
                parameters={"connect.decimal.precision": "5"},
            )
        )
        self.assertIsInstance(small_vardec, VariableScaleDecimalHandler)
        self.assertEqual(small_vardec.arrow_type, pa.decimal128(5, 5))
        b64_12345 = base64.b64encode((12345).to_bytes(2, "big", signed=True)).decode(
            "ascii"
        )
        self.assertEqual(
            small_vardec.convert_json({"scale": 5, "value": b64_12345}),
            Decimal("0.12345"),
        )

        # Fixed-scale Decimal precision out of bounds
        with self.assertRaises(ValueError):
            reg.compile_handler(
                FieldSpec(
                    name="amount",
                    type_name="bytes",
                    logical_name="org.apache.kafka.connect.data.Decimal",
                    parameters={"scale": "2", "connect.decimal.precision": "50"},
                )
            )

        # Complex specs missing child definitions or unsupported type
        for bad_spec in (
            FieldSpec(name="arr", type_name="array"),
            FieldSpec(name="mp", type_name="map"),
            FieldSpec(name="st", type_name="struct"),
            FieldSpec(name="u", type_name="unknown_type"),
        ):
            with self.assertRaises(ValueError):
                reg.compile_handler(bad_spec)

        # Nested map key fails fast
        bad_map = FieldSpec.from_json(
            {
                "type": "map",
                "field": "m",
                "keys": {"type": "array", "items": {"type": "string"}},
                "values": {"type": "int32"},
            }
        )
        with self.assertRaises(ValueError):
            reg.compile_handler(bad_map)

        # Map compilation preserves handler & metadata
        hstore_spec = FieldSpec.from_json(
            {
                "type": "map",
                "field": "tags",
                "keys": {"type": "string", "optional": False},
                "values": {"type": "string", "optional": True},
            }
        )
        hstore_handler = reg.compile_handler(hstore_spec)
        self.assertIsInstance(hstore_handler, MapTypeHandler)
        self.assertIsInstance(
            reg.compile_handler(FieldSpec("n", "int32")), IntTypeHandler
        )
        self.assertIsInstance(
            reg.compile_handler(
                FieldSpec(
                    "d",
                    "bytes",
                    logical_name="org.apache.kafka.connect.data.Decimal",
                    parameters={"scale": "2", "connect.decimal.precision": "10"},
                )
            ),
            DecimalTypeHandler,
        )

    def test_field_spec_from_connect_rejects_none_and_empty_names(self) -> None:
        with self.assertRaises(ValueError):
            FieldSpec.from_connect(None)

        class _NoneTypeSchema:
            def type(self) -> Any:
                return None

        with self.assertRaises(ValueError):
            FieldSpec.from_connect(_NoneTypeSchema())


if __name__ == "__main__":
    unittest.main()
