import unittest

from pydbzengine.schema.models import (
    CanonicalField,
    CanonicalPrimitiveType,
    CanonicalSchema,
    CanonicalType,
)


class TestSchemaModels(unittest.TestCase):
    def test_canonical_schema_fingerprint_determinism(self):
        f1 = CanonicalField(
            name="id",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.INT32),
            optional=False,
        )
        f2 = CanonicalField(
            name="name",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
            optional=True,
        )
        schema1 = CanonicalSchema(
            identifier="test.table",
            fields=(f1, f2),
            primary_keys=("id",),
        )

        # Create identical schema with fresh instances
        f1_dup = CanonicalField(
            name="id",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.INT32),
            optional=False,
        )
        f2_dup = CanonicalField(
            name="name",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
            optional=True,
        )
        schema2 = CanonicalSchema(
            identifier="test.table",
            fields=(f1_dup, f2_dup),
            primary_keys=("id",),
        )

        self.assertTrue(schema1.same_schema(schema2))
        self.assertEqual(schema1.fingerprint, schema2.fingerprint)
        self.assertGreater(len(schema1.fingerprint), 20)

    def test_canonical_schema_fingerprint_changes_on_field_diff(self):
        f1 = CanonicalField(
            name="id",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.INT32),
            optional=False,
        )
        schema1 = CanonicalSchema(identifier="test.table", fields=(f1,))

        f2 = CanonicalField(
            name="email",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
            optional=True,
        )
        schema2 = CanonicalSchema(identifier="test.table", fields=(f1, f2))

        self.assertFalse(schema1.same_schema(schema2))
        self.assertNotEqual(schema1.fingerprint, schema2.fingerprint)

    def test_complex_types_representation(self):
        # Decimal type
        dec_type = CanonicalType(
            primitive=CanonicalPrimitiveType.DECIMAL, precision=10, scale=2
        )
        self.assertEqual(dec_type.precision, 10)
        self.assertEqual(dec_type.scale, 2)

        # Struct type
        subfield = CanonicalField(
            name="street",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.STRING),
        )
        struct_type = CanonicalType(
            primitive=CanonicalPrimitiveType.STRUCT,
            fields=(subfield,),
        )
        self.assertEqual(len(struct_type.fields), 1)

        # List type
        list_type = CanonicalType(
            primitive=CanonicalPrimitiveType.LIST,
            element_type=CanonicalType(primitive=CanonicalPrimitiveType.INT64),
        )
        self.assertEqual(list_type.element_type.primitive, CanonicalPrimitiveType.INT64)

    def test_find_field(self):
        f = CanonicalField(
            name="created_at",
            field_type=CanonicalType(primitive=CanonicalPrimitiveType.TIMESTAMPTZ),
        )
        schema = CanonicalSchema(identifier="test", fields=(f,))
        self.assertIsNotNone(schema.find_field("created_at"))
        self.assertIsNone(schema.find_field("unknown"))
        self.assertEqual(schema.field_names, ["created_at"])


if __name__ == "__main__":
    unittest.main()
