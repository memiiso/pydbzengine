import io
import time

import pandas as pd
import pyarrow as pa
import pyarrow.json as pj
from pyiceberg.catalog import load_catalog

from base_postgresql import BasePostgresqlTest
from catalog_rest import CatalogRestContainer
from mock_events import MockChangeEvent
from pydbzengine.handlers.iceberg import IcebergChangeHandlerV2
from s3_minio import S3Minio


class TestIcebergChangeHandlerV2(BasePostgresqlTest):
    def setUp(self):
        print("setUp")
        super().setUp()
        self.S3MiNIO = S3Minio()
        self.RESTCATALOG = CatalogRestContainer()
        self.S3MiNIO.start()
        self.RESTCATALOG.start(s3_endpoint=self.S3MiNIO.endpoint())
        # Set pandas options to display all rows and columns, and prevent truncation of cell content
        pd.set_option("display.max_rows", None)  # Show all rows
        pd.set_option("display.max_columns", None)  # Show all columns
        pd.set_option("display.width", None)  # Auto-detect terminal width
        pd.set_option("display.max_colwidth", None)  # Do not truncate cell contents

    def tearDown(self):
        super().tearDown()
        self.S3MiNIO.stop()
        self.RESTCATALOG.stop()

    def test_read_json_lines_example(self):
        json_data = """
{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 24}
{"id": 3, "name": "Charlie", "age": 35}
    """.strip()  # .strip() removes leading/trailing whitespace/newlines
        json_buffer = io.BytesIO(json_data.encode("utf-8"))
        json_buffer.seek(0)
        # =============================
        table_inferred = pj.read_json(json_buffer)
        print("\nInferred Schema:")
        print(table_inferred.schema)
        # =============================
        explicit_schema = pa.schema(
            [
                pa.field("id", pa.int64()),  # Integer type for 'id'
                pa.field("name", pa.string()),  # String type for 'name'
            ]
        )
        json_buffer.seek(0)
        po = pj.ParseOptions(explicit_schema=explicit_schema)
        table_explicit = pj.read_json(json_buffer, parse_options=po)
        print("\nExplicit Schema:")
        print(table_explicit.schema)

    def _apply_source_db_changes(self):
        time.sleep(12)
        self.execute_on_source_db(
            "UPDATE inventory.customers SET first_name='George__UPDATE1' WHERE ID = 1002 ;"
        )
        # self.execute_on_source_db("ALTER TABLE inventory.customers DROP COLUMN email;")
        self.execute_on_source_db(
            "UPDATE inventory.customers SET first_name='George__UPDATE2'  WHERE ID = 1002 ;"
        )
        self.execute_on_source_db(
            "DELETE FROM inventory.orders WHERE purchaser = 1002 ;"
        )
        self.execute_on_source_db("DELETE FROM inventory.customers WHERE id = 1002 ;")
        self.execute_on_source_db(
            "ALTER TABLE inventory.customers ADD birth_date date;"
        )
        self.execute_on_source_db(
            "UPDATE inventory.customers SET birth_date = '2020-01-01'  WHERE id = 1001 ;"
        )

    def test_iceberg_handler(self):
        dest_ns1_database = "my_warehouse"
        dest_ns2_schema = "dbz_cdc_data"
        catalog_conf = {
            "uri": self.RESTCATALOG.get_uri(),
            "warehouse": "warehouse",
            "s3.endpoint": self.S3MiNIO.endpoint(),
            "s3.access-key-id": S3Minio.AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": S3Minio.AWS_SECRET_ACCESS_KEY,
        }
        destination_namespace = (
            dest_ns1_database,
            dest_ns2_schema,
        )
        handler = IcebergChangeHandlerV2(
            catalog=load_catalog(name="rest", **catalog_conf),
            destination_namespace=destination_namespace,
            event_flattening_enabled=True,
        )

        # TEST
        testing_catalog = load_catalog(name="rest", **catalog_conf)
        testing_catalog.create_namespace(namespace=destination_namespace)

        test_dest = "inventory.customers"
        records = [
            MockChangeEvent(
                key='{"id": 1}',
                value='{"id": 1, "name": "Alice", "age": 30}',
                destination=test_dest,
            ),
            MockChangeEvent(
                key='{"id": 2}',
                value='{"id": 2, "name": "Bob", "age": 24}',
                destination=test_dest,
            ),
            MockChangeEvent(
                key='{"id": 3}',
                value='{"id": 3, "name": "Charlie", "age": 35}',
                destination=test_dest,
            ),
        ]
        handler.handleJsonBatch(records)

        test_ns = (dest_ns1_database,)
        print(testing_catalog.list_namespaces())
        self._wait_for_condition(
            predicate=lambda: test_ns in testing_catalog.list_namespaces(),
            failure_message=f"Namespace {test_ns} did not appear in the catalog",
        )

        test_tbl_ref = ("my_warehouse", "dbz_cdc_data", "inventory_customers")
        test_tbl_ns = (
            dest_ns1_database,
            dest_ns2_schema,
        )
        self._wait_for_condition(
            predicate=lambda: test_tbl_ref in testing_catalog.list_tables(test_tbl_ns),
            failure_message=f"Table {test_tbl_ref} did not appear in the tables",
        )
        test_tbl_ref = ("my_warehouse", "dbz_cdc_data", "inventory_customers")
        data = self.red_table(testing_catalog, test_tbl_ref)
        self.pprint_table(data=data)

        self._wait_for_condition(
            predicate=lambda: "Charlie"
            in str(self.red_table(testing_catalog, test_tbl_ref)),
            failure_message=f"Expected row not consumed!",
        )
        self._wait_for_condition(
            predicate=lambda: self.red_table(testing_catalog, test_tbl_ref).num_rows
            >= 3,
            failure_message=f"Rows not consumed",
        )
        # # =================================================================
        # ## ==== PART 2 Add additional field ========================
        # # =================================================================
        records = [
            MockChangeEvent(
                key='{"id": 1}',
                value='{"id": 1, "name": "Alice", "age": 30, "lastname":"Wonder"}',
                destination=test_dest,
            ),
            MockChangeEvent(
                key='{"id": 2}',
                value='{"id": 2, "name": "Bob", "age": 24, "lastname":"Sponge"}',
                destination=test_dest,
            ),
            MockChangeEvent(
                key='{"id": 3}',
                value='{"id": 3, "name": "Charlie", "age": 35, "lastname":"Chaplin"}',
                destination=test_dest,
            ),
        ]
        handler.handleJsonBatch(records)
        data = self.red_table(testing_catalog, test_tbl_ref)
        self.pprint_table(data=data)

    def red_table(self, catalog, table_identifier) -> "pa.Table":
        tbl = catalog.load_table(identifier=table_identifier)
        data = tbl.scan().to_arrow()
        self.pprint_table(data)
        return data

    def pprint_table(self, data):
        print("--- Iceberg Table Content ---")
        print(data.to_pandas())
        print("---------------------------\n")

    def _wait_for_condition(
        self, predicate, failure_message: str, retries: int = 10, delay_seconds: int = 2
    ):
        attempts = 0
        while attempts < retries:
            print(f"Attempt {attempts + 1}/{retries}: Checking condition...")
            if predicate():
                print("Condition met.")
                return

            attempts += 1
            # Avoid sleeping after the last attempt
            if attempts < retries:
                time.sleep(delay_seconds)

        raise TimeoutError(
            f"{failure_message} after {retries} attempts ({retries * delay_seconds} seconds)."
        )

    def _get_handler_and_catalog(self, namespace_suffix=""):
        """Helper to create handler and catalog with unique namespace for test isolation."""
        dest_ns1_database = "my_warehouse"
        dest_ns2_schema = (
            f"dbz_test_{namespace_suffix}" if namespace_suffix else "dbz_test"
        )
        catalog_conf = {
            "uri": self.RESTCATALOG.get_uri(),
            "warehouse": "warehouse",
            "s3.endpoint": self.S3MiNIO.endpoint(),
            "s3.access-key-id": S3Minio.AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": S3Minio.AWS_SECRET_ACCESS_KEY,
        }
        destination_namespace = (
            dest_ns1_database,
            dest_ns2_schema,
        )
        catalog = load_catalog(name="rest", **catalog_conf)
        catalog.create_namespace(namespace=destination_namespace)
        handler = IcebergChangeHandlerV2(
            catalog=catalog,
            destination_namespace=destination_namespace,
            event_flattening_enabled=True,
        )
        return handler, catalog, destination_namespace

    def test_schema_inference(self):
        """Verifies handler infers schema from JSON data correctly."""
        handler, catalog, namespace = self._get_handler_and_catalog("schema_infer")
        test_dest = "test.schema_inference"

        records = [
            MockChangeEvent(
                key='{"id": 1}',
                value='{"id": 1, "name": "Alice", "age": 30, "active": true}',
                destination=test_dest,
            ),
        ]
        handler.handleJsonBatch(records)

        table_id = namespace + ("test_schema_inference",)
        table = catalog.load_table(table_id)
        schema = table.schema()

        # Verify inferred fields exist
        field_names = [f.name for f in schema.fields]
        self.assertIn("id", field_names)
        self.assertIn("name", field_names)
        self.assertIn("age", field_names)
        self.assertIn("active", field_names)
        # Verify metadata fields exist
        self.assertIn("_consumed_at", field_names)
        self.assertIn("_dbz_event_key", field_names)
        self.assertIn("_dbz_event_key_hash", field_names)

    def test_schema_evolution_adds_column(self):
        """Verifies new columns in incoming data are automatically added to table."""
        handler, catalog, namespace = self._get_handler_and_catalog("schema_evol")
        test_dest = "test.schema_evolution"

        # First batch - initial schema
        records = [
            MockChangeEvent(
                key='{"id": 1}',
                value='{"id": 1, "name": "Alice"}',
                destination=test_dest,
            ),
        ]
        handler.handleJsonBatch(records)

        table_id = namespace + ("test_schema_evolution",)
        initial_fields = [f.name for f in catalog.load_table(table_id).schema().fields]
        self.assertIn("name", initial_fields)
        self.assertNotIn("email", initial_fields)

        # Second batch - adds new column
        records = [
            MockChangeEvent(
                key='{"id": 2}',
                value='{"id": 2, "name": "Bob", "email": "bob@test.com"}',
                destination=test_dest,
            ),
        ]
        handler.handleJsonBatch(records)

        evolved_fields = [f.name for f in catalog.load_table(table_id).schema().fields]
        self.assertIn("email", evolved_fields)

    def test_null_type_sanitization(self):
        """Verifies null JSON fields are converted to string type fallback."""
        handler, catalog, namespace = self._get_handler_and_catalog("null_sanitize")
        test_dest = "test.null_handling"

        records = [
            MockChangeEvent(
                key='{"id": 1}',
                value='{"id": 1, "nullable_field": null}',
                destination=test_dest,
            ),
        ]
        handler.handleJsonBatch(records)

        table_id = namespace + ("test_null_handling",)
        table = catalog.load_table(table_id)
        schema = table.schema()

        nullable_field = schema.find_field("nullable_field")
        # Null type should be sanitized to string
        self.assertEqual(str(nullable_field.field_type), "string")

    def test_metadata_enrichment(self):
        """Verifies metadata columns are populated with correct values."""
        handler, catalog, namespace = self._get_handler_and_catalog("metadata")
        test_dest = "test.metadata_check"

        records = [
            MockChangeEvent(
                key='{"pk": 42}',
                value='{"id": 1, "data": "test"}',
                destination=test_dest,
            ),
        ]
        handler.handleJsonBatch(records)

        table_id = namespace + ("test_metadata_check",)
        data = catalog.load_table(table_id).scan().to_arrow()

        # Verify metadata columns are populated
        self.assertEqual(data.num_rows, 1)
        self.assertIsNotNone(data.column("_consumed_at")[0].as_py())
        self.assertEqual(data.column("_dbz_event_key")[0].as_py(), '{"pk": 42}')
        self.assertIsNotNone(data.column("_dbz_event_key_hash")[0].as_py())

    def test_nested_struct_handling(self):
        """Verifies nested JSON objects are handled correctly."""
        handler, catalog, namespace = self._get_handler_and_catalog("nested")
        test_dest = "test.nested_struct"

        records = [
            MockChangeEvent(
                key='{"id": 1}',
                value='{"id": 1, "address": {"city": "NYC", "zip": "10001"}}',
                destination=test_dest,
            ),
        ]
        handler.handleJsonBatch(records)

        table_id = namespace + ("test_nested_struct",)
        table = catalog.load_table(table_id)
        schema = table.schema()

        # Verify address is a struct
        address_field = schema.find_field("address")
        self.assertIn("struct", str(address_field.field_type).lower())

        # Verify data is readable
        data = table.scan().to_arrow()
        self.assertEqual(data.num_rows, 1)

    def test_empty_event_key(self):
        """Verifies handler works when event key is empty."""
        handler, catalog, namespace = self._get_handler_and_catalog("empty_key")
        test_dest = "test.empty_key"

        records = [
            MockChangeEvent(
                key="", value='{"id": 1, "data": "no_key"}', destination=test_dest
            ),
        ]
        handler.handleJsonBatch(records)

        table_id = namespace + ("test_empty_key",)
        data = catalog.load_table(table_id).scan().to_arrow()

        self.assertEqual(data.num_rows, 1)
        self.assertEqual(data.column("_dbz_event_key")[0].as_py(), "")

    def test_multiple_tables_batch(self):
        """Verifies batch with events for different destinations works."""
        handler, catalog, namespace = self._get_handler_and_catalog("multi_tbl")

        records = [
            MockChangeEvent(
                key='{"id": 1}',
                value='{"id": 1, "name": "Alice"}',
                destination="test.table_a",
            ),
            MockChangeEvent(
                key='{"id": 2}',
                value='{"id": 2, "name": "Bob"}',
                destination="test.table_b",
            ),
            MockChangeEvent(
                key='{"id": 3}',
                value='{"id": 3, "name": "Charlie"}',
                destination="test.table_a",
            ),
        ]
        handler.handleJsonBatch(records)

        table_a_id = namespace + ("test_table_a",)
        table_b_id = namespace + ("test_table_b",)

        data_a = catalog.load_table(table_a_id).scan().to_arrow()
        data_b = catalog.load_table(table_b_id).scan().to_arrow()

        self.assertEqual(data_a.num_rows, 2)
        self.assertEqual(data_b.num_rows, 1)

    def test_nested_schema_evolution(self):
        """Verifies that adding a field within a nested struct evolves the schema correctly."""
        handler, catalog, namespace = self._get_handler_and_catalog("nested_evol")
        test_dest = "test.nested_evolution"

        # First batch - simple nested struct
        records = [
            MockChangeEvent(
                key='{"id": 1}',
                value='{"id": 1, "owner": {"name": "Alice"}}',
                destination=test_dest,
            ),
        ]
        handler.handleJsonBatch(records)

        table_id = namespace + ("test_nested_evolution",)
        initial_schema = catalog.load_table(table_id).schema()
        owner_field = initial_schema.find_field("owner")
        owner_struct = owner_field.field_type
        # Verify initial fields in nested struct
        nested_field_names = [f.name for f in owner_struct.fields]
        self.assertIn("name", nested_field_names)
        self.assertNotIn("email", nested_field_names)
        print(initial_schema)

        # Second batch - add field inside the nested struct
        records = [
            MockChangeEvent(
                key='{"id": 2}',
                value='{"id": 2, "owner": {"name": "Bob", "email": "bob@test.com"}}',
                destination=test_dest,
            ),
        ]
        handler.handleJsonBatch(records)

        evolved_schema = catalog.load_table(table_id).schema()
        evolved_owner_field = evolved_schema.find_field("owner")
        evolved_owner_struct = evolved_owner_field.field_type
        # Verify evolved fields in nested struct
        evolved_nested_field_names = [f.name for f in evolved_owner_struct.fields]
        self.assertIn("name", evolved_nested_field_names)
        self.assertIn("email", evolved_nested_field_names)
        print(evolved_schema)

        # Verify data is readable
        data = catalog.load_table(table_id).scan().to_arrow()
        self.assertEqual(data.num_rows, 2)
        # Check that we can access the nested evolved field
        p_table = data.to_pandas()
        self.assertTrue(
            any(
                p_table["owner"].apply(
                    lambda x: x.get("email") == "bob@test.com" if x else False
                )
            )
        )
        print(p_table)
