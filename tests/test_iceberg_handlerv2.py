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
    S3MiNIO = S3Minio()
    RESTCATALOG = CatalogRestContainer()

    def setUp(self):
        print("setUp")
        self.clean_offset_file()
        self.SOURCEPGDB.start()
        self.S3MiNIO.start()
        self.RESTCATALOG.start(s3_endpoint=self.S3MiNIO.endpoint())
        # Set pandas options to display all rows and columns, and prevent truncation of cell content
        pd.set_option('display.max_rows', None)  # Show all rows
        pd.set_option('display.max_columns', None)  # Show all columns
        pd.set_option('display.width', None)  # Auto-detect terminal width
        pd.set_option('display.max_colwidth', None)  # Do not truncate cell contents

    def tearDown(self):
        self.SOURCEPGDB.stop()
        self.S3MiNIO.stop()
        self.RESTCATALOG.stop()
        self.clean_offset_file()

    def test_read_json_lines_example(self):
        json_data = """
{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 24}
{"id": 3, "name": "Charlie", "age": 35}
    """.strip()  # .strip() removes leading/trailing whitespace/newlines
        json_buffer = io.BytesIO(json_data.encode('utf-8'))
        json_buffer.seek(0)
        # =============================
        table_inferred = pj.read_json(json_buffer)
        print("\nInferred Schema:")
        print(table_inferred.schema)
        # =============================
        explicit_schema = pa.schema([
            pa.field('id', pa.int64()),  # Integer type for 'id'
            pa.field('name', pa.string()),  # String type for 'name'
        ])
        json_buffer.seek(0)
        po = pj.ParseOptions(explicit_schema=explicit_schema)
        table_explicit = pj.read_json(json_buffer, parse_options=po)
        print("\nExplicit Schema:")
        print(table_explicit.schema)

    def _apply_source_db_changes(self):
        time.sleep(12)
        self.execute_on_source_db("UPDATE inventory.customers SET first_name='George__UPDATE1' WHERE ID = 1002 ;")
        # self.execute_on_source_db("ALTER TABLE inventory.customers DROP COLUMN email;")
        self.execute_on_source_db("UPDATE inventory.customers SET first_name='George__UPDATE2'  WHERE ID = 1002 ;")
        self.execute_on_source_db("DELETE FROM inventory.orders WHERE purchaser = 1002 ;")
        self.execute_on_source_db("DELETE FROM inventory.customers WHERE id = 1002 ;")
        self.execute_on_source_db("ALTER TABLE inventory.customers ADD birth_date date;")
        self.execute_on_source_db("UPDATE inventory.customers SET birth_date = '2020-01-01'  WHERE id = 1001 ;")

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
        handler = IcebergChangeHandlerV2(catalog=load_catalog(name="rest", **catalog_conf),
                                         destination_namespace=(dest_ns1_database, dest_ns2_schema,),
                                         event_flattening_enabled=True
                                         )

        test_dest = "inventory.customers"
        records = [
            MockChangeEvent(key='{"id": 1}', value='{"id": 1, "name": "Alice", "age": 30}', destination=test_dest),
            MockChangeEvent(key='{"id": 2}', value='{"id": 2, "name": "Bob", "age": 24}', destination=test_dest),
            MockChangeEvent(key='{"id": 3}', value='{"id": 3, "name": "Charlie", "age": 35}', destination=test_dest)
        ]
        handler.handleJsonBatch(records)

        # TEST
        testing_catalog = load_catalog(name="rest", **catalog_conf)
        test_ns = (dest_ns1_database,)
        print(testing_catalog.list_namespaces())
        self._wait_for_condition(
            predicate=lambda: test_ns in testing_catalog.list_namespaces(),
            failure_message=f"Namespace {test_ns} did not appear in the catalog"
        )

        test_tbl_ref = ('my_warehouse', 'dbz_cdc_data', 'inventory_customers')
        test_tbl_ns = (dest_ns1_database, dest_ns2_schema,)
        self._wait_for_condition(
            predicate=lambda: test_tbl_ref in testing_catalog.list_tables(test_tbl_ns),
            failure_message=f"Table {test_tbl_ref} did not appear in the tables"
        )
        test_tbl_ref = ('my_warehouse', 'dbz_cdc_data', 'inventory_customers')
        data = self.red_table(testing_catalog, test_tbl_ref)
        self.pprint_table(data=data)

        self._wait_for_condition(
            predicate=lambda: "Charlie" in str(self.red_table(testing_catalog, test_tbl_ref)),
            failure_message=f"Expected row not consumed!"
        )
        self._wait_for_condition(
            predicate=lambda: self.red_table(testing_catalog, test_tbl_ref).num_rows >= 3,
            failure_message=f"Rows not consumed"
        )
        # # =================================================================
        # ## ==== PART 2 Add additional field ========================
        # # =================================================================
        records = [
            MockChangeEvent(key='{"id": 1}', value='{"id": 1, "name": "Alice", "age": 30, "lastname":"Wonder"}', destination=test_dest),
            MockChangeEvent(key='{"id": 2}', value='{"id": 2, "name": "Bob", "age": 24, "lastname":"Sponge"}', destination=test_dest),
            MockChangeEvent(key='{"id": 3}', value='{"id": 3, "name": "Charlie", "age": 35, "lastname":"Chaplin"}', destination=test_dest)
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

    def _wait_for_condition(self, predicate, failure_message: str, retries: int = 10, delay_seconds: int = 2):
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

        raise TimeoutError(f"{failure_message} after {retries} attempts ({retries * delay_seconds} seconds).")
