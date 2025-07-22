import io
import threading
import time

import pandas as pd
import pyarrow as pa
import pyarrow.json as pj
import waiting
from pyiceberg.catalog import load_catalog

from base_postgresql_test import BasePostgresqlTest
from catalog_rest import CatalogRestContainer
from pydbzengine import DebeziumJsonEngine
from pydbzengine.handlers.iceberg import IcebergChangeHandlerV2
from pydbzengine.helper import Utils
from s3_minio import S3Minio


class TestIcebergChangeHandler(BasePostgresqlTest):
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
        catalog = load_catalog(name="rest", **catalog_conf)
        handler = IcebergChangeHandlerV2(catalog=catalog,
                                         destination_namespace=(dest_ns1_database, dest_ns2_schema,),
                                         event_flattening_enabled=True
                                         )
        dbz_props = self.debezium_engine_props(unwrap_messages=True)
        engine = DebeziumJsonEngine(properties=dbz_props, handler=handler)

        t = threading.Thread(target=self._apply_source_db_changes)
        t.start()
        Utils.run_engine_async(engine=engine, timeout_sec=77, blocking=False)

        test_ns = (dest_ns1_database,)
        print(catalog.list_namespaces())
        waiting.wait(predicate=lambda: test_ns in catalog.list_namespaces(), timeout_seconds=7.5)

        test_tbl = ('my_warehouse', 'dbz_cdc_data', 'testc_inventory_customers')
        test_tbl_ns = (dest_ns1_database, dest_ns2_schema,)
        waiting.wait(predicate=lambda: test_tbl in catalog.list_tables(test_tbl_ns), timeout_seconds=10.5)

        test_tbl_data = ('my_warehouse', 'dbz_cdc_data', 'testc_inventory_customers')
        waiting.wait(predicate=lambda: "sally.thomas@acme.com" in str(self.red_table(catalog, test_tbl_data)),
                     timeout_seconds=10.5)
        waiting.wait(predicate=lambda: self.red_table(catalog, test_tbl_data).num_rows >= 4, timeout_seconds=10.5)

        data = self.red_table(catalog, test_tbl_data)
        self.pprint_table(data=data)
        # =================================================================
        ## ==== PART 2 CONSUME CHANGES FROM BINLOG ========================
        # =================================================================
        waiting.wait(predicate=lambda: self.red_table(catalog, test_tbl_data).num_rows >= 7, timeout_seconds=77)
        data = self.red_table(catalog, test_tbl_data)
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
