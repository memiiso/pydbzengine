import unittest

import pandas as pd
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

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
        pd.set_option('display.max_rows', None)         # Show all rows
        pd.set_option('display.max_columns', None)      # Show all columns
        pd.set_option('display.width', None)            # Auto-detect terminal width
        pd.set_option('display.max_colwidth', None)     # Do not truncate cell contents

    def tearDown(self):
        self.SOURCEPGDB.stop()
        self.S3MiNIO.stop()
        self.clean_offset_file()

    @unittest.skip
    def test_iceberg_catalog(self):
        conf = {
            "uri": self.RESTCATALOG.get_uri(),
            # "s3.path-style.access": "true",
            "warehouse": "warehouse",
            "s3.endpoint": self.S3MiNIO.endpoint(),
            "s3.access-key-id": S3Minio.AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": S3Minio.AWS_SECRET_ACCESS_KEY,
        }
        print(conf)
        catalog = load_catalog(
            name="rest",
            **conf
        )
        catalog.create_namespace('my_warehouse')
        debezium_event_schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=False),
        )
        table = catalog.create_table(identifier=("my_warehouse", "test_table",), schema=debezium_event_schema)
        print(f"Created iceberg table {table.refs()}")

    def test_iceberg_handler(self):
        dest_ns1_database="my_warehouse"
        dest_ns2_schema="dbz_cdc_data"
        conf = {
            "uri": self.RESTCATALOG.get_uri(),
            # "s3.path-style.access": "true",
            "warehouse": "warehouse",
            "s3.endpoint": self.S3MiNIO.endpoint(),
            "s3.access-key-id": S3Minio.AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": S3Minio.AWS_SECRET_ACCESS_KEY,
        }
        catalog = load_catalog(name="rest",**conf)

        handler = IcebergChangeHandlerV2(catalog=catalog,
                                         destination_namespace=(dest_ns1_database, dest_ns2_schema,),
                                         event_flattening_enabled=True
                                         )

        dbz_props = self.debezium_engine_props(unwrap_messages=True)
        engine = DebeziumJsonEngine(properties=dbz_props, handler=handler)
        with self.assertLogs(IcebergChangeHandlerV2.LOGGER_NAME, level='INFO') as cm:
            # run async then interrupt after timeout time to test the result!
            Utils.run_engine_async(engine=engine, timeout_sec=44)

        # for t in cm.output:
        #     print(t)
        self.assertRegex(text=str(cm.output), expected_regex='.*Created iceberg table.*')
        self.assertRegex(text=str(cm.output), expected_regex='.*Appended.*records to table.*')

        # catalog.create_namespace(dest_ns1_database)
        namespaces = catalog.list_namespaces()
        self.assertIn((dest_ns1_database,) , namespaces, msg="Namespace not found in catalog")

        tables = catalog.list_tables((dest_ns1_database, dest_ns2_schema,))
        print(tables)
        self.assertIn(('my_warehouse', 'dbz_cdc_data', 'testc_inventory_customers'), tables, msg="Namespace not found in catalog")

        tbl = catalog.load_table(identifier=('my_warehouse', 'dbz_cdc_data', 'testc_inventory_customers'))
        data = tbl.scan().to_arrow()
        self.assertIn("sally.thomas@acme.com", str(data))
        self.assertIn("annek@noanswer.org", str(data))
        self.assertEqual(data.num_rows, 4)
        self.pprint_table(data=data)
        #=================================================================
        ## ==== PART 2 CONSUME CHANGES FROM BINLOG =======================
        #=================================================================
        self.execute_on_source_db("UPDATE inventory.customers SET first_name='George__UPDATE1' WHERE ID = 1002 ;")
        # self.execute_on_source_db("ALTER TABLE inventory.customers DROP COLUMN email;")
        self.execute_on_source_db("UPDATE inventory.customers SET first_name='George__UPDATE2'  WHERE ID = 1002 ;")
        self.execute_on_source_db("DELETE FROM inventory.orders WHERE purchaser = 1002 ;")
        self.execute_on_source_db("DELETE FROM inventory.customers WHERE id = 1002 ;")
        self.execute_on_source_db("ALTER TABLE inventory.customers ADD birth_date date;")
        self.execute_on_source_db("UPDATE inventory.customers SET birth_date = '2020-01-01'  WHERE id = 1001 ;")
        # run
        Utils.run_engine_async(engine=engine, timeout_sec=44)
        # test
        # @TODO test that new field is received and added to iceberg!
        data = tbl.scan().to_arrow()
        self.pprint_table(data=data)
        self.assertEqual(data.num_rows, 4)

    def pprint_table(self, data):
        print("--- Iceberg Table Content ---")
        print(data.to_pandas())
        print("---------------------------\n")
