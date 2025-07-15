import unittest

from pyiceberg.catalog import load_catalog

from base_postgresql_test import BasePostgresqlTest
from catalog_rest import CatalogRestContainer
from pydbzengine import DebeziumJsonEngine
from pydbzengine.handlers.iceberg import IcebergChangeHandler, DEBEZIUM_EVENT_SCHEMA
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
        table = catalog.create_table(identifier=("my_warehouse","test_table",), schema=DEBEZIUM_EVENT_SCHEMA)
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

        handler = IcebergChangeHandler(catalog=catalog, destination_namespace=(dest_ns1_database, dest_ns2_schema,))

        dbz_props = self.debezium_engine_props(unwrap_messages=False)
        engine = DebeziumJsonEngine(properties=dbz_props, handler=handler)

        with self.assertLogs(IcebergChangeHandler.LOGGER_NAME, level='INFO') as cm:
            # run async then interrupt after timeout time to test the result!
            Utils.run_engine_async(engine=engine, timeout_sec=44)

        for t in cm.output:
            print(t)
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
        print(data)
        self.assertEqual(data.num_rows, 4)