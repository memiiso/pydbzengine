import os
from pathlib import Path

import dlt
import duckdb
from testcontainers.core.config import testcontainers_config
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from pydbzengine import DebeziumJsonEngine, Properties
from pydbzengine.debeziumdlt import DltChangeHandler
from pydbzengine.helper import Utils

# set global variables
CURRENT_DIR = Path(__file__).parent
DUCKDB_FILE = CURRENT_DIR.joinpath("dbz_cdc_events_example.duckdb")
OFFSET_FILE = CURRENT_DIR.joinpath('postgresql-offsets.dat')

# cleanup
if OFFSET_FILE.exists():
    os.remove(OFFSET_FILE)
if DUCKDB_FILE.exists():
    os.remove(DUCKDB_FILE)

def wait_for_postgresql_to_start(self) -> None:
    wait_for_logs(self, ".*database system is ready to accept connections.*")
    wait_for_logs(self, ".*PostgreSQL init process complete.*")


class DbPostgresql:
    POSTGRES_USER = "postgres"
    POSTGRES_PASSWORD = "postgres"
    POSTGRES_DBNAME = "postgres"
    POSTGRES_IMAGE = "debezium/example-postgres:3.0.0.Final"
    POSTGRES_HOST = "localhost"
    POSTGRES_PORT_DEFAULT = 5432
    CONTAINER: PostgresContainer = (PostgresContainer(image=POSTGRES_IMAGE,
                                                      port=POSTGRES_PORT_DEFAULT,
                                                      username=POSTGRES_USER,
                                                      password=POSTGRES_PASSWORD,
                                                      dbname=POSTGRES_DBNAME,
                                                      )
                                    .with_exposed_ports(POSTGRES_PORT_DEFAULT)
                                    )
    PostgresContainer._connect = wait_for_postgresql_to_start

    def start(self):
        testcontainers_config.ryuk_disabled = True
        print("Starting Postgresql Db...")
        self.CONTAINER.start()

    def stop(self):
        print("Stopping Postgresql Db...")
        self.CONTAINER.stop()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()


def debezium_engine_props(sourcedb: DbPostgresql):
    props = Properties()
    props.setProperty("name", "engine")
    props.setProperty("snapshot.mode", "initial_only")
    props.setProperty("database.hostname", sourcedb.CONTAINER.get_container_host_ip())
    props.setProperty("database.port",
                      sourcedb.CONTAINER.get_exposed_port(sourcedb.POSTGRES_PORT_DEFAULT))
    props.setProperty("database.user", sourcedb.POSTGRES_USER)
    props.setProperty("database.password", sourcedb.POSTGRES_PASSWORD)
    props.setProperty("database.dbname", sourcedb.POSTGRES_DBNAME)
    props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    props.setProperty("offset.storage.file.filename", OFFSET_FILE.as_posix())
    props.setProperty("max.batch.size", "5")
    props.setProperty("poll.interval.ms", "10000")
    props.setProperty("converter.schemas.enable", "false")
    props.setProperty("offset.flush.interval.ms", "1000")
    props.setProperty("database.server.name", "testc")
    props.setProperty("database.server.id", "1234")
    props.setProperty("topic.prefix", "testc")
    props.setProperty("schema.whitelist", "inventory")
    props.setProperty("database.whitelist", "inventory")
    props.setProperty("table.whitelist", "inventory.*")
    props.setProperty("replica.identity.autoset.values", "inventory.*:FULL")
    # // debezium unwrap message
    props.setProperty("transforms", "unwrap")
    props.setProperty("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
    props.setProperty("transforms.unwrap.add.fields", "op,table,source.ts_ms,sourcedb,ts_ms")
    props.setProperty("transforms.unwrap.delete.handling.mode", "rewrite")
    # props.setProperty("debezium.transforms.unwrap.drop.tombstones", "true")
    return props

def main():
    """
    Demonstrates capturing change data from PostgreSQL using Debezium and loading
    it into DuckDB using dlt.

    This example starts a PostgreSQL container, configures Debezium to capture changes,
    processes the change events with a custom handler using dlt, and finally queries
    the DuckDB database to display the loaded data.
    """

    # Start the PostgreSQL container that will serve as the replication source.
    sourcedb = DbPostgresql()
    sourcedb.start()

    # Get Debezium engine configuration properties, including connection details
    # for the PostgreSQL database. This function debezium_engine_props returns all the properties
    props = debezium_engine_props(sourcedb=sourcedb)

    # Create a dlt pipeline to load the change events into DuckDB.
    dlt_pipeline = dlt.pipeline(
        pipeline_name="dbz_cdc_events_example",
        destination="duckdb",
        dataset_name="dbz_data"
    )

    # Instantiate change event handler (DltChangeHandler) that uses the dlt pipeline
    # to process and load the Debezium events.  This handler has
    # the logic for transforming and loading the events.
    handler = DltChangeHandler(dlt_pipeline=dlt_pipeline)

    # Create a DebeziumJsonEngine instance, providing the configuration properties
    # and the custom event handler.
    engine = DebeziumJsonEngine(properties=props, handler=handler)

    # Run the Debezium engine asynchronously with a timeout.  This allows the example
    # to run for a limited time and then terminate automatically.
    Utils.run_engine_async(engine=engine, timeout_sec=60)
    # engine.run()  # This would be used for synchronous execution (without timeout)

    # ================ PRINT THE CONSUMED DATA FROM DUCKDB ===========================
    # Connect to the DuckDB database.
    con = duckdb.connect(DUCKDB_FILE.as_posix())

    # Retrieve a list of all tables in the DuckDB database.
    result = con.sql("SHOW ALL TABLES").fetchall()

    # Iterate through the tables and display the data from tables within the 'dbz_data' schema.
    for r in result:
        database, schema, table = r[:3]  # Extract database, schema, and table names.
        if schema == "dbz_data":  # Only show data from the schema where Debezium loaded the data.
            print(f"Data in table {table}:")
            con.sql(f"select * from {database}.{schema}.{table} limit 5").show() # Display table data


if __name__ == "__main__":
    """
    Main entry point for the script.

    Before running, ensure you have installed the necessary dependencies:
    `pip install pydbzengine[dev]`
    """
    main()