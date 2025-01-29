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
    # start PG container which is used as replication source
    sourcedb = DbPostgresql()
    sourcedb.start()
    # get debezium engine configuration Properties
    props = debezium_engine_props(sourcedb=sourcedb)
    # create dlt pipeline to consume events to duckdb
    dlt_pipeline = dlt.pipeline(
        pipeline_name="dbz_cdc_events_example",
        destination="duckdb",
        dataset_name="dbz_data"
    )
    # create handler class, which will process generated debezium events wih dlt
    handler = DltChangeHandler(dlt_pipeline=dlt_pipeline)
    # give the config and the handler class to the DebeziumJsonEngine
    engine = DebeziumJsonEngine(properties=props, handler=handler)
    # run the engine async then interrupt after timeout seconds, to test the result!
    Utils.run_engine_async(engine=engine, timeout_sec=60)
    # engine.run()

    # ================ PRINT THE CONSUMED DATA ===========================
    con = duckdb.connect(DUCKDB_FILE.as_posix())
    result = con.sql("SHOW ALL TABLES").fetchall()
    for r in result:
        database, schema, table = r[:3]
        if schema == "dbz_data":
            con.sql(f"select * from {database}.{schema}.{table}").show()

if __name__ == "__main__":
    main()