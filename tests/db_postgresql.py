from pathlib import Path

from testcontainers.core.config import testcontainers_config
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from pydbzengine import Properties


def wait_for_pg_start(self) -> None:
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
    PostgresContainer._connect = wait_for_pg_start

    def start(self):
        print("Starting Postgresql Db...")
        testcontainers_config.ryuk_disabled = True
        self.CONTAINER.start()

    def stop(self):
        print("Stopping Postgresql Db...")
        self.CONTAINER.stop()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def get_debezium_engine_properties(self, unwrap_messages=True):
        current_dir = Path(__file__).parent
        offset_file_path = current_dir.joinpath('postgresql-offsets.dat')

        props = Properties()
        props.setProperty("name", "engine")
        props.setProperty("snapshot.mode", "always")
        props.setProperty("database.hostname", self.CONTAINER.get_container_host_ip())
        props.setProperty("database.port",
                          self.CONTAINER.get_exposed_port(self.POSTGRES_PORT_DEFAULT))
        props.setProperty("database.user", self.POSTGRES_USER)
        props.setProperty("database.password", self.POSTGRES_PASSWORD)
        props.setProperty("database.dbname", self.POSTGRES_DBNAME)
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        props.setProperty("offset.storage.file.filename", offset_file_path.as_posix())
        props.setProperty("poll.interval.ms", "10000")
        props.setProperty("converter.schemas.enable", "false")
        props.setProperty("offset.flush.interval.ms", "1000")
        props.setProperty("topic.prefix", "testc")
        props.setProperty("schema.whitelist", "inventory")
        props.setProperty("database.whitelist", "inventory")
        props.setProperty("table.whitelist", "inventory.products")
        props.setProperty("replica.identity.autoset.values", "inventory.*:FULL")

        if unwrap_messages:
            props.setProperty("transforms", "unwrap")
            props.setProperty("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
            props.setProperty("transforms.unwrap.add.fields", "op,table,source.ts_ms,sourcedb,ts_ms")
            props.setProperty("transforms.unwrap.delete.handling.mode", "rewrite")
        return props
