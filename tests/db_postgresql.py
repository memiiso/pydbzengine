from testcontainers.core.config import testcontainers_config
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer


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
        testcontainers_config.ryuk_disabled = True
        self.CONTAINER.start()
        print(f"Postgresql Started: {self.CONTAINER.get_connection_url()}")

    def stop(self):
        try:
            self.CONTAINER.stop()
        except:
            pass

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
