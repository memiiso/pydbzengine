import sqlalchemy
from sqlalchemy.engine import Connection
from testcontainers.core.config import testcontainers_config
from testcontainers.postgres import PostgresContainer


class PostgresDbHelper:
    POSTGRES_USER = "postgres"
    POSTGRES_PASSWORD = "postgres"
    POSTGRES_DBNAME = "postgres"
    POSTGRES_IMAGE = "quay.io/debezium/example-postgres:3.4"
    POSTGRES_HOST = "localhost"
    POSTGRES_PORT_DEFAULT = 5432

    def start(self):
        self.source_pg_db: PostgresContainer = PostgresContainer(
            image=self.POSTGRES_IMAGE,
            port=self.POSTGRES_PORT_DEFAULT,
            username=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            dbname=self.POSTGRES_DBNAME,
        ).with_exposed_ports(self.POSTGRES_PORT_DEFAULT)
        testcontainers_config.ryuk_disabled = True
        self.source_pg_db.start()
        print(f"Postgresql Started: {self.source_pg_db.get_connection_url()}")

    def stop(self):
        try:
            self.source_pg_db.stop()
        except Exception:
            pass

    def get_connection(self) -> Connection:
        url = self.source_pg_db.get_connection_url()
        print(url)
        engine = sqlalchemy.create_engine(url)
        return engine.connect()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def execute_sql(self, sql: str):
        with self.get_connection() as conn:
            conn.execute(sqlalchemy.text(sql))
