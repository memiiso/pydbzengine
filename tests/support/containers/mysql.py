from __future__ import annotations

import time

import jpype
from testcontainers.core.config import testcontainers_config
from testcontainers.core.container import DockerContainer

from pydbzengine.engine.jvm import ensure_jvm_started


class MySqlDbHelper:
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "debezium"
    MYSQL_DBNAME = "inventory"
    MYSQL_IMAGE = "debezium/example-mysql:2.7.3.Final"
    MYSQL_PORT_DEFAULT = 3306

    def start(self) -> None:
        ensure_jvm_started()
        self.source_mysql_db: DockerContainer = (
            DockerContainer(self.MYSQL_IMAGE)
            .with_exposed_ports(self.MYSQL_PORT_DEFAULT)
            .with_env("MYSQL_ROOT_PASSWORD", self.MYSQL_PASSWORD)
            .with_env("MYSQL_USER", "mysqluser")
            .with_env("MYSQL_PASSWORD", "mysqlpw")
        )
        testcontainers_config.ryuk_disabled = True
        self.source_mysql_db.start()
        self._wait_until_ready()

    def get_jdbc_url(self) -> str:
        host = self.source_mysql_db.get_container_host_ip()
        port = self.source_mysql_db.get_exposed_port(self.MYSQL_PORT_DEFAULT)
        return (
            f"jdbc:mysql://{host}:{port}/{self.MYSQL_DBNAME}"
            "?allowPublicKeyRetrieval=true&useSSL=false&allowMultiQueries=true"
        )

    def _wait_until_ready(self, timeout_sec: float = 60.0) -> None:
        driver_manager = jpype.JClass("java.sql.DriverManager")
        deadline = time.monotonic() + timeout_sec
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            try:
                conn = driver_manager.getConnection(
                    self.get_jdbc_url(), self.MYSQL_USER, self.MYSQL_PASSWORD
                )
                conn.close()
                return
            except Exception as e:
                last_err = e
                time.sleep(1.0)
        raise TimeoutError(
            f"MySQL container did not become ready within {timeout_sec}s"
        ) from last_err

    def execute_sql(self, sql: str) -> None:
        driver_manager = jpype.JClass("java.sql.DriverManager")
        conn = driver_manager.getConnection(
            self.get_jdbc_url(), self.MYSQL_USER, self.MYSQL_PASSWORD
        )
        try:
            stmt = conn.createStatement()
            try:
                stmt.execute(sql)
            finally:
                stmt.close()
        finally:
            conn.close()

    def stop(self) -> None:
        try:
            self.source_mysql_db.stop()
        except Exception:
            pass

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()
