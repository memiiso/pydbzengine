from testcontainers.core.config import testcontainers_config
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


class CatalogRestContainer(DockerContainer):
    IMAGE = "apache/iceberg-rest-fixture:latest"
    REST_PORT = 8181
    READY_LOG_MESSAGE = "Server - Started"

    def __init__(self):
        super().__init__(self.IMAGE)
        self.with_exposed_ports(self.REST_PORT)
        # self.with_env("AWS_ACCESS_KEY_ID", S3Minio.AWS_ACCESS_KEY_ID)
        # self.with_env("AWS_SECRET_ACCESS_KEY", S3Minio.AWS_SECRET_ACCESS_KEY)
        # self.with_env("AWS_REGION", S3Minio.AWS_REGION)
        # self.with_env("CATALOG_WAREHOUSE", f"s3://{S3Minio.S3_WAREHOUSE_BUCKET}/")
        # self.with_env("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        # self.with_env("CATALOG_S3_PATH__STYLE__ACCESS", "true")

    def start(self, s3_endpoint:str):
        self.with_env("CATALOG_S3_ENDPOINT", s3_endpoint)
        testcontainers_config.ryuk_disabled = True
        super().start()
        wait_for_logs(self, self.READY_LOG_MESSAGE, timeout=60)
        print(f"Iceberg REST Catalog Started: {self.get_uri()}")
        return self

    def get_uri(self) -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.REST_PORT)
        return f"http://{host}:{port}"

    def get_catalog(self) -> 'Catalog':
        from pyiceberg.catalog import load_catalog
        catalog = load_catalog(
            "default",  # Catalog name
            **{
                "type": "rest",
                "uri": self.get_uri(),
            }
        )
        return catalog

    def list_namespaces(self):
        catalog = self.get_catalog()
        namespaces = catalog.list_namespaces()
        print("Namespaces:", namespaces)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
