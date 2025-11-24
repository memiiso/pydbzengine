import json

from minio import Minio
from testcontainers.core.config import testcontainers_config
from testcontainers.minio import MinioContainer


class S3Minio:
    AWS_ACCESS_KEY_ID = "admin"
    AWS_SECRET_ACCESS_KEY = "password"
    AWS_REGION = "us-east-1"
    S3_WAREHOUSE_BUCKET = "icebergdata"

    def __init__(self, image="minio/minio:RELEASE.2025-04-08T15-41-24Z"):
        self.minio = MinioContainer(
            image=image,
            access_key=self.AWS_ACCESS_KEY_ID,
            secret_key=self.AWS_SECRET_ACCESS_KEY,
            port=9000
        ).with_exposed_ports(9000).with_exposed_ports(9001)
        self._client = None

    def get_client(self):
        host_ip = self.minio.get_container_host_ip()
        exposed_port = self.minio.get_exposed_port(self.minio.port)
        return Minio(
            endpoint=f"{host_ip}:{exposed_port}",
            access_key=self.minio.access_key,
            secret_key=self.minio.secret_key,
            secure=False
        )

    def start(self):
        testcontainers_config.ryuk_disabled = True
        self.minio.start()
        self._client = self.get_client()
        print(f"Minio Started: {self.endpoint()}")
        print(f"Minio Web: {self.web_url()}")
        self.setup_warehouse_bucket()
        return self

    def endpoint(self) -> str:
        host_ip = self.minio.get_container_host_ip()
        exposed_port = self.minio.get_exposed_port(self.minio.port)
        return f"http://{host_ip}:{exposed_port}"

    def web_url(self) -> str:
        host_ip = self.minio.get_container_host_ip()
        exposed_port = self.minio.get_exposed_port(9001)
        return f"http://{host_ip}:{exposed_port}"

    def setup_warehouse_bucket(self):
        bucket_name = self.S3_WAREHOUSE_BUCKET
        self.client.make_bucket(bucket_name=bucket_name)

        # Equivalent to `/usr/bin/mc policy set public minio/warehouse`
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
                }
            ]
        }
        self.client.set_bucket_policy(
            bucket_name=bucket_name,
            policy=json.dumps(policy)
        )
        print(f"Bucket '{bucket_name}' created successfully.")

    def stop(self):
        """Stops the Minio container."""
        if self.minio and self.minio.get_wrapped_container():
            self.minio.stop()

    @property
    def client(self):
        if not self._client:
            raise RuntimeError(
                "Client not initialized. Call start() or use as a context manager first."
            )
        return self._client

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
