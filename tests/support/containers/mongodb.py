from __future__ import annotations

import time

import jpype
from testcontainers.core.config import testcontainers_config
from testcontainers.core.container import DockerContainer

from pydbzengine.engine.jvm import ensure_jvm_started


class MongoDbHelper:
    MONGO_IMAGE = "quay.io/debezium/example-mongodb:3.4"
    MONGO_PORT_DEFAULT = 27017
    MONGO_DBNAME = "inventory"
    MONGO_ADMIN_USER = "admin"
    MONGO_ADMIN_PASSWORD = "admin"
    MONGO_USER = "debezium"
    MONGO_PASSWORD = "dbz"

    def start(self) -> None:
        ensure_jvm_started()
        self.source_mongo_db: DockerContainer = DockerContainer(
            self.MONGO_IMAGE
        ).with_exposed_ports(self.MONGO_PORT_DEFAULT)
        testcontainers_config.ryuk_disabled = True
        self.source_mongo_db.start()
        self._init_inventory_and_wait()

    def _get_admin_direct_uri(self) -> str:
        host = self.source_mongo_db.get_container_host_ip()
        port = self.source_mongo_db.get_exposed_port(self.MONGO_PORT_DEFAULT)
        return (
            f"mongodb://{self.MONGO_ADMIN_USER}:{self.MONGO_ADMIN_PASSWORD}"
            f"@{host}:{port}/?directConnection=true&authSource=admin"
        )

    def get_connection_string(self) -> str:
        host = self.source_mongo_db.get_container_host_ip()
        port = self.source_mongo_db.get_exposed_port(self.MONGO_PORT_DEFAULT)
        return (
            f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}"
            f"@{host}:{port}/?replicaSet=rs0&directConnection=true&authSource=admin"
        )

    def _init_inventory_and_wait(self, timeout_sec: float = 60.0) -> None:
        mongo_clients = jpype.JClass("com.mongodb.client.MongoClients")
        bson_doc = jpype.JClass("org.bson.Document")
        deadline = time.monotonic() + timeout_sec
        last_err: Exception | None = None
        initiated = False
        admin_uri = self._get_admin_direct_uri()

        while time.monotonic() < deadline:
            client = None
            try:
                client = mongo_clients.create(admin_uri)
                admin_db = client.getDatabase("admin")
                ping = admin_db.runCommand(bson_doc.parse('{"ping": 1}'))
                if ping is not None and not initiated:
                    exit_code, output = self.source_mongo_db.exec(
                        "bash /usr/local/bin/init-inventory.sh -h localhost"
                    )
                    if exit_code != 0:
                        raise RuntimeError(
                            f"init-inventory.sh failed (exit={exit_code}): {output!r}"
                        )
                    initiated = True
                hello = admin_db.runCommand(bson_doc.parse('{"hello": 1}'))
                if bool(hello.getBoolean("isWritablePrimary", False)):
                    return
            except Exception as e:
                last_err = e
            finally:
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass
            time.sleep(1.0)
        raise TimeoutError(
            f"Debezium example-mongodb container did not become ready within {timeout_sec}s"
        ) from last_err

    def insert_json(self, collection_name: str, json_doc: str) -> None:
        mongo_clients = jpype.JClass("com.mongodb.client.MongoClients")
        bson_doc = jpype.JClass("org.bson.Document")
        client = mongo_clients.create(self.get_connection_string())
        try:
            db = client.getDatabase(self.MONGO_DBNAME)
            coll = db.getCollection(collection_name)
            coll.insertOne(bson_doc.parse(json_doc))
        finally:
            client.close()

    def update_json(
        self, collection_name: str, filter_json: str, update_json: str
    ) -> None:
        mongo_clients = jpype.JClass("com.mongodb.client.MongoClients")
        bson_doc = jpype.JClass("org.bson.Document")
        client = mongo_clients.create(self.get_connection_string())
        try:
            db = client.getDatabase(self.MONGO_DBNAME)
            coll = db.getCollection(collection_name)
            coll.updateOne(bson_doc.parse(filter_json), bson_doc.parse(update_json))
        finally:
            client.close()

    def delete_json(self, collection_name: str, filter_json: str) -> None:
        mongo_clients = jpype.JClass("com.mongodb.client.MongoClients")
        bson_doc = jpype.JClass("org.bson.Document")
        client = mongo_clients.create(self.get_connection_string())
        try:
            db = client.getDatabase(self.MONGO_DBNAME)
            coll = db.getCollection(collection_name)
            coll.deleteOne(bson_doc.parse(filter_json))
        finally:
            client.close()

    def drop_collection(self, collection_name: str) -> None:
        mongo_clients = jpype.JClass("com.mongodb.client.MongoClients")
        client = mongo_clients.create(self.get_connection_string())
        try:
            db = client.getDatabase(self.MONGO_DBNAME)
            db.getCollection(collection_name).drop()
        finally:
            client.close()

    def stop(self) -> None:
        try:
            self.source_mongo_db.stop()
        except Exception:
            pass

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()
