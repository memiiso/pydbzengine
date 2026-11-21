import unittest

from pydbzengine import (
    BasePythonChangeHandler,
    DebeziumEngine,
    DebeziumJsonEngine,
    EventFormat,
    RecordCommitter,
)


class DummyHandler(BasePythonChangeHandler):
    def handleJsonBatch(self, records) -> None:
        pass


class TestDebeziumEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from pydbzengine.engine.jvm import ensure_jvm_started

        ensure_jvm_started()

    def test_config_validation_missing_properties_raises_error(self) -> None:
        with self.assertRaisesRegex(ValueError, ".*Please provide debezium config.*"):
            DebeziumEngine(properties=None, handler=DummyHandler())

    def test_config_validation_missing_handler_raises_error(self) -> None:
        with self.assertRaisesRegex(ValueError, ".*Please provide handler.*"):
            DebeziumEngine(properties={"name": "my-connector"}, handler=None)  # type: ignore[arg-type]

    def test_engine_run_with_invalid_connector_config_raises_error(self) -> None:
        props = {
            "name": "my-connector",
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "transforms": "router",
            "transforms.router.type": "org.apache.kafka.connect.transforms.NotExists",
        }

        with self.assertRaisesRegex(
            Exception, ".*Error.*while.*instantiating.*transformation.*router"
        ):
            engine = DebeziumEngine(properties=props, handler=DummyHandler())
            engine.run()

    def test_handler_exception_propagation(self) -> None:
        props = {
            "name": "my-connector",
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        }

        class FailingHandler(BasePythonChangeHandler):
            def handleJsonBatch(self, records) -> None:
                raise ValueError("Oops, simulation error!")

        class DummyCommitter(RecordCommitter):
            def markProcessed(self, record) -> None:
                pass

            def markBatchFinished(self) -> None:
                pass

        handler = FailingHandler()
        engine = DebeziumEngine(properties=props, handler=handler)

        class DummyJavaEngine:
            def run(self) -> None:
                try:
                    engine.consumer.handleBatch([], DummyCommitter())
                except Exception:
                    pass

        # Direct attribute assignment rather than __dict__ modification
        engine._engine = DummyJavaEngine()

        try:
            with self.assertRaisesRegex(ValueError, "Oops, simulation error!"):
                engine.run()
        finally:
            from pydbzengine.engine._jvm import JThread

            JThread.interrupted()

    def test_format_resolution_defaults_and_options(self) -> None:
        from pydbzengine.engine._jvm import EngineFormat

        handler = DummyHandler()
        # Default is json
        e_default = DebeziumEngine(properties={"name": "test"}, handler=handler)
        self.assertEqual(e_default._resolve_format(), EngineFormat.JSON)

        # Explicit string format 'json'
        e_json = DebeziumEngine(
            properties={"name": "test"}, handler=handler, format="json"
        )
        self.assertEqual(e_json._resolve_format(), EngineFormat.JSON)

        # Explicit string format 'connect'
        e_conn = DebeziumEngine(
            properties={"name": "test"}, handler=handler, format="connect"
        )
        self.assertEqual(e_conn._resolve_format(), EngineFormat.CONNECT)

        # Case insensitive 'CONNECT'
        e_conn_upper = DebeziumEngine(
            properties={"name": "test"}, handler=handler, format="CONNECT"
        )
        self.assertEqual(e_conn_upper._resolve_format(), EngineFormat.CONNECT)

        # Explicit EventFormat enum
        e_direct = DebeziumEngine(
            properties={"name": "test"}, handler=handler, format=EventFormat.CONNECT
        )
        self.assertEqual(e_direct._resolve_format(), EngineFormat.CONNECT)

        # Backward-compatible Java EngineFormat classes and engine.properties / engine.format
        e_java_conn = DebeziumEngine(
            properties={"name": "test"}, handler=handler, format=EngineFormat.CONNECT
        )
        self.assertEqual(e_java_conn._resolve_format(), EngineFormat.CONNECT)
        self.assertEqual(e_java_conn.format, EventFormat.CONNECT)
        self.assertEqual(e_java_conn.properties["name"], "test")

        e_java_json = DebeziumEngine(
            properties={"name": "test"}, handler=handler, format=EngineFormat.JSON
        )
        self.assertEqual(e_java_json._resolve_format(), EngineFormat.JSON)
        self.assertEqual(e_java_json.format, EventFormat.JSON)

    def test_raw_handler_accepts_non_schema_converters_and_asymmetric_schemas(
        self,
    ) -> None:
        import jpype

        handler = DummyHandler()
        e_str_conv = DebeziumEngine(
            properties={
                "name": "test",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            },
            handler=handler,
        )
        self.assertEqual(e_str_conv.format, EventFormat.JSON)
        self.assertEqual(
            e_str_conv.properties["key.converter"],
            "org.apache.kafka.connect.storage.StringConverter",
        )

        e_asym = DebeziumEngine(
            properties={
                "name": "test",
                "value.converter.schemas.enable": "false",
                "key.converter.schemas.enable": "true",
            },
            handler=handler,
        )
        self.assertFalse(e_asym.config.value_schemas_enabled)
        self.assertTrue(e_asym.config.key_schemas_enabled)
        self.assertNotIn(
            "converter.schemas.enable", e_asym.config.to_engine_properties()
        )

        # Custom Java format class passthrough for non-schema handlers
        custom_java_cls = jpype.JClass("io.debezium.engine.format.CloudEvents")
        e_custom = DebeziumEngine(
            properties={"name": "test"}, handler=handler, format=custom_java_cls
        )
        self.assertIs(e_custom.format, custom_java_cls)
        self.assertIs(e_custom._resolve_format(), custom_java_cls)

    def test_format_resolution_invalid_format_raises_error(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported format"):
            DebeziumEngine(
                properties={"name": "test"},
                handler=DummyHandler(),
                format="unsupported_format",
            )

    def test_context_manager_lifecycle(self) -> None:
        class MockEngineInternal:
            closed = False

            def close(self) -> None:
                self.closed = True

        mock_internal = MockEngineInternal()
        with DebeziumEngine(
            properties={"name": "test"}, handler=DummyHandler()
        ) as engine:
            engine._engine = mock_internal
            self.assertFalse(mock_internal.closed)

        self.assertTrue(mock_internal.closed)
        self.assertIsNone(engine._engine)


class TestDebeziumJsonEngine(unittest.TestCase):
    def test_backward_compatibility_subclass_defaults_to_json(self) -> None:
        from pydbzengine.engine._jvm import EngineFormat

        engine = DebeziumJsonEngine(properties={"name": "test"}, handler=DummyHandler())
        self.assertIsInstance(engine, DebeziumEngine)
        self.assertEqual(engine.config.format, "json")
        self.assertEqual(engine._resolve_format(), EngineFormat.JSON)

    def test_subclass_validation_missing_properties_raises_error(self) -> None:
        with self.assertRaisesRegex(ValueError, ".*Please provide debezium config.*"):
            DebeziumJsonEngine(properties=None, handler=DummyHandler())

    def test_subclass_validation_missing_handler_raises_error(self) -> None:
        with self.assertRaisesRegex(ValueError, ".*Please provide handler.*"):
            DebeziumJsonEngine(properties={"name": "test"}, handler=None)  # type: ignore[arg-type]
