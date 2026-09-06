import unittest

from pydbzengine import BasePythonChangeHandler, DebeziumJsonEngine, RecordCommitter
from pydbzengine._jvm import Properties


class TestDebeziumJsonEngine(unittest.TestCase):
    def test_wrong_config_raises_error(self) -> None:
        class DummyHandler(BasePythonChangeHandler):
            def handleJsonBatch(self, records) -> None:
                pass

        props = Properties()
        props.setProperty("name", "my-connector")
        props.setProperty(
            "connector.class", "io.debezium.connector.postgresql.PostgresConnector"
        )
        props.setProperty("transforms", "router")
        props.setProperty(
            "transforms.router.type", "org.apache.kafka.connect.transforms.NotExists"
        )

        with self.assertRaisesRegex(
            Exception, ".*Error.*while.*instantiating.*transformation.*router"
        ):
            engine = DebeziumJsonEngine(properties=props, handler=DummyHandler())
            engine.run()

        # Engine arguments validated fail-fast
        with self.assertRaisesRegex(ValueError, ".*Please provide debezium config.*"):
            DebeziumJsonEngine(properties=None, handler=DummyHandler())

        with self.assertRaisesRegex(ValueError, ".*Please provide handler.*"):
            DebeziumJsonEngine(properties=props, handler=None)  # type: ignore[arg-type]

    def test_handler_exception_propagation(self) -> None:
        props = Properties()
        props.setProperty("name", "my-connector")
        props.setProperty(
            "connector.class", "io.debezium.connector.postgresql.PostgresConnector"
        )

        class FailingHandler(BasePythonChangeHandler):
            def handleJsonBatch(self, records) -> None:
                raise ValueError("Oops, simulation error!")

        class DummyCommitter(RecordCommitter):
            def markProcessed(self, record) -> None:
                pass

            def markBatchFinished(self) -> None:
                pass

        handler = FailingHandler()
        engine = DebeziumJsonEngine(properties=props, handler=handler)

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
            from pydbzengine._jvm import JavaLangThread

            JavaLangThread.interrupted()
