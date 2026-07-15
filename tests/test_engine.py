import unittest

from pydbzengine import BasePythonChangeHandler, DebeziumJsonEngine
from pydbzengine._jvm import Properties


class TestDebeziumJsonEngine(unittest.TestCase):
    def test_wrong_config_raises_error(self):
        class DummyHandler(BasePythonChangeHandler):
            def handleJsonBatch(self, records):
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
        ):  # Wrong message
            engine = DebeziumJsonEngine(properties=props, handler=DummyHandler())
            engine.run()

        # test engine arguments validated
        with self.assertRaisesRegex(
            Exception, ".*Please provide debezium config.*"
        ):  # Wrong message
            engine = DebeziumJsonEngine(properties=None, handler=DummyHandler())
            engine.run()
        with self.assertRaisesRegex(
            Exception, ".*Please provide handler.*"
        ):  # Wrong message
            engine = DebeziumJsonEngine(properties=props, handler=None)
            engine.run()

    def test_handler_exception_propagation(self):
        from unittest.mock import MagicMock

        props = Properties()
        props.setProperty("name", "my-connector")
        props.setProperty(
            "connector.class", "io.debezium.connector.postgresql.PostgresConnector"
        )

        class FailingHandler(BasePythonChangeHandler):
            def handleJsonBatch(self, records):
                raise ValueError("Oops, simulation error!")

        handler = FailingHandler()
        engine = DebeziumJsonEngine(properties=props, handler=handler)

        # Mock the Java engine's run to simulate Java invoking our consumer callback
        mock_java_engine = MagicMock()

        def mock_java_run():
            try:
                engine.consumer.handleBatch([], MagicMock())
            except Exception:
                pass

        mock_java_engine.run.side_effect = mock_java_run
        engine.__dict__["engine"] = mock_java_engine

        try:
            with self.assertRaisesRegex(ValueError, "Oops, simulation error!"):
                engine.run()
        finally:
            from pydbzengine._jvm import JavaLangThread

            JavaLangThread.interrupted()
