import unittest

from pydbzengine import BasePythonChangeHandler, DebeziumJsonEngine
from pydbzengine._jvm import Properties


class TestDebeziumJsonEngine(unittest.TestCase):

    def test_wrong_config_raises_error(self):
        props = Properties()
        props.setProperty("name", "my-connector")
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("transforms", "router")
        props.setProperty("transforms.router.type", "org.apache.kafka.connect.transforms.NotExists")

        with self.assertRaisesRegex(Exception,
                                    ".*Error.*while.*instantiating.*transformation.*router"):  # Wrong message
            engine = DebeziumJsonEngine(properties=props, handler=BasePythonChangeHandler())
            engine.run()

        # test engine arguments validated
        with self.assertRaisesRegex(Exception,
                                    ".*Please provide debezium config.*"):  # Wrong message
            engine = DebeziumJsonEngine(properties=None, handler=BasePythonChangeHandler())
            engine.run()
        with self.assertRaisesRegex(Exception,
                                    ".*Please provide handler.*"):  # Wrong message
            engine = DebeziumJsonEngine(properties=props, handler=None)
            engine.run()
