from unittest import TestCase

from pydbzengine import DebeziumJsonEngine
from pydbzengine.handlers.dlt import DltChangeHandler
from pydbzengine.handlers.iceberg import IcebergChangeHandler
from pydbzengine.logger import LoggingMixin


class DummySampleClass(LoggingMixin):
    pass


class DummyCustomNamedClass(LoggingMixin):
    LOGGER_NAME = "custom.override.Logger"


class TestLoggingMixin(TestCase):
    def test_logger_name_derivation(self) -> None:
        obj = DummySampleClass()
        expected = f"{DummySampleClass.__module__}.{DummySampleClass.__qualname__}"
        self.assertEqual(obj.logger.name, expected)

    def test_logger_caching(self) -> None:
        obj = DummySampleClass()
        first_access = obj.logger
        second_access = obj.logger
        self.assertIs(first_access, second_access)

    def test_log_alias(self) -> None:
        obj = DummySampleClass()
        self.assertIs(obj.log, obj.logger)

    def test_custom_logger_name_override(self) -> None:
        obj = DummyCustomNamedClass()
        self.assertEqual(obj.logger.name, "custom.override.Logger")
        self.assertEqual(obj.log.name, "custom.override.Logger")

    def test_engine_inherits_logging(self) -> None:
        class DummyHandler:
            pass

        engine = DebeziumJsonEngine(properties={}, handler=DummyHandler())  # type: ignore[arg-type]
        expected = f"{DebeziumJsonEngine.__module__}.{DebeziumJsonEngine.__qualname__}"
        self.assertEqual(engine.logger.name, expected)
        self.assertEqual(engine.log.name, expected)

    def test_legacy_dlt_handler_logger_name_preserved(self) -> None:
        class MockPipeline:
            pass

        handler = DltChangeHandler(dlt_pipeline=MockPipeline())
        self.assertEqual(handler.log.name, DltChangeHandler.LOGGER_NAME)
        self.assertEqual(handler.logger.name, DltChangeHandler.LOGGER_NAME)

    def test_legacy_iceberg_handler_logger_name_preserved(self) -> None:
        class MockCatalog:
            pass

        handler = IcebergChangeHandler(
            catalog=MockCatalog(),
            destination_namespace=("default",),
        )
        self.assertEqual(handler.log.name, IcebergChangeHandler.LOGGER_NAME)
        self.assertEqual(handler.logger.name, IcebergChangeHandler.LOGGER_NAME)
