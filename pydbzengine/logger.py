from __future__ import annotations

import functools
import logging
from typing import ClassVar


class LoggingMixin:
    """
    Provides a lazily-cached, hierarchical logger for object instances.

    The logger name defaults to `<module>.<qualname>` (e.g. 'pydbzengine.sinks.iceberg.writer.IcebergTableWriter'),
    or respects `LOGGER_NAME` if explicitly defined on the class.
    """

    LOGGER_NAME: ClassVar[str | None] = None

    @functools.cached_property
    def logger(self) -> logging.Logger:
        cls = self.__class__
        logger_name = self.LOGGER_NAME or f"{cls.__module__}.{cls.__qualname__}"
        return logging.getLogger(logger_name)

    @property
    def log(self) -> logging.Logger:
        """Backward-compatibility alias for legacy handlers expecting self.log."""
        return self.logger
