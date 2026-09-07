from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Protocol, runtime_checkable

from pydbzengine.logger import LoggingMixin
from pydbzengine.schema.models import CanonicalSchema

__all__ = [
    "BaseSchemaReader",
    "SupportsChangeEvent",
]


@runtime_checkable
class SupportsChangeEvent(Protocol):
    """Protocol for CDC events exposing value, key, and destination accessors."""

    def value(self) -> Any: ...
    def key(self) -> Any: ...
    def destination(self) -> str: ...
    def partition(self) -> int: ...


class BaseSchemaReader(ABC, LoggingMixin):
    """Abstract base class for extracting canonical schemas from CDC change events."""

    @abstractmethod
    def can_read(self, record: Any) -> bool:
        """Returns True if this reader can extract schema from the given record."""
        pass

    @abstractmethod
    def extract_schema(
        self,
        record: Any,
        flattening_enabled: bool = True,
    ) -> CanonicalSchema:
        """Strictly extracts a CanonicalSchema from the record."""
        pass
