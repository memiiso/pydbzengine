from __future__ import annotations

try:
    import pyarrow as _pa
except ImportError as e:
    raise ImportError(
        "pyarrow>=19.0.0 is required to use pydbzengine.schema. "
        "Please install it using 'pip install pydbzengine[arrow]' or 'pip install pyarrow>=19.0.0'."
    ) from e

from pydbzengine.engine.base import EventFormat
from pydbzengine.schema.connect import ConnectEventConverter
from pydbzengine.schema.consumer import SchemaAwareChangeHandler
from pydbzengine.schema.converter import EventConverter
from pydbzengine.schema.json import JsonEventConverter
from pydbzengine.schema.models import (
    ChangeBatch,
    ChangeRecord,
    EventKind,
    SchemaDiff,
    TableBatch,
    TableSchema,
)

__all__ = [
    "ChangeBatch",
    "ChangeRecord",
    "ConnectEventConverter",
    "EventConverter",
    "EventFormat",
    "EventKind",
    "JsonEventConverter",
    "SchemaAwareChangeHandler",
    "SchemaDiff",
    "TableBatch",
    "TableSchema",
]
