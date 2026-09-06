from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class CanonicalPrimitiveType(str, Enum):
    BOOLEAN = "boolean"
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    BINARY = "binary"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"  # microsecond without timezone
    TIMESTAMPTZ = "timestamptz"  # microsecond with timezone (UTC)
    DECIMAL = "decimal"  # with precision and scale
    UUID = "uuid"
    STRUCT = "struct"
    LIST = "list"
    MAP = "map"


@dataclass(frozen=True)
class CanonicalType:
    primitive: CanonicalPrimitiveType
    precision: int | None = None
    scale: int | None = None
    fields: tuple[CanonicalField, ...] | None = None  # for STRUCT
    element_type: CanonicalType | None = None  # for LIST
    key_type: CanonicalType | None = None  # for MAP
    value_type: CanonicalType | None = None  # for MAP

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"primitive": self.primitive.value}
        if self.precision is not None:
            d["precision"] = self.precision
        if self.scale is not None:
            d["scale"] = self.scale
        if self.fields is not None:
            d["fields"] = [f.to_dict() for f in self.fields]
        if self.element_type is not None:
            d["element_type"] = self.element_type.to_dict()
        if self.key_type is not None:
            d["key_type"] = self.key_type.to_dict()
        if self.value_type is not None:
            d["value_type"] = self.value_type.to_dict()
        return d


@dataclass(frozen=True)
class CanonicalField:
    name: str
    field_type: CanonicalType
    optional: bool = True
    doc: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "name": self.name,
            "field_type": self.field_type.to_dict(),
            "optional": self.optional,
        }
        if self.doc is not None:
            d["doc"] = self.doc
        return d


@dataclass(frozen=True)
class CanonicalSchema:
    identifier: str
    fields: tuple[CanonicalField, ...]
    primary_keys: tuple[str, ...] = ()
    fingerprint: str = field(default="")

    def __post_init__(self) -> None:
        if not self.fingerprint:
            # Deterministic fingerprint from field definitions and primary keys
            payload = {
                "identifier": self.identifier,
                "fields": [f.to_dict() for f in self.fields],
                "primary_keys": sorted(self.primary_keys),
            }
            canonical_json = json.dumps(payload, sort_keys=True)
            sha = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
            # Object is frozen, so use object.__setattr__
            object.__setattr__(self, "fingerprint", sha)

    @property
    def field_names(self) -> list[str]:
        return [f.name for f in self.fields]

    def find_field(self, name: str) -> CanonicalField | None:
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def same_schema(self, other: CanonicalSchema) -> bool:
        return self.fingerprint == other.fingerprint
