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

    def __post_init__(self) -> None:
        if self.primitive == CanonicalPrimitiveType.DECIMAL:
            if self.scale is not None and self.precision is not None:
                if not (0 <= self.scale <= self.precision):
                    raise ValueError(
                        f"Decimal scale ({self.scale}) must be between 0 and precision ({self.precision})"
                    )
        elif self.scale is not None:
            raise ValueError(
                f"Scale cannot be defined on non-DECIMAL type: {self.primitive}"
            )

        if self.primitive == CanonicalPrimitiveType.STRUCT:
            if not self.fields:
                raise ValueError("STRUCT type requires at least one field definition.")
        elif self.fields is not None:
            raise ValueError(
                f"Fields cannot be defined on non-STRUCT type: {self.primitive}"
            )

        if self.primitive == CanonicalPrimitiveType.LIST:
            if self.element_type is None:
                raise ValueError("LIST type requires an element_type.")
        elif self.element_type is not None:
            raise ValueError(
                f"Element type cannot be defined on non-LIST type: {self.primitive}"
            )

        if self.primitive == CanonicalPrimitiveType.MAP:
            if self.key_type is None or self.value_type is None:
                raise ValueError("MAP type requires both key_type and value_type.")
        elif self.key_type is not None or self.value_type is not None:
            raise ValueError(
                f"Key/value types cannot be defined on non-MAP type: {self.primitive}"
            )

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
            # Deterministic structural fingerprint computed from fields and sorted primary keys
            payload = {
                "fields": [f.to_dict() for f in self.fields],
                "primary_keys": sorted(self.primary_keys),
            }
            canonical_json = json.dumps(payload, sort_keys=True)
            sha = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
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
