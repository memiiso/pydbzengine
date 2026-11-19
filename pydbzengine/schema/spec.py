from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

__all__ = [
    "FieldSpec",
]


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """Normalized field schema specification parsed from JSON or Java Connect Schema."""

    name: str
    type_name: str
    optional: bool = True
    logical_name: str | None = None
    doc: str | None = None
    parameters: Mapping[str, str] = field(default_factory=dict)
    fields: tuple[FieldSpec, ...] = ()
    items: FieldSpec | None = None
    keys: FieldSpec | None = None
    values: FieldSpec | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError(
                f"FieldSpec name must be a non-empty string, got {self.name!r}"
            )
        stripped_name = self.name.strip()
        if stripped_name != self.name:
            object.__setattr__(self, "name", stripped_name)

        if not isinstance(self.type_name, str) or not self.type_name.strip():
            raise ValueError(
                f"FieldSpec type_name must be a non-empty string for field '{self.name}', got {self.type_name!r}"
            )
        norm_type = self.type_name.strip().lower()
        if norm_type != self.type_name:
            object.__setattr__(self, "type_name", norm_type)

        if not isinstance(self.optional, bool):
            raise ValueError(
                f"Expected bool for schema 'optional' in field '{self.name}', got {type(self.optional).__name__}"
            )
        if self.logical_name is not None:
            if not isinstance(self.logical_name, str):
                raise ValueError(
                    f"Expected str for schema 'name' in field '{self.name}', got {type(self.logical_name).__name__}"
                )
            norm_logical = self.logical_name.strip() or None
            if norm_logical != self.logical_name:
                object.__setattr__(self, "logical_name", norm_logical)
        if self.doc is not None and not isinstance(self.doc, str):
            raise ValueError(
                f"Expected str for schema 'doc' in field '{self.name}', got {type(self.doc).__name__}"
            )
        if not isinstance(self.parameters, Mapping):
            raise ValueError(
                f"Expected mapping for schema 'parameters' in field '{self.name}', got {type(self.parameters).__name__}"
            )
        sorted_params = {
            str(k): str(v)
            for k, v in sorted(self.parameters.items(), key=lambda kv: str(kv[0]))
        }
        object.__setattr__(self, "parameters", MappingProxyType(sorted_params))

    def __hash__(self) -> int:
        return hash(
            (
                self.name,
                self.type_name,
                self.optional,
                self.logical_name,
                self.doc,
                tuple(self.parameters.items()),
                self.fields,
                self.items,
                self.keys,
                self.values,
            )
        )

    @classmethod
    def from_json(
        cls,
        raw: Mapping[str, Any],
        *,
        field_name: str | None = None,
    ) -> FieldSpec:
        if not isinstance(raw, Mapping):
            raise ValueError(
                f"Expected dict in schema definition, got {type(raw).__name__}"
            )
        resolved_name = field_name if field_name is not None else raw.get("field")
        if (
            not resolved_name
            or not isinstance(resolved_name, str)
            or not resolved_name.strip()
        ):
            raise ValueError(f"Encountered field without valid name in schema: {raw}")

        raw_type = raw.get("type")
        if not isinstance(raw_type, str) or not raw_type.strip():
            raise ValueError(
                f"Missing or invalid 'type' in schema for field '{resolved_name}': {raw}"
            )
        type_str = raw_type.strip().lower()
        raw_params = raw.get("parameters")
        raw_fields = raw.get("fields")
        if raw_fields is not None and not isinstance(raw_fields, list):
            raise ValueError(
                f"Expected list for schema 'fields' in field '{resolved_name}', got {type(raw_fields).__name__}"
            )
        sub_fields = tuple(cls.from_json(f) for f in raw_fields) if raw_fields else ()

        raw_items = raw.get("items")
        items_spec = (
            cls.from_json(raw_items, field_name="item")
            if type_str == "array" and raw_items is not None
            else None
        )
        raw_keys = raw.get("keys")
        keys_spec = (
            cls.from_json(raw_keys, field_name="key")
            if type_str == "map" and raw_keys is not None
            else None
        )
        raw_values = raw.get("values")
        values_spec = (
            cls.from_json(raw_values, field_name="value")
            if type_str == "map" and raw_values is not None
            else None
        )

        return cls(
            name=resolved_name,
            type_name=type_str,
            optional=raw.get("optional", True),
            logical_name=raw.get("name"),
            doc=raw.get("doc"),
            parameters={} if raw_params is None else raw_params,
            fields=sub_fields,
            items=items_spec,
            keys=keys_spec,
            values=values_spec,
        )

    @classmethod
    def from_connect(
        cls,
        j_schema: Any,
        *,
        field_name: str = "root",
    ) -> FieldSpec:
        if j_schema is None:
            raise ValueError(
                f"Java Connect schema cannot be None for field '{field_name}'"
            )
        j_type = j_schema.type()
        if j_type is None:
            raise ValueError(
                f"Java Connect schema type() cannot be None for field '{field_name}'"
            )
        raw_type_name = j_type.name()
        type_str = (
            str(raw_type_name).strip().lower() if raw_type_name is not None else ""
        )
        if not type_str:
            raise ValueError(
                f"Java Connect schema type name cannot be empty for field '{field_name}'"
            )

        optional = bool(j_schema.isOptional())
        raw_doc = j_schema.doc()
        doc = str(raw_doc) if raw_doc else None
        raw_logical = j_schema.name()
        logical_name = str(raw_logical) if raw_logical else None
        raw_params = j_schema.parameters()
        parameters = (
            {str(k): str(v) for k, v in raw_params.items()}
            if raw_params is not None
            else {}
        )

        sub_fields: tuple[FieldSpec, ...] = ()
        items_spec: FieldSpec | None = None
        keys_spec: FieldSpec | None = None
        values_spec: FieldSpec | None = None

        if type_str == "struct":
            j_fields = j_schema.fields()
            if j_fields is not None:
                parsed_sub_fields: list[FieldSpec] = []
                for sub_f in j_fields:
                    raw_sub_name = sub_f.name() if sub_f is not None else None
                    if raw_sub_name is None or not str(raw_sub_name).strip():
                        raise ValueError(
                            f"Java Connect struct field name cannot be empty in '{field_name}'"
                        )
                    parsed_sub_fields.append(
                        cls.from_connect(sub_f.schema(), field_name=str(raw_sub_name))
                    )
                sub_fields = tuple(parsed_sub_fields)
        elif type_str == "array":
            val_schema = j_schema.valueSchema()
            if val_schema is not None:
                items_spec = cls.from_connect(val_schema, field_name="item")
        elif type_str == "map":
            key_schema = j_schema.keySchema()
            val_schema = j_schema.valueSchema()
            if key_schema is not None:
                keys_spec = cls.from_connect(key_schema, field_name="key")
            if val_schema is not None:
                values_spec = cls.from_connect(val_schema, field_name="value")

        return cls(
            name=field_name,
            type_name=type_str,
            optional=optional,
            logical_name=logical_name,
            doc=doc,
            parameters=parameters,
            fields=sub_fields,
            items=items_spec,
            keys=keys_spec,
            values=values_spec,
        )
