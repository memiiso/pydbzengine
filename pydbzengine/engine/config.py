from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, ClassVar

from pydbzengine.engine.base import EventFormat

__all__ = [
    "DebeziumConfig",
]


class DebeziumConfig:
    """
    Immutable configuration value object for DebeziumEngine and change handlers.

    Normalizes mapping or Java Properties inputs and resolves wire format,
    schema envelope mode, and heartbeat routing settings deterministically.
    """

    DEFAULT_HEARTBEAT_PREFIX: ClassVar[str] = "__debezium-heartbeat"

    def __init__(
        self,
        properties: Mapping[str, Any] | None = None,
        format: str | EventFormat | None = None,
        *,
        schemas_enabled: bool | None = None,
        heartbeat_prefix: str | None = None,
    ) -> None:
        if properties is None:
            normalized: dict[str, Any] = {}
        elif isinstance(properties, Mapping):
            normalized = {
                str(k): (str(v) if type(v).__name__ == "java.lang.String" else v)
                for k, v in properties.items()
            }
        else:
            raise TypeError(
                f"Expected Mapping for properties, got {type(properties).__name__}"
            )
        self.properties: Mapping[str, Any] = MappingProxyType(normalized)
        self._format: EventFormat = self._resolve_format(self.properties, format)
        self._schemas_enabled: bool = self._resolve_schemas_enabled(
            self.properties, schemas_enabled
        )
        self._heartbeat_prefix: str = self._resolve_heartbeat_prefix(
            self.properties, heartbeat_prefix
        )

    @property
    def format(self) -> EventFormat:
        """Returns the resolved wire serialization format (defaults to EventFormat.JSON)."""
        return self._format

    @property
    def schemas_enabled(self) -> bool:
        """Returns whether event schemas are enabled."""
        return self._schemas_enabled

    @property
    def heartbeat_prefix(self) -> str:
        """Returns the configured heartbeat topic prefix."""
        return self._heartbeat_prefix

    def to_engine_properties(self) -> dict[str, str]:
        """Returns normalized string properties for the Java Debezium embedded engine."""
        engine_props: dict[str, str] = {
            k: ("true" if v is True else "false" if v is False else str(v))
            for k, v in self.properties.items()
        }
        if self._format is EventFormat.JSON:
            engine_props["converter.schemas.enable"] = (
                "true" if self._schemas_enabled else "false"
            )
        if (
            "topic.heartbeat.prefix" in self.properties
            or self._heartbeat_prefix != self.DEFAULT_HEARTBEAT_PREFIX
        ):
            engine_props["topic.heartbeat.prefix"] = self._heartbeat_prefix
        return engine_props

    @classmethod
    def _resolve_format(
        cls,
        properties: Mapping[str, Any],
        format: str | EventFormat | None,
    ) -> EventFormat:
        general_format = (
            EventFormat.resolve_from(properties["converter.class"])
            if "converter.class" in properties
            else None
        )

        value_format: EventFormat | None = None
        for key in ("value.converter", "value.converter.class"):
            if key in properties:
                value_format = EventFormat.resolve_from(properties[key])
                break

        key_format: EventFormat | None = None
        for key in ("key.converter", "key.converter.class"):
            if key in properties:
                key_format = EventFormat.resolve_from(properties[key])
                break

        if (
            value_format is not None
            and key_format is not None
            and value_format is not key_format
        ):
            raise ValueError(
                f"Having two different formats for event ({value_format.value!r}) "
                f"and key ({key_format.value!r}) is not supported."
            )

        converter_format = value_format if value_format is not None else key_format
        if (
            general_format is not None
            and converter_format is not None
            and general_format is not converter_format
        ):
            raise ValueError(
                f"Conflicting format configuration in properties: engine format "
                f"{general_format.value!r} does not match converter format {converter_format.value!r}."
            )

        property_format = (
            converter_format if converter_format is not None else general_format
        )
        if format is not None:
            resolved_explicit = EventFormat.resolve_from(format)
            if property_format is not None and property_format is not resolved_explicit:
                raise ValueError(
                    f"Conflicting format configuration: explicit format {resolved_explicit.value!r} "
                    f"does not match property format {property_format.value!r}."
                )
            return resolved_explicit

        return property_format if property_format is not None else EventFormat.JSON

    @classmethod
    def _resolve_heartbeat_prefix(
        cls,
        properties: Mapping[str, Any],
        heartbeat_prefix: str | None,
    ) -> str:
        prop_prefix: str | None = None
        if "topic.heartbeat.prefix" in properties:
            raw_prefix = properties["topic.heartbeat.prefix"]
            if not isinstance(raw_prefix, str) or not raw_prefix.strip():
                raise ValueError(
                    f"topic.heartbeat.prefix must be a non-empty string, got {raw_prefix!r}"
                )
            prop_prefix = raw_prefix.strip()

        if heartbeat_prefix is not None:
            if not isinstance(heartbeat_prefix, str) or not heartbeat_prefix.strip():
                raise ValueError(
                    f"heartbeat_prefix must be a non-empty string, got {heartbeat_prefix!r}"
                )
            explicit_prefix = heartbeat_prefix.strip()
            if prop_prefix is not None and prop_prefix != explicit_prefix:
                raise ValueError(
                    f"Conflicting heartbeat_prefix configuration: explicit {explicit_prefix!r} "
                    f"does not match topic.heartbeat.prefix {prop_prefix!r}."
                )
            return explicit_prefix

        return prop_prefix if prop_prefix is not None else cls.DEFAULT_HEARTBEAT_PREFIX

    @staticmethod
    def _parse_bool_prop(properties: Mapping[str, Any], key: str) -> bool:
        val = properties[key]
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            norm = val.strip().lower()
            if norm in ("true", "false"):
                return norm == "true"
        raise ValueError(f"Invalid boolean value for {key}: {val!r}")

    def _resolve_schemas_enabled(
        self,
        properties: Mapping[str, Any],
        schemas_enabled: bool | None,
    ) -> bool:
        if schemas_enabled is not None and not isinstance(schemas_enabled, bool):
            raise TypeError(
                f"Expected bool for schemas_enabled, got {type(schemas_enabled).__name__}"
            )
        if self._format is EventFormat.CONNECT:
            if schemas_enabled is False:
                raise ValueError(
                    "EventFormat.CONNECT always carries schemas; schemas_enabled=False is not supported."
                )
            return True

        conv_enabled = (
            self._parse_bool_prop(properties, "converter.schemas.enable")
            if "converter.schemas.enable" in properties
            else None
        )
        top_enabled = (
            self._parse_bool_prop(properties, "schemas.enable")
            if "schemas.enable" in properties
            else None
        )
        if (
            conv_enabled is not None
            and top_enabled is not None
            and conv_enabled != top_enabled
        ):
            raise ValueError(
                f"Conflicting schema configuration in properties: converter.schemas.enable={conv_enabled} "
                f"does not match schemas.enable={top_enabled}."
            )
        general_enabled = conv_enabled if conv_enabled is not None else top_enabled

        val_enabled = (
            self._parse_bool_prop(properties, "value.converter.schemas.enable")
            if "value.converter.schemas.enable" in properties
            else None
        )
        key_enabled = (
            self._parse_bool_prop(properties, "key.converter.schemas.enable")
            if "key.converter.schemas.enable" in properties
            else None
        )

        effective_val = val_enabled if val_enabled is not None else general_enabled
        effective_key = key_enabled if key_enabled is not None else general_enabled

        prop_enabled: bool | None
        if effective_val is not None and effective_key is not None:
            if effective_val != effective_key:
                raise ValueError(
                    "Having two different schema configurations for event "
                    f"(schemas.enable={effective_val}) and key (schemas.enable={effective_key}) is not supported."
                )
            prop_enabled = effective_val
        elif effective_val is not None:
            prop_enabled = effective_val
        elif effective_key is not None:
            prop_enabled = effective_key
        else:
            prop_enabled = None

        if schemas_enabled is not None:
            if prop_enabled is not None and prop_enabled != schemas_enabled:
                raise ValueError(
                    f"Conflicting schema configuration: explicit schemas_enabled={schemas_enabled} "
                    f"does not match property schemas.enable={prop_enabled}."
                )
            return schemas_enabled

        return prop_enabled if prop_enabled is not None else True
