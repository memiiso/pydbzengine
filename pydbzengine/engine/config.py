from __future__ import annotations

from typing import Any, ClassVar

from pydbzengine.engine.base import EventFormat

__all__ = [
    "DebeziumConfig",
]


class DebeziumConfig:
    """
    Centralized configuration object for DebeziumEngine and change handlers.

    Accepts a Python dict[str, Any] and extracts engine and schema settings.
    """

    DEFAULT_HEARTBEAT_PREFIX: ClassVar[str] = "__debezium-heartbeat"

    def __init__(
        self,
        properties: dict[str, Any],
        format: str | EventFormat | None = None,
    ) -> None:
        if not isinstance(properties, dict):
            raise TypeError(
                f"Expected dict for properties, got {type(properties).__name__}"
            )
        self.properties: dict[str, Any] = properties
        self._format: EventFormat = self._resolve_format(self.properties, format)
        self._schemas_enabled: bool = self._resolve_schemas_enabled(self.properties)
        self._heartbeat_prefix: str = self._resolve_heartbeat_prefix(self.properties)

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

    @classmethod
    def _resolve_format(
        cls,
        properties: dict[str, Any],
        format: str | EventFormat | None,
    ) -> EventFormat:
        general_format: EventFormat | None = None
        for key in ("format", "engine.format", "converter.format", "converter.class"):
            if key in properties:
                general_format = EventFormat.resolve_from(properties[key])
                break

        value_format: EventFormat | None = None
        for key in (
            "value.format",
            "format.value",
            "value.converter",
            "value.converter.class",
        ):
            if key in properties:
                value_format = EventFormat.resolve_from(properties[key])
                break

        key_format: EventFormat | None = None
        for key in (
            "key.format",
            "format.key",
            "key.converter",
            "key.converter.class",
        ):
            if key in properties:
                key_format = EventFormat.resolve_from(properties[key])
                break

        effective_value = value_format if value_format is not None else general_format
        effective_key = key_format if key_format is not None else general_format

        if (
            effective_value is not None
            and effective_key is not None
            and effective_value is not effective_key
        ):
            raise ValueError(
                f"Having two different formats for event ({effective_value.value!r}) "
                f"and key ({effective_key.value!r}) is not supported."
            )
        if (
            general_format is not None
            and effective_value is not None
            and general_format is not effective_value
        ):
            raise ValueError(
                f"Conflicting format configuration in properties: engine format "
                f"{general_format.value!r} does not match converter format {effective_value.value!r}."
            )

        property_format = (
            effective_value if effective_value is not None else effective_key
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
    def _resolve_heartbeat_prefix(cls, properties: dict[str, Any]) -> str:
        raw_prefix = properties.get(
            "topic.heartbeat.prefix", cls.DEFAULT_HEARTBEAT_PREFIX
        )
        if not isinstance(raw_prefix, str) or not raw_prefix.strip():
            raise ValueError(
                f"topic.heartbeat.prefix must be a non-empty string, got {raw_prefix!r}"
            )
        return raw_prefix.strip()

    @staticmethod
    def _parse_bool_prop(properties: dict[str, Any], key: str) -> bool:
        val = properties[key]
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            norm = val.strip().lower()
            if norm in ("true", "false"):
                return norm == "true"
        raise ValueError(f"Invalid boolean value for {key}: {val!r}")

    def _resolve_schemas_enabled(self, properties: dict[str, Any]) -> bool:
        if self._format is EventFormat.CONNECT:
            return True
        general_enabled: bool | None = None
        for key in ("converter.schemas.enable", "schemas.enable"):
            if key in properties:
                general_enabled = self._parse_bool_prop(properties, key)
                break

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

        if effective_val is not None and effective_key is not None:
            if effective_val != effective_key:
                raise ValueError(
                    "Having two different schema configurations for event "
                    f"(schemas.enable={effective_val}) and key (schemas.enable={effective_key}) is not supported."
                )
            return effective_val
        if effective_val is not None:
            return effective_val
        if effective_key is not None:
            return effective_key
        return True
