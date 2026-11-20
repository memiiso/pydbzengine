from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, ClassVar

from pydbzengine.engine.base import EventFormat

__all__ = ["DebeziumConfig"]


def _parse_bool(value: Any, key: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        norm = value.strip().lower()
        if norm in ("true", "false"):
            return norm == "true"
    raise ValueError(f"Invalid boolean value for {key}: {value!r}")


def _require_non_empty_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string, got {value!r}")
    return value.strip()


def _first_present(properties: Mapping[str, Any], keys: tuple[str, ...]) -> Any | None:
    for key in keys:
        if key in properties:
            return properties[key]
    return None


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
        self._format = self._resolve_format(self.properties, format)
        self._schemas_enabled = self._resolve_schemas_enabled(
            self.properties, self._format, schemas_enabled
        )
        self._heartbeat_prefix = self._resolve_heartbeat_prefix(
            self.properties, heartbeat_prefix
        )

    @property
    def format(self) -> EventFormat:
        return self._format

    @property
    def schemas_enabled(self) -> bool:
        return self._schemas_enabled

    @property
    def heartbeat_prefix(self) -> str:
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
        general = (
            EventFormat.resolve_from(properties["converter.class"])
            if "converter.class" in properties
            else None
        )
        raw_val = _first_present(
            properties, ("value.converter", "value.converter.class")
        )
        val_fmt = EventFormat.resolve_from(raw_val) if raw_val is not None else None
        raw_key = _first_present(properties, ("key.converter", "key.converter.class"))
        key_fmt = EventFormat.resolve_from(raw_key) if raw_key is not None else None

        if val_fmt is not None and key_fmt is not None and val_fmt is not key_fmt:
            raise ValueError(
                f"Having two different formats for event ({val_fmt.value!r}) "
                f"and key ({key_fmt.value!r}) is not supported."
            )

        conv_fmt = val_fmt if val_fmt is not None else key_fmt
        if general is not None and conv_fmt is not None and general is not conv_fmt:
            raise ValueError(
                f"Conflicting format configuration in properties: engine format "
                f"{general.value!r} does not match converter format {conv_fmt.value!r}."
            )

        prop_fmt = conv_fmt if conv_fmt is not None else general
        if format is not None:
            explicit = EventFormat.resolve_from(format)
            if prop_fmt is not None and prop_fmt is not explicit:
                raise ValueError(
                    f"Conflicting format configuration: explicit format {explicit.value!r} "
                    f"does not match property format {prop_fmt.value!r}."
                )
            return explicit

        return prop_fmt if prop_fmt is not None else EventFormat.JSON

    @classmethod
    def _resolve_heartbeat_prefix(
        cls,
        properties: Mapping[str, Any],
        heartbeat_prefix: str | None,
    ) -> str:
        prop_prefix = (
            _require_non_empty_str(
                properties["topic.heartbeat.prefix"], "topic.heartbeat.prefix"
            )
            if "topic.heartbeat.prefix" in properties
            else None
        )
        if heartbeat_prefix is not None:
            explicit = _require_non_empty_str(heartbeat_prefix, "heartbeat_prefix")
            if prop_prefix is not None and prop_prefix != explicit:
                raise ValueError(
                    f"Conflicting heartbeat_prefix configuration: explicit {explicit!r} "
                    f"does not match topic.heartbeat.prefix {prop_prefix!r}."
                )
            return explicit
        return prop_prefix if prop_prefix is not None else cls.DEFAULT_HEARTBEAT_PREFIX

    @staticmethod
    def _resolve_schemas_enabled(
        properties: Mapping[str, Any],
        event_format: EventFormat,
        schemas_enabled: bool | None,
    ) -> bool:
        if schemas_enabled is not None and not isinstance(schemas_enabled, bool):
            raise TypeError(
                f"Expected bool for schemas_enabled, got {type(schemas_enabled).__name__}"
            )
        if event_format is EventFormat.CONNECT:
            if schemas_enabled is False:
                raise ValueError(
                    "EventFormat.CONNECT always carries schemas; schemas_enabled=False is not supported."
                )
            return True

        conv = (
            _parse_bool(
                properties["converter.schemas.enable"], "converter.schemas.enable"
            )
            if "converter.schemas.enable" in properties
            else None
        )
        top = (
            _parse_bool(properties["schemas.enable"], "schemas.enable")
            if "schemas.enable" in properties
            else None
        )
        if conv is not None and top is not None and conv != top:
            raise ValueError(
                f"Conflicting schema configuration in properties: converter.schemas.enable={conv} "
                f"does not match schemas.enable={top}."
            )
        general = conv if conv is not None else top

        val = (
            _parse_bool(
                properties["value.converter.schemas.enable"],
                "value.converter.schemas.enable",
            )
            if "value.converter.schemas.enable" in properties
            else general
        )
        key = (
            _parse_bool(
                properties["key.converter.schemas.enable"],
                "key.converter.schemas.enable",
            )
            if "key.converter.schemas.enable" in properties
            else general
        )
        if val is not None and key is not None and val != key:
            raise ValueError(
                "Having two different schema configurations for event "
                f"(schemas.enable={val}) and key (schemas.enable={key}) is not supported."
            )

        prop_enabled = val if val is not None else key
        if schemas_enabled is not None:
            if prop_enabled is not None and prop_enabled != schemas_enabled:
                raise ValueError(
                    f"Conflicting schema configuration: explicit schemas_enabled={schemas_enabled} "
                    f"does not match property schemas.enable={prop_enabled}."
                )
            return schemas_enabled

        return prop_enabled if prop_enabled is not None else True
