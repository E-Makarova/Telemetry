from __future__ import annotations

from telemetry.models import InvalidTelemetryValue, RawTelemetryValue, TelemetryObject, ValidTelemetryValue
from telemetry.scalar_types import ScalarTypeRegistry


class TelemetryValidator:
    def __init__(self, objects: dict[tuple[str, str], TelemetryObject], registry: ScalarTypeRegistry) -> None:
        self._objects = objects
        self._registry = registry

    def validate(self, raw: RawTelemetryValue) -> ValidTelemetryValue | InvalidTelemetryValue:
        key = (raw.source_id, raw.object_id)
        obj = self._objects.get(key)
        if obj is None:
            return InvalidTelemetryValue(raw=raw, reason="unknown source/object")
        try:
            value = self._registry.validate(obj.scalar_type, raw.value)
        except ValueError as exc:
            return InvalidTelemetryValue(raw=raw, reason=str(exc))
        return ValidTelemetryValue(
            source_id=raw.source_id,
            object_id=raw.object_id,
            scalar_type=obj.scalar_type,
            value=value,
            source_ts=raw.source_ts,
            received_ts=raw.received_ts,
        )

