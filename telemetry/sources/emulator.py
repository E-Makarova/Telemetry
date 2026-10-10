from __future__ import annotations

import asyncio
import random
import time
from typing import Any

from telemetry.config import SourceSettings
from telemetry.models import RawTelemetryValue, SourceConfiguration, TelemetryObject
from telemetry.sources.base import TelemetrySource


class EmulatedTelemetrySource(TelemetrySource):
    def __init__(self, settings: SourceSettings, seed: int | None = None) -> None:
        self.settings = settings
        self._random = random.Random(seed if seed is not None else settings.source_id)
        self._objects = self._build_objects()
        self._values: dict[str, Any] = {
            item.object_id: self._initial_value(item.scalar_type) for item in self._objects
        }

    def _build_objects(self) -> tuple[TelemetryObject, ...]:
        types = ("integer", "float", "boolean")
        return tuple(
            TelemetryObject(
                source_id=self.settings.source_id,
                object_id=f"obj_{i:04d}",
                name=f"{self.settings.source_id}.object.{i:04d}",
                scalar_type=types[i % len(types)],
            )
            for i in range(1, self.settings.object_count + 1)
        )

    def _initial_value(self, scalar_type: str) -> int | float | bool:
        if scalar_type == "integer":
            return self._random.randint(-1000, 1000)
        if scalar_type == "float":
            return self._random.uniform(-1000.0, 1000.0)
        if scalar_type == "boolean":
            return bool(self._random.getrandbits(1))
        raise ValueError(f"unsupported scalar type: {scalar_type}")

    def _next_value(self, scalar_type: str, current: Any) -> int | float | bool:
        if scalar_type == "integer":
            return int(current) + self._random.randint(-10, 10)
        if scalar_type == "float":
            return float(current) + self._random.uniform(-10.0, 10.0)
        if scalar_type == "boolean":
            return not bool(current)
        raise ValueError(f"unsupported scalar type: {scalar_type}")

    def _invalid_value(self) -> str:
        return "invalid-value"

    async def get_configuration(self) -> SourceConfiguration:
        return SourceConfiguration(
            source_id=self.settings.source_id,
            name=self.settings.name,
            objects=self._objects,
        )

    async def poll_changes(self) -> list[RawTelemetryValue]:
        if self.settings.max_delay_ms:
            await asyncio.sleep(self._random.uniform(0, self.settings.max_delay_ms / 1000))
        if self._random.random() < self.settings.error_rate:
            raise RuntimeError(f"source unavailable: {self.settings.source_id}")

        count = self._random.randint(
            self.settings.min_changes_per_tick,
            self.settings.max_changes_per_tick,
        )
        count = min(count, len(self._objects))
        changed = self._random.sample(self._objects, count)
        now = time.time_ns()
        result: list[RawTelemetryValue] = []
        for item in changed:
            if self._random.random() < self.settings.invalid_rate:
                value: Any = self._invalid_value()
            else:
                value = self._next_value(item.scalar_type, self._values[item.object_id])
                self._values[item.object_id] = value
            result.append(
                RawTelemetryValue(
                    source_id=item.source_id,
                    object_id=item.object_id,
                    value=value,
                    source_ts=now,
                    received_ts=time.time_ns(),
                )
            )
        return result

    async def current_snapshot(self) -> list[RawTelemetryValue]:
        now = time.time_ns()
        return [
            RawTelemetryValue(
                source_id=item.source_id,
                object_id=item.object_id,
                value=self._values[item.object_id],
                source_ts=now,
                received_ts=now,
            )
            for item in self._objects
        ]
