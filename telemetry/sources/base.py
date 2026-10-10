from __future__ import annotations

from abc import ABC, abstractmethod

from telemetry.models import RawTelemetryValue, SourceConfiguration


class TelemetrySource(ABC):
    @abstractmethod
    async def get_configuration(self) -> SourceConfiguration:
        raise NotImplementedError

    @abstractmethod
    async def poll_changes(self) -> list[RawTelemetryValue]:
        raise NotImplementedError

    @abstractmethod
    async def current_snapshot(self) -> list[RawTelemetryValue]:
        raise NotImplementedError
