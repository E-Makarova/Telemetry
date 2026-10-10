from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class TelemetryObject:
    source_id: str
    object_id: str
    name: str
    scalar_type: str


@dataclass(frozen=True, slots=True)
class SourceConfiguration:
    source_id: str
    name: str
    objects: tuple[TelemetryObject, ...]


@dataclass(frozen=True, slots=True)
class RawTelemetryValue:
    source_id: str
    object_id: str
    value: Any
    source_ts: int | None
    received_ts: int


@dataclass(frozen=True, slots=True)
class ValidTelemetryValue:
    source_id: str
    object_id: str
    scalar_type: str
    value: int | float | bool
    source_ts: int | None
    received_ts: int


@dataclass(frozen=True, slots=True)
class InvalidTelemetryValue:
    raw: RawTelemetryValue
    reason: str


@dataclass(frozen=True, slots=True)
class TrendConfig:
    trend_id: str
    source_id: str
    object_id: str
    window_size: int


@dataclass(frozen=True, slots=True)
class TrendSample:
    sequence: int
    trend_id: str
    timestamp: int
    scalar_type: str
    value: int | float | bool

