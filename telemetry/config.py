from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from telemetry.models import TrendConfig


@dataclass(frozen=True, slots=True)
class SourceSettings:
    source_id: str
    name: str
    object_count: int
    min_changes_per_tick: int
    max_changes_per_tick: int
    error_rate: float = 0.0
    invalid_rate: float = 0.0
    max_delay_ms: int = 0


@dataclass(frozen=True, slots=True)
class SQLiteSettings:
    path: Path
    batch_size: int
    flush_interval_ms: int
    retention_days: int


@dataclass(frozen=True, slots=True)
class WalSettings:
    path: Path
    fsync_each_record: bool
    compact_every_records: int


@dataclass(frozen=True, slots=True)
class TrendSettings:
    directory: Path
    window_size: int
    interval_seconds: float
    trends: tuple[TrendConfig, ...]


@dataclass(frozen=True, slots=True)
class QueueSettings:
    input_maxsize: int
    valid_maxsize: int


@dataclass(frozen=True, slots=True)
class AppConfig:
    data_dir: Path
    log_dir: Path
    poll_interval_seconds: float
    sources: tuple[SourceSettings, ...]
    sqlite: SQLiteSettings
    wal: WalSettings
    trends: TrendSettings
    queues: QueueSettings


def _path(base: Path, value: str) -> Path:
    expanded = os.path.expandvars(value)
    p = Path(expanded).expanduser()
    return p if p.is_absolute() else base / p


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    base = config_path.parent.resolve()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    data_dir = _path(base, raw["data_dir"])
    log_dir = _path(base, raw["log_dir"])
    sqlite = SQLiteSettings(
        path=_path(base, raw["sqlite"]["path"]),
        batch_size=int(raw["sqlite"]["batch_size"]),
        flush_interval_ms=int(raw["sqlite"]["flush_interval_ms"]),
        retention_days=int(raw["sqlite"]["retention_days"]),
    )
    wal = WalSettings(
        path=_path(base, raw["wal"]["path"]),
        fsync_each_record=bool(raw["wal"]["fsync_each_record"]),
        compact_every_records=int(raw["wal"]["compact_every_records"]),
    )
    window_size = int(raw["trends"]["window_size"])
    trends = tuple(
        TrendConfig(
            trend_id=item["trend_id"],
            source_id=item["source_id"],
            object_id=item["object_id"],
            window_size=window_size,
        )
        for item in raw["trends"]["items"]
    )
    return AppConfig(
        data_dir=data_dir,
        log_dir=log_dir,
        poll_interval_seconds=float(raw["poll_interval_seconds"]),
        sources=tuple(SourceSettings(**item) for item in raw["sources"]),
        sqlite=sqlite,
        wal=wal,
        trends=TrendSettings(
            directory=_path(base, raw["trends"]["directory"]),
            window_size=window_size,
            interval_seconds=float(raw["trends"]["interval_seconds"]),
            trends=trends,
        ),
        queues=QueueSettings(**raw["queues"]),
    )
