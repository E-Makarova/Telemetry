from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import time
import tracemalloc
import uuid
from pathlib import Path

from telemetry.app import TelemetryApplication
from telemetry.config import SourceSettings, load_config
from telemetry.models import RawTelemetryValue, TrendConfig, TrendSample, ValidTelemetryValue
from telemetry.scalar_types import default_scalar_registry
from telemetry.sources.emulator import EmulatedTelemetrySource
from telemetry.storage.sqlite import SQLiteRepository
from telemetry.trends.manager import TrendManager
from telemetry.trends.ring_storage import RingTrendStorage
from telemetry.validation.validator import TelemetryValidator
from telemetry.wal.trend_wal import TrendWal


def workspace_tmp(name: str) -> Path:
    path = Path(tempfile.gettempdir()) / "telemetry_tests" / f"{name}_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def make_config(tmp_path: Path, *, window_size: int = 5, max_changes: int = 600) -> Path:
    cfg = {
        "data_dir": str(tmp_path / "data"),
        "log_dir": str(tmp_path / "logs"),
        "poll_interval_seconds": 0.05,
        "queues": {"input_maxsize": 5000, "valid_maxsize": 5000},
        "sqlite": {
            "path": str(tmp_path / "data" / "telemetry.sqlite3"),
            "batch_size": 200,
            "flush_interval_ms": 25,
            "retention_days": 30,
        },
        "wal": {
            "path": str(tmp_path / "data" / "trends" / "trend.wal"),
            "fsync_each_record": True,
            "compact_every_records": 10000,
        },
        "trends": {
            "directory": str(tmp_path / "data" / "trends"),
            "window_size": window_size,
            "interval_seconds": 0.05,
            "items": [
                {"trend_id": f"trend_{i:03d}", "source_id": f"source_{((i - 1) // 5) + 1}", "object_id": f"obj_{((i - 1) % 5) + 1:04d}"}
                for i in range(1, 21)
            ],
        },
        "sources": [
            {
                "source_id": f"source_{i}",
                "name": f"Source {i}",
                "object_count": 1000,
                "min_changes_per_tick": 100,
                "max_changes_per_tick": max_changes,
                "error_rate": 0.0,
                "invalid_rate": 0.0,
                "max_delay_ms": 0,
            }
            for i in range(1, 5)
        ],
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    return path


def test_configuration_and_sources_have_4000_objects() -> None:
    tmp_path = workspace_tmp("configuration")
    config = load_config(make_config(tmp_path))
    sources = [EmulatedTelemetrySource(item, seed=i) for i, item in enumerate(config.sources)]
    configs = asyncio.run(_get_configs(sources))
    assert len(configs) == 4
    assert all(len(item.objects) == 1000 for item in configs)
    assert sum(len(item.objects) for item in configs) == 4000
    assert {obj.scalar_type for cfg in configs for obj in cfg.objects} == {"integer", "float", "boolean"}


async def _get_configs(sources: list[EmulatedTelemetrySource]):
    return [await source.get_configuration() for source in sources]


def test_source_generates_only_changed_values_within_limits() -> None:
    settings = SourceSettings("source_1", "Source 1", 1000, 100, 600)
    source = EmulatedTelemetrySource(settings, seed=1)
    values = asyncio.run(source.poll_changes())
    assert 100 <= len(values) <= 600
    assert len({item.object_id for item in values}) == len(values)


def test_scalar_validation_accepts_and_rejects_types() -> None:
    source = EmulatedTelemetrySource(SourceSettings("source_1", "Source 1", 3, 1, 1), seed=1)
    config = asyncio.run(source.get_configuration())
    objects = {(obj.source_id, obj.object_id): obj for obj in config.objects}
    validator = TelemetryValidator(objects, default_scalar_registry())
    by_type = {obj.scalar_type: obj for obj in config.objects}
    now = time.time_ns()
    int_obj = by_type["integer"]
    float_obj = by_type["float"]
    bool_obj = by_type["boolean"]
    assert isinstance(validator.validate(RawTelemetryValue(int_obj.source_id, int_obj.object_id, 10, now, now)), ValidTelemetryValue)
    assert isinstance(validator.validate(RawTelemetryValue(float_obj.source_id, float_obj.object_id, 1.5, now, now)), ValidTelemetryValue)
    assert isinstance(validator.validate(RawTelemetryValue(bool_obj.source_id, bool_obj.object_id, True, now, now)), ValidTelemetryValue)
    invalid = validator.validate(RawTelemetryValue(int_obj.source_id, int_obj.object_id, "bad", now, now))
    assert not isinstance(invalid, ValidTelemetryValue)


def test_valid_saved_invalid_logged_and_not_saved() -> None:
    tmp_path = workspace_tmp("valid_invalid")
    config = load_config(make_config(tmp_path))
    app = TelemetryApplication(config)
    configs = asyncio.run(app.initialize())
    obj = configs[0].objects[0]
    assert app.validator is not None
    now = time.time_ns()
    valid = app.validator.validate(RawTelemetryValue(obj.source_id, obj.object_id, 10, now, now))
    invalid = app.validator.validate(RawTelemetryValue(obj.source_id, obj.object_id, "bad", now, now))
    assert isinstance(valid, ValidTelemetryValue)
    app.repo.insert_valid_batch([valid])
    assert not isinstance(invalid, ValidTelemetryValue)
    app.repo.insert_invalid(invalid)
    assert app.repo.telemetry_count() == 1
    assert app.repo.invalid_count() == 1
    app.repo.close()


def test_trends_are_created_and_written_without_change() -> None:
    tmp_path = workspace_tmp("trends_no_change")
    config = load_config(make_config(tmp_path, window_size=5))
    app = TelemetryApplication(config)
    configs = asyncio.run(app.initialize())
    now = time.time_ns()
    initial_values = []
    for trend in config.trends.trends:
        obj = next(o for cfg in configs for o in cfg.objects if o.source_id == trend.source_id and o.object_id == trend.object_id)
        value = 1 if obj.scalar_type == "integer" else 1.0 if obj.scalar_type == "float" else True
        initial_values.append(ValidTelemetryValue(obj.source_id, obj.object_id, obj.scalar_type, value, now, now))
    app.repo.insert_valid_batch(initial_values)
    assert app.trend_manager is not None
    assert app.trend_manager.tick() == 20
    assert app.trend_manager.tick() == 20
    assert len(app.trend_manager.read_window("trend_001")) == 2
    app.repo.close()


def test_sliding_window_evicts_oldest_value() -> None:
    tmp_path = workspace_tmp("sliding")
    config = load_config(make_config(tmp_path, window_size=3))
    app = TelemetryApplication(config)
    configs = asyncio.run(app.initialize())
    trend = config.trends.trends[0]
    obj = next(o for cfg in configs for o in cfg.objects if o.source_id == trend.source_id and o.object_id == trend.object_id)
    assert app.trend_manager is not None
    for i in range(5):
        now = time.time_ns()
        app.repo.insert_valid_batch([ValidTelemetryValue(obj.source_id, obj.object_id, obj.scalar_type, i, now, now)])
        app.trend_manager.tick()
    samples = app.trend_manager.read_window(trend.trend_id)
    assert len(samples) == 3
    assert [sample.value for sample in samples] == [2, 3, 4]
    app.repo.close()


def test_recovery_from_wal_and_corrupted_tail() -> None:
    tmp_path = workspace_tmp("recovery")
    db = tmp_path / "telemetry.sqlite3"
    repo = SQLiteRepository(db)
    repo.initialize()
    source = EmulatedTelemetrySource(SourceSettings("source_1", "Source 1", 1000, 1, 1), seed=1)
    cfg = asyncio.run(source.get_configuration())
    repo.upsert_source_configurations([cfg])
    trend = TrendConfig("trend_001", "source_1", "obj_0001", 5)
    trend_dir = tmp_path / "trends"
    repo.upsert_trends((trend,), trend_dir)
    wal = TrendWal(trend_dir / "trend.wal", fsync_each_record=True)
    wal.append(TrendSample(1, "trend_001", time.time_ns(), "integer", 10))
    with (trend_dir / "trend.wal").open("ab") as fh:
        fh.write(b"broken")
    manager = TrendManager(repo, wal, RingTrendStorage(trend_dir), (trend,), compact_every_records=1000)
    assert manager.recover() == 1
    assert [sample.value for sample in manager.read_window("trend_001")] == [10]
    repo.close()


def test_max_load_smoke_and_memory_is_bounded() -> None:
    tmp_path = workspace_tmp("load")
    tracemalloc.start()
    config = load_config(make_config(tmp_path, window_size=10, max_changes=600))
    app = TelemetryApplication(config)
    asyncio.run(app.run_for(0.4))
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    with sqlite3.connect(config.sqlite.path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM telemetry_values").fetchone()[0]
    assert count > 0
    assert peak < 50 * 1024 * 1024


def test_sqlite_schema_has_required_tables() -> None:
    tmp_path = workspace_tmp("schema")
    config = load_config(make_config(tmp_path))
    repo = SQLiteRepository(config.sqlite.path)
    repo.initialize()
    tables = {
        row[0]
        for row in repo.conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    assert {"sources", "telemetry_objects", "telemetry_values", "current_values", "trends", "trend_state", "invalid_values"} <= tables
    assert repo.conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    repo.close()


def test_config_contains_20_fixed_trends() -> None:
    tmp_path = workspace_tmp("config_trends")
    config = load_config(make_config(tmp_path))
    assert len(config.trends.trends) == 20
    assert len({trend.trend_id for trend in config.trends.trends}) == 20
