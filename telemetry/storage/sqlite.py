from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from telemetry.models import InvalidTelemetryValue, SourceConfiguration, TrendConfig, TrendSample, ValidTelemetryValue


class SQLiteRepository:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.execute("PRAGMA busy_timeout = 5000")

    def close(self) -> None:
        self.conn.close()

    def initialize(self) -> None:
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def upsert_source_configurations(self, configs: list[SourceConfiguration]) -> None:
        now = time.time_ns()
        with self.conn:
            for cfg in configs:
                self.conn.execute(
                    "INSERT OR REPLACE INTO sources(source_id, name, created_at) VALUES (?, ?, ?)",
                    (cfg.source_id, cfg.name, now),
                )
                self.conn.executemany(
                    """
                    INSERT OR REPLACE INTO telemetry_objects
                    (source_id, object_id, name, scalar_type, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (obj.source_id, obj.object_id, obj.name, obj.scalar_type, now)
                        for obj in cfg.objects
                    ],
                )

    def upsert_trends(self, trends: tuple[TrendConfig, ...], directory: Path) -> None:
        now = time.time_ns()
        with self.conn:
            for trend in trends:
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO trends
                    (trend_id, source_id, object_id, window_size, storage_path, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trend.trend_id,
                        trend.source_id,
                        trend.object_id,
                        trend.window_size,
                        str(directory / f"{trend.trend_id}.ring"),
                        now,
                    ),
                )
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO trend_state
                    (trend_id, last_sequence, next_slot, count, updated_ts)
                    VALUES (?, 0, 0, 0, ?)
                    """,
                    (trend.trend_id, now),
                )

    def insert_valid_batch(self, values: list[ValidTelemetryValue]) -> None:
        if not values:
            return
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO telemetry_values
                (source_id, object_id, source_ts, received_ts, scalar_type, int_value, float_value, bool_value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [self._value_row(item) for item in values],
            )
            self.conn.executemany(
                """
                INSERT INTO current_values
                (source_id, object_id, updated_ts, scalar_type, int_value, float_value, bool_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id, object_id) DO UPDATE SET
                    updated_ts = excluded.updated_ts,
                    scalar_type = excluded.scalar_type,
                    int_value = excluded.int_value,
                    float_value = excluded.float_value,
                    bool_value = excluded.bool_value
                """,
                [self._current_row(item) for item in values],
            )

    def upsert_current_batch(self, values: list[ValidTelemetryValue]) -> None:
        if not values:
            return
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO current_values
                (source_id, object_id, updated_ts, scalar_type, int_value, float_value, bool_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id, object_id) DO UPDATE SET
                    updated_ts = excluded.updated_ts,
                    scalar_type = excluded.scalar_type,
                    int_value = excluded.int_value,
                    float_value = excluded.float_value,
                    bool_value = excluded.bool_value
                """,
                [self._current_row(item) for item in values],
            )

    def insert_invalid(self, invalid: InvalidTelemetryValue) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO invalid_values(source_id, object_id, received_ts, raw_value, reason)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    invalid.raw.source_id,
                    invalid.raw.object_id,
                    invalid.raw.received_ts,
                    json.dumps(invalid.raw.value),
                    invalid.reason,
                ),
            )

    def get_current_value(self, source_id: str, object_id: str) -> ValidTelemetryValue | None:
        row = self.conn.execute(
            """
            SELECT source_id, object_id, updated_ts, scalar_type, int_value, float_value, bool_value
            FROM current_values
            WHERE source_id = ? AND object_id = ?
            """,
            (source_id, object_id),
        ).fetchone()
        if row is None:
            return None
        return ValidTelemetryValue(
            source_id=row["source_id"],
            object_id=row["object_id"],
            scalar_type=row["scalar_type"],
            value=self._extract_value(row["scalar_type"], row),
            source_ts=None,
            received_ts=row["updated_ts"],
        )

    def trend_state(self, trend_id: str) -> tuple[int, int, int]:
        row = self.conn.execute(
            "SELECT last_sequence, next_slot, count FROM trend_state WHERE trend_id = ?",
            (trend_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown trend: {trend_id}")
        return int(row["last_sequence"]), int(row["next_slot"]), int(row["count"])

    def update_trend_state(self, sample: TrendSample, next_slot: int, count: int) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE trend_state
                SET last_sequence = ?, next_slot = ?, count = ?, updated_ts = ?
                WHERE trend_id = ? AND last_sequence < ?
                """,
                (sample.sequence, next_slot, count, time.time_ns(), sample.trend_id, sample.sequence),
            )

    def telemetry_count(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM telemetry_values").fetchone()[0])

    def invalid_count(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM invalid_values").fetchone()[0])

    def checkpoint(self) -> None:
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    @staticmethod
    def _value_row(item: ValidTelemetryValue) -> tuple:
        return (
            item.source_id,
            item.object_id,
            item.source_ts,
            item.received_ts,
            item.scalar_type,
            item.value if item.scalar_type == "integer" else None,
            item.value if item.scalar_type == "float" else None,
            int(item.value) if item.scalar_type == "boolean" else None,
        )

    @staticmethod
    def _current_row(item: ValidTelemetryValue) -> tuple:
        return SQLiteRepository._value_row(item)[:2] + (
            item.received_ts,
            item.scalar_type,
            item.value if item.scalar_type == "integer" else None,
            item.value if item.scalar_type == "float" else None,
            int(item.value) if item.scalar_type == "boolean" else None,
        )

    @staticmethod
    def _extract_value(scalar_type: str, row: sqlite3.Row) -> int | float | bool:
        if scalar_type == "integer":
            return int(row["int_value"])
        if scalar_type == "float":
            return float(row["float_value"])
        if scalar_type == "boolean":
            return bool(row["bool_value"])
        raise ValueError(f"unknown scalar type: {scalar_type}")


SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS telemetry_objects (
    source_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    name TEXT NOT NULL,
    scalar_type TEXT NOT NULL CHECK (scalar_type IN ('integer', 'float', 'boolean')),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (source_id, object_id),
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE IF NOT EXISTS telemetry_values (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    source_ts INTEGER,
    received_ts INTEGER NOT NULL,
    scalar_type TEXT NOT NULL CHECK (scalar_type IN ('integer', 'float', 'boolean')),
    int_value INTEGER,
    float_value REAL,
    bool_value INTEGER CHECK (bool_value IN (0, 1) OR bool_value IS NULL),
    FOREIGN KEY (source_id, object_id) REFERENCES telemetry_objects(source_id, object_id)
);

CREATE INDEX IF NOT EXISTS idx_telemetry_values_object_time
ON telemetry_values(source_id, object_id, received_ts);

CREATE INDEX IF NOT EXISTS idx_telemetry_values_time
ON telemetry_values(received_ts);

CREATE TABLE IF NOT EXISTS current_values (
    source_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    updated_ts INTEGER NOT NULL,
    scalar_type TEXT NOT NULL CHECK (scalar_type IN ('integer', 'float', 'boolean')),
    int_value INTEGER,
    float_value REAL,
    bool_value INTEGER CHECK (bool_value IN (0, 1) OR bool_value IS NULL),
    PRIMARY KEY (source_id, object_id),
    FOREIGN KEY (source_id, object_id) REFERENCES telemetry_objects(source_id, object_id)
);

CREATE TABLE IF NOT EXISTS trends (
    trend_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    window_size INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (source_id, object_id) REFERENCES telemetry_objects(source_id, object_id)
);

CREATE TABLE IF NOT EXISTS trend_state (
    trend_id TEXT PRIMARY KEY,
    last_sequence INTEGER NOT NULL,
    next_slot INTEGER NOT NULL,
    count INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    FOREIGN KEY (trend_id) REFERENCES trends(trend_id)
);

CREATE TABLE IF NOT EXISTS invalid_values (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT,
    object_id TEXT,
    received_ts INTEGER NOT NULL,
    raw_value TEXT NOT NULL,
    reason TEXT NOT NULL
);
"""
