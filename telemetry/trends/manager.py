from __future__ import annotations

import time

from telemetry.models import TrendConfig, TrendSample, ValidTelemetryValue
from telemetry.storage.sqlite import SQLiteRepository
from telemetry.trends.ring_storage import RingTrendStorage
from telemetry.wal.trend_wal import TrendWal


class TrendManager:
    def __init__(
        self,
        repo: SQLiteRepository,
        wal: TrendWal,
        ring_storage: RingTrendStorage,
        trends: tuple[TrendConfig, ...],
        compact_every_records: int,
    ) -> None:
        self.repo = repo
        self.wal = wal
        self.ring_storage = ring_storage
        self.trends = trends
        self.compact_every_records = compact_every_records
        self._sequence = self._load_sequence()
        self._since_compact = 0

    def _load_sequence(self) -> int:
        max_seq = 0
        for trend in self.trends:
            last_sequence, _, _ = self.repo.trend_state(trend.trend_id)
            max_seq = max(max_seq, last_sequence)
        return max_seq

    def recover(self) -> int:
        applied = 0
        by_id = {trend.trend_id: trend for trend in self.trends}
        for sample in self.wal.iter_valid_records():
            trend = by_id.get(sample.trend_id)
            if trend is None:
                continue
            last_sequence, next_slot, count = self.repo.trend_state(sample.trend_id)
            if sample.sequence <= last_sequence:
                continue
            self.ring_storage.write_sample(sample.trend_id, trend.window_size, next_slot, sample)
            self.repo.update_trend_state(
                sample,
                next_slot=(next_slot + 1) % trend.window_size,
                count=min(count + 1, trend.window_size),
            )
            applied += 1
        self.wal.compact()
        return applied

    def tick(self) -> int:
        written = 0
        timestamp = time.time_ns()
        for trend in self.trends:
            value = self.repo.get_current_value(trend.source_id, trend.object_id)
            if value is None:
                continue
            self.append_value(trend, value, timestamp)
            written += 1
        if self._since_compact >= self.compact_every_records:
            self.wal.compact()
            self._since_compact = 0
        return written

    def append_value(self, trend: TrendConfig, value: ValidTelemetryValue, timestamp: int) -> TrendSample:
        self._sequence += 1
        sample = TrendSample(
            sequence=self._sequence,
            trend_id=trend.trend_id,
            timestamp=timestamp,
            scalar_type=value.scalar_type,
            value=value.value,
        )
        self.wal.append(sample)
        _, next_slot, count = self.repo.trend_state(trend.trend_id)
        self.ring_storage.write_sample(trend.trend_id, trend.window_size, next_slot, sample)
        self.repo.update_trend_state(
            sample,
            next_slot=(next_slot + 1) % trend.window_size,
            count=min(count + 1, trend.window_size),
        )
        self._since_compact += 1
        return sample

    def read_window(self, trend_id: str) -> list[TrendSample]:
        trend = next(item for item in self.trends if item.trend_id == trend_id)
        _, next_slot, count = self.repo.trend_state(trend_id)
        return self.ring_storage.read_window(trend_id, trend.window_size, next_slot, count)
