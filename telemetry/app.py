from __future__ import annotations

import argparse
import asyncio
import logging
import time
from contextlib import suppress
from pathlib import Path

from telemetry.config import AppConfig, load_config
from telemetry.logging_config import configure_logging
from telemetry.models import InvalidTelemetryValue, RawTelemetryValue, SourceConfiguration, TelemetryObject, ValidTelemetryValue
from telemetry.scalar_types import default_scalar_registry
from telemetry.sources.emulator import EmulatedTelemetrySource
from telemetry.storage.sqlite import SQLiteRepository
from telemetry.trends.manager import TrendManager
from telemetry.trends.ring_storage import RingTrendStorage
from telemetry.validation.validator import TelemetryValidator
from telemetry.wal.trend_wal import TrendWal

LOGGER = logging.getLogger(__name__)


class TelemetryApplication:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.sources = [
            EmulatedTelemetrySource(settings, seed=i + 1)
            for i, settings in enumerate(config.sources)
        ]
        self.repo = SQLiteRepository(config.sqlite.path)
        self.input_queue: asyncio.Queue[RawTelemetryValue | None] = asyncio.Queue(
            maxsize=config.queues.input_maxsize
        )
        self.valid_queue: asyncio.Queue[ValidTelemetryValue | None] = asyncio.Queue(
            maxsize=config.queues.valid_maxsize
        )
        self.validator: TelemetryValidator | None = None
        self.trend_manager: TrendManager | None = None

    async def initialize(self) -> list[SourceConfiguration]:
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        self.config.trends.directory.mkdir(parents=True, exist_ok=True)
        self.repo.initialize()
        configs = [await source.get_configuration() for source in self.sources]
        self.repo.upsert_source_configurations(configs)
        self.repo.upsert_trends(self.config.trends.trends, self.config.trends.directory)
        objects: dict[tuple[str, str], TelemetryObject] = {}
        for cfg in configs:
            for obj in cfg.objects:
                objects[(obj.source_id, obj.object_id)] = obj
        self.validator = TelemetryValidator(objects, default_scalar_registry())
        snapshot_values: list[ValidTelemetryValue] = []
        for source in self.sources:
            for raw in await source.current_snapshot():
                result = self.validator.validate(raw)
                if isinstance(result, ValidTelemetryValue):
                    snapshot_values.append(result)
                else:
                    self.repo.insert_invalid(result)
        self.repo.upsert_current_batch(snapshot_values)
        self.trend_manager = TrendManager(
            repo=self.repo,
            wal=TrendWal(self.config.wal.path, self.config.wal.fsync_each_record),
            ring_storage=RingTrendStorage(self.config.trends.directory),
            trends=self.config.trends.trends,
            compact_every_records=self.config.wal.compact_every_records,
        )
        recovered = self.trend_manager.recover()
        LOGGER.info(
            "application initialized sources=%s objects=%s snapshot_values=%s recovered_trend_records=%s",
            len(configs),
            len(objects),
            len(snapshot_values),
            recovered,
        )
        return configs

    async def run_for(self, seconds: float) -> None:
        await self.initialize()
        stop_at = time.monotonic() + seconds
        tasks = [
            asyncio.create_task(self._source_loop(source, stop_at))
            for source in self.sources
        ]
        tasks.extend(
            [
                asyncio.create_task(self._validation_loop(stop_at)),
                asyncio.create_task(self._db_writer_loop(stop_at)),
                asyncio.create_task(self._trend_loop(stop_at)),
                asyncio.create_task(self._stats_loop(stop_at)),
            ]
        )
        try:
            await asyncio.gather(*tasks)
        finally:
            await self.input_queue.put(None)
            await self.valid_queue.put(None)
            for task in tasks:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            self.repo.checkpoint()
            self.repo.close()

    async def _source_loop(self, source: EmulatedTelemetrySource, stop_at: float) -> None:
        while time.monotonic() < stop_at:
            started = time.monotonic()
            try:
                for item in await source.poll_changes():
                    await self.input_queue.put(item)
            except Exception:
                LOGGER.exception("source polling failed")
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, self.config.poll_interval_seconds - elapsed))

    async def _validation_loop(self, stop_at: float) -> None:
        assert self.validator is not None
        while time.monotonic() < stop_at or not self.input_queue.empty():
            try:
                raw = await asyncio.wait_for(self.input_queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            if raw is None:
                break
            result = self.validator.validate(raw)
            if isinstance(result, InvalidTelemetryValue):
                LOGGER.warning(
                    "invalid value source=%s object=%s reason=%s",
                    raw.source_id,
                    raw.object_id,
                    result.reason,
                )
                self.repo.insert_invalid(result)
            else:
                await self.valid_queue.put(result)

    async def _db_writer_loop(self, stop_at: float) -> None:
        batch: list[ValidTelemetryValue] = []
        flush_interval = self.config.sqlite.flush_interval_ms / 1000
        last_flush = time.monotonic()
        while time.monotonic() < stop_at or batch or not self.valid_queue.empty():
            timeout = max(0.01, flush_interval - (time.monotonic() - last_flush))
            try:
                item = await asyncio.wait_for(self.valid_queue.get(), timeout=timeout)
            except TimeoutError:
                item = None
            if item is not None:
                batch.append(item)
            should_flush = (
                len(batch) >= self.config.sqlite.batch_size
                or (batch and time.monotonic() - last_flush >= flush_interval)
                or (item is None and batch)
            )
            if should_flush:
                self.repo.insert_valid_batch(batch)
                batch.clear()
                last_flush = time.monotonic()

    async def _trend_loop(self, stop_at: float) -> None:
        assert self.trend_manager is not None
        while time.monotonic() < stop_at:
            started = time.monotonic()
            try:
                written = self.trend_manager.tick()
                LOGGER.debug("trend tick written=%s", written)
            except Exception:
                LOGGER.exception("trend tick failed")
            await asyncio.sleep(max(0.0, self.config.trends.interval_seconds - (time.monotonic() - started)))

    async def _stats_loop(self, stop_at: float) -> None:
        while time.monotonic() < stop_at:
            await asyncio.sleep(5)
            LOGGER.info(
                "stats input_queue=%s valid_queue=%s telemetry_rows=%s invalid_rows=%s",
                self.input_queue.qsize(),
                self.valid_queue.qsize(),
                self.repo.telemetry_count(),
                self.repo.invalid_count(),
            )


async def init_database(config: AppConfig) -> None:
    app = TelemetryApplication(config)
    await app.initialize()
    app.repo.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Telemetry system")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--init-db", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--seconds", type=float, default=30.0)
    args = parser.parse_args(argv)

    config = load_config(Path(args.config))
    configure_logging(config.log_dir)
    if args.init_db:
        asyncio.run(init_database(config))
        return 0
    if args.run:
        asyncio.run(TelemetryApplication(config).run_for(args.seconds))
        return 0
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
