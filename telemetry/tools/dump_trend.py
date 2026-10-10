from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path

from telemetry.app import TelemetryApplication
from telemetry.config import load_config


def format_timestamp(timestamp_ns: int) -> str:
    """Convert Unix timestamp in nanoseconds to local readable time."""
    return datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


async def _run(config_path: Path, trend_id: str) -> int:
    app = TelemetryApplication(load_config(config_path))
    await app.initialize()
    assert app.trend_manager is not None
    samples = app.trend_manager.read_window(trend_id)
    print("ВРЕМЯ (локальное) | sequence | тренд | тип | значение")
    for sample in samples[-20:]:
        print(f"{format_timestamp(sample.timestamp)} | {sample.sequence} | {sample.trend_id} | {sample.scalar_type} | {sample.value}")
    app.repo.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--trend-id", required=True)
    args = parser.parse_args()
    return asyncio.run(_run(Path(args.config), args.trend_id))


if __name__ == "__main__":
    raise SystemExit(main())
