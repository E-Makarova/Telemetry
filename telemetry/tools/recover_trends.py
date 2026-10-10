from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from telemetry.app import TelemetryApplication
from telemetry.config import load_config
from telemetry.logging_config import configure_logging


async def _run(config_path: Path) -> int:
    config = load_config(config_path)
    configure_logging(config.log_dir)
    app = TelemetryApplication(config)
    await app.initialize()
    app.repo.close()
    print("recovery_complete=true")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()
    return asyncio.run(_run(Path(args.config)))


if __name__ == "__main__":
    raise SystemExit(main())

