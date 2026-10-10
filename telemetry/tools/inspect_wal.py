from __future__ import annotations

import argparse
from pathlib import Path

from telemetry.config import load_config
from telemetry.wal.trend_wal import TrendWal


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()
    config = load_config(Path(args.config))
    records = list(TrendWal(config.wal.path, fsync_each_record=False).iter_valid_records())
    print(f"valid_wal_records={len(records)}")
    if records:
        print(f"first_sequence={records[0].sequence}")
        print(f"last_sequence={records[-1].sequence}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

