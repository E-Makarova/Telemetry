from __future__ import annotations

import asyncio
import sqlite3
import tracemalloc

from telemetry.app import TelemetryApplication
from telemetry.config import load_config

from test_system import make_config, workspace_tmp


def test_load_pipeline_with_four_sources_and_trends() -> None:
    tmp_path = workspace_tmp("separate_load")
    config = load_config(make_config(tmp_path, window_size=20, max_changes=600))
    tracemalloc.start()
    app = TelemetryApplication(config)
    asyncio.run(app.run_for(0.8))
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    with sqlite3.connect(config.sqlite.path) as conn:
        telemetry_rows = conn.execute("SELECT COUNT(*) FROM telemetry_values").fetchone()[0]
        trend_count = conn.execute("SELECT COUNT(*) FROM trend_state WHERE count > 0").fetchone()[0]
    assert telemetry_rows > 0
    assert trend_count == 20
    assert peak < 75 * 1024 * 1024

