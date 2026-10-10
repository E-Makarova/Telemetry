from __future__ import annotations

import os
import struct
from pathlib import Path

from telemetry.models import TrendSample


MAGIC = b"TRND1\0\0\0"
HEADER = struct.Struct(">8sII")
RECORD = struct.Struct(">QqB?dI")
TYPE_CODES = {"integer": 1, "float": 2, "boolean": 3}
TYPE_NAMES = {value: key for key, value in TYPE_CODES.items()}


class RingTrendStorage:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)

    def ensure_file(self, trend_id: str, window_size: int) -> Path:
        path = self.directory / f"{trend_id}.ring"
        expected_size = HEADER.size + RECORD.size * window_size
        if not path.exists() or path.stat().st_size != expected_size:
            with path.open("wb") as fh:
                fh.write(HEADER.pack(MAGIC, 1, window_size))
                fh.write(b"\0" * (RECORD.size * window_size))
                fh.flush()
                os.fsync(fh.fileno())
        return path

    def write_sample(self, trend_id: str, window_size: int, slot: int, sample: TrendSample) -> None:
        path = self.ensure_file(trend_id, window_size)
        type_code = TYPE_CODES[sample.scalar_type]
        numeric = float(sample.value)
        payload = struct.pack(">QqB?d", sample.sequence, sample.timestamp, type_code, True, numeric)
        crc = zlib_crc(payload)
        record = RECORD.pack(sample.sequence, sample.timestamp, type_code, True, numeric, crc)
        with path.open("r+b", buffering=0) as fh:
            fh.seek(HEADER.size + (slot % window_size) * RECORD.size)
            fh.write(record)
            os.fsync(fh.fileno())

    def read_window(self, trend_id: str, window_size: int, next_slot: int, count: int) -> list[TrendSample]:
        path = self.ensure_file(trend_id, window_size)
        start = 0 if count < window_size else next_slot
        samples: list[TrendSample] = []
        with path.open("rb") as fh:
            for i in range(count):
                slot = (start + i) % window_size
                fh.seek(HEADER.size + slot * RECORD.size)
                raw = fh.read(RECORD.size)
                sequence, timestamp, type_code, present, numeric, crc = RECORD.unpack(raw)
                if not present:
                    continue
                payload = struct.pack(">QqB?d", sequence, timestamp, type_code, present, numeric)
                if zlib_crc(payload) != crc:
                    continue
                scalar_type = TYPE_NAMES[type_code]
                value: int | float | bool
                if scalar_type == "integer":
                    value = int(numeric)
                elif scalar_type == "boolean":
                    value = bool(numeric)
                else:
                    value = float(numeric)
                samples.append(TrendSample(sequence, trend_id, timestamp, scalar_type, value))
        return samples


def zlib_crc(payload: bytes) -> int:
    import zlib

    return zlib.crc32(payload) & 0xFFFFFFFF

