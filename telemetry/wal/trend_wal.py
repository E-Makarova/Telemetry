from __future__ import annotations

import json
import os
import struct
import zlib
from pathlib import Path
from typing import Iterator

from telemetry.models import TrendSample


HEADER = struct.Struct(">I")
CRC = struct.Struct(">I")


class TrendWal:
    def __init__(self, path: Path, fsync_each_record: bool = True) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fsync_each_record = fsync_each_record
        self.path.touch(exist_ok=True)

    def append(self, sample: TrendSample) -> int:
        payload = json.dumps(
            {
                "sequence": sample.sequence,
                "trend_id": sample.trend_id,
                "timestamp": sample.timestamp,
                "scalar_type": sample.scalar_type,
                "value": sample.value,
                "committed": True,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        record = HEADER.pack(len(payload)) + payload + CRC.pack(zlib.crc32(payload) & 0xFFFFFFFF)
        with self.path.open("ab", buffering=0) as fh:
            offset = fh.tell()
            fh.write(record)
            if self.fsync_each_record:
                os.fsync(fh.fileno())
        return offset

    def iter_valid_records(self) -> Iterator[TrendSample]:
        for sample, _ in self.iter_valid_records_with_offsets():
            yield sample

    def iter_valid_records_with_offsets(self) -> Iterator[tuple[TrendSample, int]]:
        offset = 0
        with self.path.open("rb") as fh:
            while True:
                header = fh.read(HEADER.size)
                if not header:
                    break
                if len(header) != HEADER.size:
                    break
                length = HEADER.unpack(header)[0]
                payload = fh.read(length)
                crc_raw = fh.read(CRC.size)
                if len(payload) != length or len(crc_raw) != CRC.size:
                    break
                expected_crc = CRC.unpack(crc_raw)[0]
                actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                if expected_crc != actual_crc:
                    break
                data = json.loads(payload.decode("utf-8"))
                if not data.get("committed", False):
                    break
                next_offset = fh.tell()
                yield (
                    TrendSample(
                        sequence=int(data["sequence"]),
                        trend_id=str(data["trend_id"]),
                        timestamp=int(data["timestamp"]),
                        scalar_type=str(data["scalar_type"]),
                        value=data["value"],
                    ),
                    next_offset,
                )
                offset = next_offset
        self.truncate(offset)

    def truncate(self, size: int) -> None:
        with self.path.open("ab") as fh:
            fh.truncate(size)
            os.fsync(fh.fileno())

    def compact(self) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("wb") as fh:
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

