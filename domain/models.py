from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class LatencyEvent:
    key: int
    t_meas_epoch: float
    t_arrival_epoch: float

    # mantêm compatibilidade com quem já envia
    flags: int = 0
    value: float = 0.0

@dataclass
class WindowStats:
    count: int = 0
    sum_ms: float = 0.0
    max_ms: float = 0.0
    last_ms: float = 0.0
    dropped: int = 0

    def add(self, lat_ms: float) -> None:
        self.count += 1
        self.sum_ms += lat_ms
        self.last_ms = lat_ms
        if lat_ms > self.max_ms:
            self.max_ms = lat_ms

    @property
    def mean_ms(self) -> float:
        return self.sum_ms / self.count if self.count else 0.0

@dataclass(frozen=True)
class WindowRow:
    key: int
    count: int
    mean_ms: float
    max_ms: float
    last_ms: float
    dropped: int

@dataclass(frozen=True)
class WindowReport:
    window_sec: float
    stamp_epoch: float
    batch_size_last: int
    shards: int
    total_enqueued: int
    total_processed: int
    total_dropped: int
    rows: List[WindowRow]
