from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

from sttp.transport.measurement import Measurement

from .models import LatencyEvent, WindowReport


class Clock(Protocol):
    def now_epoch(self) -> float: ...


class KeyExtractor(Protocol):
    def key_from(self, measurement: Measurement, metadata: object) -> int: ...


class ReportSink(Protocol):
    def handle(self, report: WindowReport) -> None: ...


# -----------------------------
# Tick-a-tick publishing (HTTP/NoSQL/etc.)
# -----------------------------

@dataclass(frozen=True)
class WriteJob:
    server_ip: str
    tempo: str        # "YYYY-MM-DD HH:MM:SS.mmm"
    ppa: int          # PPA de salvamento (destino)
    indicator: float  # latência, score, 0/1 etc.

class TickSink(Protocol):
    def publish(self, job: WriteJob) -> None: ...

class PpaMapper(Protocol):
    def try_map(self, ppa_src: int) -> Optional[int]:
        """Mapeia src (cálculo) -> dst (salvamento). None = não publica."""
        ...