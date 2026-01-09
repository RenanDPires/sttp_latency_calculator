from __future__ import annotations
from typing import Protocol
from sttp.transport.measurement import Measurement

from .models import LatencyEvent, WindowReport

class Clock(Protocol):
    def now_epoch(self) -> float: ...

class KeyExtractor(Protocol):
    def key_from(self, measurement: Measurement, metadata) -> int: ...

class ReportSink(Protocol):
    def handle(self, report: WindowReport) -> None: ...
