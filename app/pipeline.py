from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from domain.models import LatencyEvent, WindowReport
from domain.ports import Clock, ReportSink

from .processor import ShardedWindowProcessor

@dataclass
class WindowPolicy:
    window_sec: float
    top_n: int

class LatencyPipeline:
    """
    Recebe eventos, agrega por janela e emite WindowReport para um Sink.
    """
    def __init__(
        self,
        processor: ShardedWindowProcessor,
        clock: Clock,
        sink: ReportSink,
        policy: WindowPolicy,
    ):
        self.processor = processor
        self.clock = clock
        self.sink = sink
        self.policy = policy

        self._started = False
        self._next_flush = 0.0
        self._last_batch_size = 0

    def on_batch_received(self, batch_size: int) -> None:
        if not self._started:
            self._started = True
            self._next_flush = self.clock.now_epoch() + self.policy.window_sec
        self._last_batch_size = batch_size

    def submit(self, ev: LatencyEvent) -> None:
        self.processor.submit(ev)

    def maybe_flush(self) -> None:
        now = self.clock.now_epoch()
        if not self._started or now < self._next_flush:
            return

        rows = self.processor.snapshot_and_reset()
        rows.sort(key=lambda r: r.max_ms, reverse=True)
        if self.policy.top_n > 0:
            rows = rows[: self.policy.top_n]

        enq, proc, drop = self.processor.totals()

        report = WindowReport(
            window_sec=self.policy.window_sec,
            stamp_epoch=now,
            batch_size_last=self._last_batch_size,
            shards=self.processor.shards,
            total_enqueued=enq,
            total_processed=proc,
            total_dropped=drop,
            rows=rows,
        )

        self.sink.handle(report)
        self._next_flush = now + self.policy.window_sec
