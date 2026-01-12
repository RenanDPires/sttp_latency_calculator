from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone

from domain.models import LatencyEvent, WindowReport
from domain.ports import Clock, ReportSink, TickSink, WriteJob, PpaMapper

from .processor import ShardedWindowProcessor


@dataclass
class WindowPolicy:
    window_sec: float
    top_n: int


class LatencyPipeline:
    """
    Recebe eventos, agrega por janela e emite WindowReport para um Sink.
    Também publica latência tick-a-tick (WriteJob) para um TickSink.
    """

    def __init__(
        self,
        processor: ShardedWindowProcessor,
        clock: Clock,
        sink: ReportSink,
        policy: WindowPolicy,
        *,
        tick_sink: TickSink,
        ppa_mapper: PpaMapper,
        tick_server_ip: str,
    ):
        self.processor = processor
        self.clock = clock
        self.sink = sink
        self.policy = policy

        self.tick_sink = tick_sink
        self.ppa_mapper = ppa_mapper
        self.tick_server_ip = tick_server_ip

        self._started = False
        self._next_flush = 0.0
        self._last_batch_size = 0

    def on_batch_received(self, batch_size: int) -> None:
        if not self._started:
            self._started = True
            self._next_flush = self.clock.now_epoch() + self.policy.window_sec
        self._last_batch_size = batch_size

    def submit(self, ev: LatencyEvent) -> None:
        # 1) Comportamenteo por janela
        self.processor.submit(ev)

        # 2) latência por tick (ms)
        lat_ms = (ev.t_arrival_epoch - ev.t_meas_epoch) * 1000.0

        # 3) map src (key) -> dst (ppa de salvamento) -- Leitura/escrita
        ppa_dst = self.ppa_mapper.try_map(ev.key)
        if ppa_dst is None:
            return

        # 4) tempo no formato exigido pelo endpoint: "YYYY-MM-DD HH:MM:SS.mmm"
        dt = datetime.fromtimestamp(ev.t_meas_epoch, tz=timezone.utc)
        tempo_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # 5) publicar no endpoint (indicator = latência)
        self.tick_sink.publish(
            WriteJob(
                server_ip=self.tick_server_ip,
                tempo=tempo_str,
                ppa=int(ppa_dst),
                indicator=float(lat_ms),
            )
        )

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
