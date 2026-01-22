from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from collections import defaultdict
import math

from domain.models import LatencyEvent, WindowReport
from domain.ports import (
    Clock,
    ReportSink,
    TickSink,
    WriteJob,
    PpaMapper,
    PpaDestinations,
)

from .processor import ShardedWindowProcessor


@dataclass
class WindowPolicy:
    window_sec: float
    top_n: int


class LatencyPipeline:
    """
    Pipeline de estatísticas por janela.

    - Janela alinhada no segundo .00
    - Para cada PPA de entrada:
        * latência média (ms)
        * frames recebidos (count)
    - 1 PPA de entrada -> 2 PPAs de saída (latência e frames)
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
        self._start_epoch = 0.0
        self._next_flush = 0.0
        self._last_batch_size = 0

        # Estatísticas acumuladas por janela (key = PPA de entrada)
        self._sum_latency_ms = defaultdict(float)
        self._count_frames = defaultdict(int)

    def on_batch_received(self, batch_size: int) -> None:
        if not self._started:
            self._started = True

            now = self.clock.now_epoch()
            # início da janela no próximo segundo cheio (.00)
            self._start_epoch = math.floor(now) + 1.0
            self._next_flush = self._start_epoch + float(self.policy.window_sec)

        self._last_batch_size = batch_size

    def submit(self, ev: LatencyEvent) -> None:
        # ignora eventos antes do início alinhado
        if self._started and ev.t_arrival_epoch < self._start_epoch:
            return

        # 1) agregação para relatório humano (latência max, etc.)
        self.processor.submit(ev)

        # 2) estatísticas por janela
        lat_ms = (ev.t_arrival_epoch - ev.t_meas_epoch) * 1000.0
        key = int(ev.key)

        self._sum_latency_ms[key] += float(lat_ms)
        self._count_frames[key] += 1

    def maybe_flush(self) -> None:
        now = self.clock.now_epoch()
        if not self._started or now < self._next_flush:
            return

        # Pode ter atraso; faz catch-up mantendo o grid exato
        while now >= self._next_flush:
            flush_epoch = self._next_flush  # boundary EXATO, alinhado no .000

            # timestamp do ponto estatístico = boundary, não "now"
            dt_flush = datetime.fromtimestamp(flush_epoch, tz=timezone.utc)
            tempo_str = dt_flush.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Publicar estatísticas por PPA de entrada
            for key, frames in list(self._count_frames.items()):
                if frames <= 0:
                    continue

                sum_ms = float(self._sum_latency_ms.get(key, 0.0))
                mean_ms = sum_ms / float(frames)

                dests = self.ppa_mapper.try_map(key)
                if dests is None:
                    continue

                self.tick_sink.publish(
                    WriteJob(
                        server_ip=self.tick_server_ip,
                        tempo=tempo_str,
                        ppa=int(dests.latency_ppa),
                        indicator=float(mean_ms),
                    )
                )

                self.tick_sink.publish(
                    WriteJob(
                        server_ip=self.tick_server_ip,
                        tempo=tempo_str,
                        ppa=int(dests.frames_ppa),
                        indicator=float(frames),
                    )
                )

            # reset stats da janela
            self._sum_latency_ms.clear()
            self._count_frames.clear()

            # relatório humano (mantém)
            rows = self.processor.snapshot_and_reset()
            rows.sort(key=lambda r: r.max_ms, reverse=True)
            if self.policy.top_n > 0:
                rows = rows[: self.policy.top_n]

            enq, proc, drop = self.processor.totals()

            report = WindowReport(
                window_sec=self.policy.window_sec,
                stamp_epoch=flush_epoch,  # opcional: também alinhar o stamp do report
                batch_size_last=self._last_batch_size,
                shards=self.processor.shards,
                total_enqueued=enq,
                total_processed=proc,
                total_dropped=drop,
                rows=rows,
            )
            self.sink.handle(report)

            # avança mantendo alinhamento perfeito
            self._next_flush += float(self.policy.window_sec)

            # atualiza now pra condição do while (se quiser)
            now = self.clock.now_epoch()
