from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import math
import threading
from queue import Queue, Full
from typing import Dict, List, Literal, Optional, Protocol, Tuple

from sttp.transport.measurement import Measurement


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


@dataclass(frozen=True)
class MeasurementEvent:
    """
    Evento de MEDIDA (não estatístico).
    Não entra no pipeline de latência.
    """
    ppa: int
    value: float
    t_meas_epoch: float
    t_arrival_epoch: float


@dataclass(frozen=True)
class ViolationEvent:
    """
    Evento de violação detectada.
    """
    t_epoch: float
    ppa: int
    value: float
    rule_id: str
    rule: str


@dataclass(frozen=True)
class LatencyAuditEvent:
    """
    Evento de auditoria de latência por medida.
    """
    t_arrival_epoch: float
    t_meas_epoch: float
    ppa: int
    value: float
    latency_ms: float
    flags: int


Op = Literal[">", "<", ">=", "<=", "==", "!="]


@dataclass(frozen=True)
class ThresholdRule:
    op: Op
    value: float
    rule_id: str
    atol: float = 0.0  # tolerância para comparações float

    def violated(self, x: float) -> bool:
        if self.op == ">":
            return x > self.value
        if self.op == "<":
            return x < self.value
        if self.op == ">=":
            return x >= self.value
        if self.op == "<=":
            return x <= self.value
        if self.op == "==":
            return math.isclose(x, self.value, abs_tol=self.atol) if self.atol else x == self.value
        if self.op == "!=":
            return not math.isclose(x, self.value, abs_tol=self.atol) if self.atol else x != self.value
        return False

    def label(self) -> str:
        return f"{self.op} {self.value}"


def latency_ms(t_arrival_epoch: float, t_meas_epoch: float) -> float:
    return (t_arrival_epoch - t_meas_epoch) * 1000.0


class Clock(Protocol):
    def now_epoch(self) -> float: ...


class KeyExtractor(Protocol):
    def key_from(self, measurement: Measurement, metadata: object) -> int: ...


class ReportSink(Protocol):
    def handle(self, report: WindowReport) -> None: ...


@dataclass(frozen=True)
class WriteJob:
    server_ip: str
    tempo: str        # "YYYY-MM-DD HH:MM:SS.mmm"
    ppa: int          # PPA de salvamento (destino)
    indicator: float  # latência, score, 0/1 etc.


class TickSink(Protocol):
    def publish(self, job: WriteJob) -> None: ...


@dataclass(frozen=True)
class PpaDestinations:
    latency_ppa: int
    frames_ppa: int


class PpaMapper(Protocol):
    def try_map(self, ppa_in: int) -> Optional[PpaDestinations]:
        ...


class ViolationSink(Protocol):
    def publish(self, ev: ViolationEvent) -> None: ...


class LatencyAuditSink(Protocol):
    def publish(self, ev: LatencyAuditEvent) -> None: ...


class ShardedWindowProcessor:
    def __init__(self, shards: int, queue_size: int):
        self.shards = shards
        self.queues = [Queue(maxsize=queue_size) for _ in range(shards)]
        self.stop = threading.Event()
        self.threads: List[threading.Thread] = []

        self._locks = [threading.Lock() for _ in range(shards)]
        self._wstats: List[Dict[int, WindowStats]] = [dict() for _ in range(shards)]

        self._tot_lock = threading.Lock()
        self.total_enqueued = 0
        self.total_dropped = 0
        self.total_processed = 0

    @staticmethod
    def _shard_of(key: int, shards: int) -> int:
        return (key * 2654435761) % shards

    def start(self) -> None:
        for i in range(self.shards):
            t = threading.Thread(target=self._worker, args=(i,), daemon=True)
            t.start()
            self.threads.append(t)

    def submit(self, ev: LatencyEvent) -> bool:
        shard = self._shard_of(ev.key, self.shards)
        try:
            self.queues[shard].put_nowait(ev)
            with self._tot_lock:
                self.total_enqueued += 1
            return True
        except Full:
            with self._locks[shard]:
                st = self._wstats[shard].get(ev.key)
                if st is None:
                    st = WindowStats()
                    self._wstats[shard][ev.key] = st
                st.dropped += 1
            with self._tot_lock:
                self.total_dropped += 1
            return False

    def _worker(self, shard_idx: int) -> None:
        q = self.queues[shard_idx]
        d = self._wstats[shard_idx]
        lock = self._locks[shard_idx]

        while not self.stop.is_set():
            ev = q.get()
            if ev is None:
                return
            try:
                lm = latency_ms(ev.t_arrival_epoch, ev.t_meas_epoch)
                with lock:
                    st = d.get(ev.key)
                    if st is None:
                        st = WindowStats()
                        d[ev.key] = st
                    st.add(lm)
                with self._tot_lock:
                    self.total_processed += 1
            finally:
                q.task_done()

    def snapshot_and_reset(self) -> List[WindowRow]:
        rows: List[WindowRow] = []
        for i in range(self.shards):
            with self._locks[i]:
                d = self._wstats[i]
                for key, st in d.items():
                    if st.count == 0 and st.dropped == 0:
                        continue
                    rows.append(WindowRow(
                        key=key,
                        count=st.count,
                        mean_ms=st.mean_ms,
                        max_ms=st.max_ms,
                        last_ms=st.last_ms,
                        dropped=st.dropped,
                    ))
                d.clear()
        return rows

    def totals(self) -> Tuple[int, int, int]:
        with self._tot_lock:
            return self.total_enqueued, self.total_processed, self.total_dropped

    def shutdown(self) -> None:
        self.stop.set()
        for q in self.queues:
            q.put(None)


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

            now = float(self.clock.now_epoch())
            w = float(self.policy.window_sec)

            # próximo múltiplo de w (ex.: 10s) estritamente no futuro
            next_boundary = (math.floor(now / w) + 1.0) * w

            # janela vigente: [start, end)
            self._next_flush = next_boundary
            self._start_epoch = self._next_flush - w

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


@dataclass
class ThresholdMonitorConfig:
    cooldown_sec: float = 0.0


class ThresholdMonitor:
    """
    Avalia regras por PPA em tempo real.
    Não escreve nada: apenas gera ViolationEvent.
    """

    def __init__(
        self,
        rules_by_ppa: Dict[int, List[ThresholdRule]],
        cfg: ThresholdMonitorConfig | None = None,
    ):
        self._rules_by_ppa = {int(k): list(v) for k, v in rules_by_ppa.items()}
        self._cfg = cfg or ThresholdMonitorConfig()
        self._last_emit: Dict[Tuple[int, str], float] = {}

    def check(self, now_epoch: float, ppa: int, value: float) -> List[ViolationEvent]:
        rules = self._rules_by_ppa.get(int(ppa))
        if not rules:
            return []

        out: List[ViolationEvent] = []
        for r in rules:
            if not r.violated(value):
                continue

            key = (int(ppa), r.rule_id)
            if self._cfg.cooldown_sec > 0:
                last = self._last_emit.get(key)
                if last is not None and (now_epoch - last) < self._cfg.cooldown_sec:
                    continue
                self._last_emit[key] = now_epoch

            out.append(
                ViolationEvent(
                    t_epoch=now_epoch,
                    ppa=int(ppa),
                    value=float(value),
                    rule_id=r.rule_id,
                    rule=r.label(),
                )
            )

        return out
