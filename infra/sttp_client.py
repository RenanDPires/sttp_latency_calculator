from __future__ import annotations

from typing import Dict, Tuple
from sttp.subscriber import Subscriber
from sttp.config import Config
from sttp.settings import Settings
from sttp.transport.measurement import Measurement
from sttp.transport.signalindexcache import SignalIndexCache
from typing import List, Optional, Set

from domain.models import LatencyEvent
from domain.ports import Clock, KeyExtractor, ViolationSink
from app.pipeline import LatencyPipeline
from app.threshold_monitor import ThresholdMonitor


class SttpLatencySubscriber(Subscriber):
    def __init__(
        self,
        pipeline: LatencyPipeline,
        clock: Clock,
        key_extractor: KeyExtractor,
        *,
        stats_keys: Optional[Set[int]] = None,
        threshold_monitor: Optional[ThresholdMonitor] = None,
        violation_sink: Optional[ViolationSink] = None,
    ):
        super().__init__()
        self.config = Config()
        self.settings = Settings()

        self.pipeline = pipeline
        self.clock = clock
        self.key_extractor = key_extractor

        # PPAs que entram no pipeline de stats (latência/frames)
        self.stats_keys: Set[int] = set(int(x) for x in (stats_keys or set()))

        # Monitor de violações (medidas) - independente do pipeline
        self.threshold_monitor = threshold_monitor
        self.violation_sink = violation_sink

        self._started = False

        # -----------------------------
        # Dedupe (drop de frames repetidos)
        # -----------------------------
        self._dedupe_ttl_s: float = 5.0  # janela de dedupe entre batches
        self._dedupe_seen: Dict[Tuple[int, float], float] = {}  # (ppa, t_meas_epoch) -> last_seen_arrival_epoch
        self._dedupe_cleanup_every: int = 2000
        self._dedupe_i: int = 0


        self.set_subscriptionupdated_receiver(self.subscription_updated)
        self.set_newmeasurements_receiver(self.new_measurements)
        self.set_connectionterminated_receiver(self.connection_terminated)

    def subscription_updated(self, signalindexcache: SignalIndexCache):
        self.statusmessage(f"Received signal index cache with {signalindexcache.count:,} mappings")

    def new_measurements(self, measurements: List[Measurement]):
        if not self._started:
            self._started = True
            self.statusmessage("Receiving measurements...")

        arrival_epoch = self.clock.now_epoch()

        # dedupe dentro do batch
        seen_batch: Set[Tuple[int, float]] = set()

        dropped_dupes = 0
        processed = 0

        for m in measurements:
            md = self.measurement_metadata(m)
            key = int(self.key_extractor.key_from(m, md))

            t_meas_epoch = float(m.datetime.timestamp())

            # ============================
            # DEDUPE GATE (batch + TTL)
            # ============================
            sig = (key, t_meas_epoch)

            # 1) Dedupe no batch
            if sig in seen_batch:
                dropped_dupes += 1
                continue
            seen_batch.add(sig)

            # 2) Dedupe entre batches (TTL)
            last_seen = self._dedupe_seen.get(sig)
            if last_seen is not None and (arrival_epoch - last_seen) <= self._dedupe_ttl_s:
                dropped_dupes += 1
                continue
            self._dedupe_seen[sig] = arrival_epoch

            # limpeza periódica do cache
            self._dedupe_i += 1
            if self._dedupe_i % self._dedupe_cleanup_every == 0:
                cutoff = arrival_epoch - self._dedupe_ttl_s
                # remove tudo que não é visto há mais que o TTL
                self._dedupe_seen = {k: v for k, v in self._dedupe_seen.items() if v >= cutoff}

            # ============================
            # daqui pra baixo: SÓ entra se NÃO for repetido
            # ============================
            processed += 1

            value = float(m.value)

            # (1) Violações (não entra se for repetido)
            if self.threshold_monitor is not None and self.violation_sink is not None:
                violations = self.threshold_monitor.check(
                    now_epoch=arrival_epoch,
                    ppa=key,
                    value=value,
                )
                for v in violations:
                    self.violation_sink.publish(v)

            # (2) Stats (latência / frames recebidos): só para chaves habilitadas
            if key not in self.stats_keys:
                continue

            ev = LatencyEvent(
                key=key,
                t_meas_epoch=t_meas_epoch,
                t_arrival_epoch=arrival_epoch,
                flags=int(m.flags),
                value=value,
            )
            self.pipeline.submit(ev)

        # Agora o "batch size" reflete o que foi realmente processado
        self.pipeline.on_batch_received(batch_size=processed)

        if dropped_dupes:
            self.statusmessage(f"[warn] dropped duplicated frames: {dropped_dupes} / {len(measurements)} (processed={processed})")

        self.pipeline.maybe_flush()



    def connection_terminated(self):
        self.default_connectionterminated_receiver()
        self._started = False
