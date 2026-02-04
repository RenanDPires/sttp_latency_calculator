from __future__ import annotations

from typing import Dict, Tuple, List, Optional, Set

from sttp.subscriber import Subscriber
from sttp.config import Config
from sttp.settings import Settings
from sttp.transport.measurement import Measurement
from sttp.transport.signalindexcache import SignalIndexCache

from core import (
    Clock,
    KeyExtractor,
    LatencyAuditEvent,
    LatencyEvent,
    LatencyPipeline,
    ThresholdMonitor,
    ViolationEvent,
    ViolationSink,
    WindowAuditSink,
)


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
        window_audit_sink: Optional[WindowAuditSink] = None,
        audit_on_negative_latency: bool = True,
        log_raw_on_negative: bool = False,
        # -----------------------------
        # Alinhamento
        # -----------------------------
        align_window_sec: int = 10,
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

        # Auditoria de latência (persistência por janela)
        self.window_audit_sink = window_audit_sink
        self.audit_on_negative_latency = audit_on_negative_latency
        self.log_raw_on_negative = log_raw_on_negative
        self._audit_window_sec = int(align_window_sec)
        self._audit_window_start: Optional[float] = None
        self._audit_events: List[LatencyAuditEvent] = []
        self._audit_has_negative = False

        self._started = False

        # -----------------------------
        # Align gate (descarta até o próximo X0)
        # -----------------------------
        self._aligned: bool = False
        self._align_window_sec: int = int(align_window_sec)
        self._align_target_epoch: Optional[float] = None  # epoch inteiro do X0 alvo
        self._dropped_before_align: int = 0

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

    # -----------------------------
    # Align helpers
    # -----------------------------
    def _compute_next_boundary_epoch(self, now_epoch: float) -> int:
        """
        Retorna o próximo múltiplo de self._align_window_sec (ex.: 10s),
        sempre como epoch inteiro (sem fração).
        """
        w = int(self._align_window_sec)
        return (int(now_epoch) // w + 1) * w

    def new_measurements(self, measurements: List[Measurement]):
        if not self._started:
            self._started = True
            self.statusmessage("Receiving measurements...")

        # ============================
        # ALIGN GATE: descarta até o próximo X0
        # ============================
        if not self._aligned:
            now_epoch = float(self.clock.now_epoch())

            if self._align_target_epoch is None:
                self._align_target_epoch = float(self._compute_next_boundary_epoch(now_epoch))
                self.statusmessage(
                    f"[align] gating enabled. dropping until epoch={self._align_target_epoch:.0f} "
                    f"(window={self._align_window_sec}s)"
                )

            if now_epoch < float(self._align_target_epoch):
                self._dropped_before_align += len(measurements)

                # log "suave" (evita spam): a cada ~5000 descartados, em média
                if (self._dropped_before_align % 5000) < len(measurements):
                    self.statusmessage(
                        f"[align] dropped_before_align={self._dropped_before_align} "
                        f"now={now_epoch:.3f} target={self._align_target_epoch:.0f}"
                    )
                return

            self._aligned = True
            self.statusmessage(
                f"[align] released at now_epoch={now_epoch:.3f} target={self._align_target_epoch:.0f} "
                f"dropped={self._dropped_before_align}"
            )

        arrival_epoch = self.clock.now_epoch()

        # dedupe dentro do batch
        seen_batch: Set[Tuple[int, float]] = set()

        dropped_dupes = 0
        processed = 0

        for m in measurements:
            md = self.measurement_metadata(m)
            key = int(self.key_extractor.key_from(m, md))

            if self.raw_measurement_sink is not None:
                self.raw_measurement_sink.publish(
                    build_raw_measurement_record(
                        arrival_epoch=float(arrival_epoch),
                        measurement=m,
                        metadata=md,
                        ppa_key=key,
                    )
                )

            t_meas_epoch = float(m.datetime.timestamp())
            latency = (arrival_epoch - t_meas_epoch) * 1000.0
            window_start = (int(arrival_epoch) // self._audit_window_sec) * self._audit_window_sec

            if self._audit_window_start is None:
                self._audit_window_start = float(window_start)
            elif window_start != int(self._audit_window_start):
                self._flush_audit_window()
                self._audit_window_start = float(window_start)

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

            self._audit_events.append(
                LatencyAuditEvent(
                    t_arrival_epoch=arrival_epoch,
                    t_meas_epoch=t_meas_epoch,
                    ppa=key,
                    value=value,
                    latency_ms=latency,
                    flags=int(m.flags),
                )
            )
            if latency < 0:
                self._audit_has_negative = True
                if self.log_raw_on_negative:
                    self.statusmessage(
                        "[raw-measurement] "
                        f"ppa={key} arrival_epoch={arrival_epoch:.6f} "
                        f"meas_epoch={t_meas_epoch:.6f} delta_ms={latency:.3f} "
                        f"value={value} flags={int(m.flags)} "
                        f"signalid={getattr(m, 'signalid', None)}"
                    )

            if latency < 0 and self.violation_sink is not None:
                self.violation_sink.publish(
                    ViolationEvent(
                        t_epoch=arrival_epoch,
                        ppa=int(key),
                        value=float(latency),
                        rule_id="LATENCY_LT_0",
                        rule="< 0 ms",
                    )
                )

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
            self.statusmessage(
                f"[warn] dropped duplicated frames: {dropped_dupes} / {len(measurements)} (processed={processed})"
            )

        self.pipeline.maybe_flush()
        self._maybe_flush_audit_by_time(self.clock.now_epoch())

    def connection_terminated(self):
        self.default_connectionterminated_receiver()
        self._started = False

        # reset do alinhamento ao reconectar
        self._aligned = False
        self._align_target_epoch = None
        self._dropped_before_align = 0
        self._flush_audit_window()
        self._audit_window_start = None
        self._audit_events = []
        self._audit_has_negative = False

    def flush_audit_window(self) -> None:
        self._flush_audit_window()
        self._audit_window_start = None
        self._audit_events = []
        self._audit_has_negative = False

    def _maybe_flush_audit_by_time(self, now_epoch: float) -> None:
        if self._audit_window_start is None:
            return
        window_end = float(self._audit_window_start) + float(self._audit_window_sec)
        if now_epoch >= window_end:
            self._flush_audit_window()
            self._audit_window_start = float(
                (int(now_epoch) // self._audit_window_sec) * self._audit_window_sec
            )

    def _flush_audit_window(self) -> None:
        if self.window_audit_sink is None or self._audit_window_start is None:
            self._audit_events = []
            self._audit_has_negative = False
            return

        should_write = (not self.audit_on_negative_latency) or self._audit_has_negative
        if should_write and self._audit_events:
            window_start = float(self._audit_window_start)
            window_end = window_start + float(self._audit_window_sec)
            self.window_audit_sink.write_window(window_start, window_end, list(self._audit_events))

        self._audit_events = []
        self._audit_has_negative = False
