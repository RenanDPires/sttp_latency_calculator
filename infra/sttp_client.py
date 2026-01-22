from __future__ import annotations

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

        self.set_subscriptionupdated_receiver(self.subscription_updated)
        self.set_newmeasurements_receiver(self.new_measurements)
        self.set_connectionterminated_receiver(self.connection_terminated)

    def subscription_updated(self, signalindexcache: SignalIndexCache):
        self.statusmessage(f"Received signal index cache with {signalindexcache.count:,} mappings")

    def new_measurements(self, measurements: List[Measurement]):
        if not self._started:
            self._started = True
            self.statusmessage("Receiving measurements...")

        # informa tamanho do batch (pipeline usa isso para report)
        self.pipeline.on_batch_received(batch_size=len(measurements))

        arrival_epoch = self.clock.now_epoch()

        for m in measurements:
            md = self.measurement_metadata(m)
            key = int(self.key_extractor.key_from(m, md))

            t_meas_epoch = m.datetime.timestamp()
            value = float(m.value)

            if self.threshold_monitor is not None and self.violation_sink is not None:
                violations = self.threshold_monitor.check(
                    now_epoch=arrival_epoch,
                    ppa=key,
                    value=value,
                )
                for v in violations:
                    self.violation_sink.publish(v)

            
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

        self.pipeline.maybe_flush()


    def connection_terminated(self):
        self.default_connectionterminated_receiver()
        self._started = False
