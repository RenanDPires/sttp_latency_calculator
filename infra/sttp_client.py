from __future__ import annotations
from sttp.subscriber import Subscriber
from sttp.config import Config
from sttp.settings import Settings
from sttp.transport.measurement import Measurement
from sttp.transport.signalindexcache import SignalIndexCache
from typing import List

from domain.models import LatencyEvent
from domain.ports import Clock, KeyExtractor
from app.pipeline import LatencyPipeline

class SttpLatencySubscriber(Subscriber):
    def __init__(self, pipeline: LatencyPipeline, clock: Clock, key_extractor: KeyExtractor):
        super().__init__()
        self.config = Config()
        self.settings = Settings()

        self.pipeline = pipeline
        self.clock = clock
        self.key_extractor = key_extractor
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

        self.pipeline.on_batch_received(batch_size=len(measurements))

        arrival_epoch = self.clock.now_epoch()

        for m in measurements:
            md = self.measurement_metadata(m)
            key = self.key_extractor.key_from(m, md)

            # baseline atual (igual ao seu código)
            t_meas_epoch = m.datetime.timestamp()

            ev = LatencyEvent(
                key=key,
                t_meas_epoch=t_meas_epoch,
                t_arrival_epoch=arrival_epoch,
                flags=int(m.flags),
                value=float(m.value),
            )
            self.pipeline.submit(ev)

        self.pipeline.maybe_flush()

    def connection_terminated(self):
        self.default_connectionterminated_receiver()
        self._started = False
