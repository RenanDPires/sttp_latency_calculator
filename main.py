from gsf import Limits

import re

from config import load_config
from app.processor import ShardedWindowProcessor
from app.pipeline import LatencyPipeline, WindowPolicy
from infra.clock import SystemClock
from infra.sinks import PrintSink
from infra.key_extractors import PpaKeyExtractor
from infra.sttp_client import SttpLatencySubscriber

from infra.http_tick_sink import HttpTickSink
from infra.ppa_mapper import DictPpaMapper

from app.threshold_monitor import ThresholdMonitor, ThresholdMonitorConfig
from infra.violations_csv_sink import AsyncCsvViolationWriter


def _extract_ppas_from_subscription(text: str) -> list[int]:
    return [int(x) for x in re.findall(r"\bPPA\s*:\s*(\d+)\b", text or "", flags=re.IGNORECASE)]


# -----------------------------
# Null objects (para threshold-only)
# -----------------------------
class NoopTickSink:
    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def __getattr__(self, _name):
        # qualquer método inesperado vira no-op
        def _noop(*_a, **_kw):
            return None
        return _noop


class IdentityPpaMapper:
    """
    Mapper "pass-through" (retorna a própria chave).
    Útil quando não há tick_write/ppa_map_*.
    """
    def map_latency(self, ppa_in: int) -> int:
        return int(ppa_in)

    def map_frames(self, ppa_in: int) -> int:
        return int(ppa_in)

    def __getattr__(self, _name):
        # se o pipeline chamar outro método, no-op
        def _noop(*_a, **_kw):
            return None
        return _noop


def main():
    cfg = load_config()
    if cfg.port < 1 or cfg.port > Limits.MAXUINT16:
        raise SystemExit(f"Port out of range: {cfg.port}")

    # ---- validação dos maps (latência vs frames devem ter as mesmas chaves) ----
    k_lat: set[int] = set()
    k_frm: set[int] = set()

    if cfg.tick_write is not None:
        k_lat = set(cfg.tick_write.ppa_map_latency.keys())
        k_frm = set(cfg.tick_write.ppa_map_frames.keys())

        if k_lat != k_frm:
            raise SystemExit(
                "Config inválida: chaves diferentes em ppa_map_latency vs ppa_map_frames. "
                f"lat_only={sorted(k_lat - k_frm)} frames_only={sorted(k_frm - k_lat)}"
            )

    # ---- prepara monitor de violações (opcional) ----
    threshold_monitor = None
    violation_writer = None
    monitor_keys: set[int] = set()

    if cfg.threshold_monitor and cfg.threshold_monitor.enabled:
        rules_by_ppa = cfg.threshold_monitor.rules or {}
        monitor_keys = set(int(k) for k in rules_by_ppa.keys())

        threshold_monitor = ThresholdMonitor(
            rules_by_ppa=rules_by_ppa,
            cfg=ThresholdMonitorConfig(cooldown_sec=cfg.threshold_monitor.cooldown_sec),
        )

        violation_writer = AsyncCsvViolationWriter(
            cfg.threshold_monitor.csv_path,
            queue_max=cfg.threshold_monitor.queue_max,
            drop_on_full=cfg.threshold_monitor.drop_on_full,
            flush_every_n=cfg.threshold_monitor.flush_every_n,
            flush_every_sec=cfg.threshold_monitor.flush_every_sec,
        )
        violation_writer.start()

        print(f"[violations] enabled=True csv={cfg.threshold_monitor.csv_path} rules_ppas={sorted(monitor_keys)}")
    else:
        print("[violations] enabled=False")

    # stats: apenas quando tick_write existe
    stats_keys: set[int] = set()
    if cfg.tick_write is not None:
        stats_keys = set(cfg.tick_write.ppa_map_latency.keys())

    # threshold
    monitor_keys: set[int] = set()
    if cfg.threshold_monitor and cfg.threshold_monitor.enabled:
        monitor_keys = set(cfg.threshold_monitor.rules.keys())

    # subscription = união
    subscription_keys = stats_keys | monitor_keys


    # ---- subscription: se não vier no YAML, gera a partir da união (stats ∪ monitor) ----
    subscription = (cfg.subscription or "").strip()
    if not subscription:
        union_keys = sorted(stats_keys | monitor_keys)
        subscription = "; ".join(f"PPA:{k}" for k in union_keys)

    print("subscription repr:", repr(subscription))
    print("PPAs solicitados na subscription:", _extract_ppas_from_subscription(subscription))

    # (útil) mostrar roteamento IN -> OUTs (somente quando há tick_write)
    if cfg.tick_write is not None:
        for k in sorted(stats_keys):
            print(
                f"[stats-map] PPA_IN {k} -> latency_out {cfg.tick_write.ppa_map_latency.get(k)} "
                f"| frames_out {cfg.tick_write.ppa_map_frames.get(k)}"
            )
    else:
        print(f"[stats] threshold-only PPAs: {sorted(stats_keys)}")

    # ---- pipeline core ----
    processor = ShardedWindowProcessor(shards=cfg.shards, queue_size=cfg.queue_size)
    processor.start()

    clock = SystemClock()
    sink = PrintSink()
    policy = WindowPolicy(window_sec=cfg.window_sec, top_n=cfg.top_n)

    # ---- tick sink + mapper (somente se tick_write existir) ----
    if cfg.tick_write is not None:
        tick_sink = HttpTickSink(
            cfg.tick_write.url,
            workers=cfg.tick_write.workers,
            queue_max=cfg.tick_write.queue_max,
            timeout_sec=cfg.tick_write.timeout_sec,
            max_retries=cfg.tick_write.max_retries,
            drop_on_full=cfg.tick_write.drop_on_full,
        )
        tick_sink.start()

        ppa_mapper = DictPpaMapper(
            cfg.tick_write.ppa_map_latency,
            cfg.tick_write.ppa_map_frames,
        )

        tick_server_ip = cfg.tick_write.server_ip
    else:
        # threshold-only: não escreve ticks/stats
        tick_sink = NoopTickSink()
        ppa_mapper = IdentityPpaMapper()
        tick_server_ip = ""

    pipeline = LatencyPipeline(
        processor=processor,
        clock=clock,
        sink=sink,
        policy=policy,
        tick_sink=tick_sink,
        ppa_mapper=ppa_mapper,
        tick_server_ip=tick_server_ip,
    )

    key_extractor = PpaKeyExtractor()

    # IMPORTANTE: subscriber deve suportar stats_keys, threshold_monitor e violation_sink
    sub = SttpLatencySubscriber(
        pipeline=pipeline,
        clock=clock,
        key_extractor=key_extractor,
        stats_keys=stats_keys,
        threshold_monitor=threshold_monitor,
        violation_sink=violation_writer,
    )

    sub.config.compress_payloaddata = False
    sub.settings.udpport = 9600
    sub.settings.use_millisecondresolution = True

    try:
        sub.subscribe(subscription, sub.settings)
        sub.connect(f"{cfg.hostname}:{cfg.port}", sub.config)
        print("Connected. Press ENTER to stop...")
        input()
    finally:
        try:
            processor.shutdown()
        finally:
            try:
                if tick_sink is not None:
                    tick_sink.stop()
            finally:
                try:
                    if violation_writer is not None:
                        violation_writer.stop()
                finally:
                    sub.dispose()


if __name__ == "__main__":
    main()
