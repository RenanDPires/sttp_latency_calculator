from gsf import Limits

import logging

from config import load_config
from app.processor import ShardedWindowProcessor
from app.pipeline import LatencyPipeline, WindowPolicy
from infra.clock import SystemClock
from infra.sinks import PrintSink
from infra.key_extractors import PpaKeyExtractor
from infra.sttp_client import SttpLatencySubscriber

from infra.http_tick_sink import HttpTickSink
from infra.ppa_mapper import DictPpaMapper


def main():
    cfg = load_config()
    if cfg.port < 1 or cfg.port > Limits.MAXUINT16:
        raise SystemExit(f"Port out of range: {cfg.port}")

    processor = ShardedWindowProcessor(shards=cfg.shards, queue_size=cfg.queue_size)
    processor.start()

    clock = SystemClock()
    sink = PrintSink()
    policy = WindowPolicy(window_sec=cfg.window_sec, top_n=cfg.top_n)

    
    tick_sink = HttpTickSink(
        cfg.tick_write.url,
        workers=cfg.tick_write.workers,
        queue_max=cfg.tick_write.queue_max,
        timeout_sec=cfg.tick_write.timeout_sec,
        max_retries=cfg.tick_write.max_retries,
        drop_on_full=cfg.tick_write.drop_on_full,
    )
    tick_sink.start()

    ppa_mapper = DictPpaMapper(cfg.tick_write.ppa_map)

    pipeline = LatencyPipeline(
        processor=processor,
        clock=clock,
        sink=sink,
        policy=policy,
        tick_sink=tick_sink,
        ppa_mapper=ppa_mapper,
        tick_server_ip=cfg.tick_write.server_ip,
    )

    key_extractor = PpaKeyExtractor()

    sub = SttpLatencySubscriber(pipeline=pipeline, clock=clock, key_extractor=key_extractor)
    sub.config.compress_payloaddata = False
    sub.settings.udpport = 9600
    sub.settings.use_millisecondresolution = True

    try:
        sub.subscribe(cfg.subscription, sub.settings)
        sub.connect(f"{cfg.hostname}:{cfg.port}", sub.config)
        print("Connected. Press ENTER to stop...")
        input()
    finally:
        try:
            processor.shutdown()
        finally:
            try:
                tick_sink.stop()
            finally:
                sub.dispose()


if __name__ == "__main__":
    main()
