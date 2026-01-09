from gsf import Limits

from config import load_config
from app.processor import ShardedWindowProcessor
from app.pipeline import LatencyPipeline, WindowPolicy
from infra.clock import SystemClock
from infra.sinks import PrintSink
from infra.key_extractors import PpaKeyExtractor
from infra.sttp_client import SttpLatencySubscriber

def main():
    cfg = load_config()
    if cfg.port < 1 or cfg.port > Limits.MAXUINT16:
        raise SystemExit(f"Port out of range: {cfg.port}")

    processor = ShardedWindowProcessor(shards=cfg.shards, queue_size=cfg.queue_size)
    processor.start()

    clock = SystemClock()
    sink = PrintSink()
    policy = WindowPolicy(window_sec=cfg.window_sec, top_n=cfg.top_n)

    pipeline = LatencyPipeline(processor=processor, clock=clock, sink=sink, policy=policy)
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
            sub.dispose()

if __name__ == "__main__":
    main()
