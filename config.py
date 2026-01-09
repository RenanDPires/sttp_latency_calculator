import os
from dataclasses import dataclass

@dataclass(frozen=True)
class AppConfig:
    hostname: str
    port: int
    subscription: str
    window_sec: float
    shards: int
    queue_size: int
    top_n: int

def load_config() -> AppConfig:
    hostname = os.getenv("HOSTNAME", "localhost")
    port = int(os.getenv("PORT", "7165"))
    subscription = os.getenv("SUBSCRIPTION", "PPA:477; PPA:479; PPA:931; PPA:933")
    window_sec = float(os.getenv("WINDOW_SEC", "1.0"))
    top_n = int(os.getenv("TOP_N", "10"))

    default_shards = max(4, min(32, (os.cpu_count() or 4) * 2))
    shards = int(os.getenv("SHARDS", str(default_shards)))
    queue_size = int(os.getenv("QUEUE_SIZE", "100000"))

    return AppConfig(hostname, port, subscription, window_sec, shards, queue_size, top_n)
