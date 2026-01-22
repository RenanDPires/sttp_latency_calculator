from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


@dataclass(frozen=True)
class TickWriteConfig:
    url: str
    server_ip: str

    workers: int = 4
    queue_max: int = 5000
    timeout_sec: float = 2.0
    max_retries: int = 3
    drop_on_full: bool = False

    # NOVO: dois destinos por PPA de entrada
    ppa_map_latency: dict[int, int] = None  # type: ignore
    ppa_map_frames: dict[int, int] = None   # type: ignore


@dataclass(frozen=True)
class AppConfig:
    hostname: str
    port: int

    window_sec: int = 10
    top_n: int = 10
    shards: int = 8
    queue_size: int = 100000

    subscription: str = ""

    tick_write: TickWriteConfig = None  # type: ignore


def _req(d: Mapping[str, Any], path: str) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, Mapping) or part not in cur:
            raise ValueError(f"Config inválida: campo obrigatório '{path}' ausente.")
        cur = cur[part]
    return cur


def _opt(d: Mapping[str, Any], path: str, default: Any) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, Mapping) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _to_int_map(x: Any, path: str) -> dict[int, int]:
    if not isinstance(x, Mapping):
        raise ValueError(f"Config inválida: '{path}' deve ser um mapa (dict).")
    out: dict[int, int] = {}
    for k, v in x.items():
        try:
            out[int(k)] = int(v)
        except Exception as e:
            raise ValueError(f"Config inválida: '{path}' contém chave/valor não-inteiro: {k}:{v}") from e
    return out


def load_config(path: str = "config.yaml") -> AppConfig:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}

    hostname = _req(data, "hostname")
    port = int(_req(data, "port"))

    window_sec = int(_opt(data, "window_sec", 10))
    top_n = int(_opt(data, "top_n", 10))
    shards = int(_opt(data, "shards", 8))
    queue_size = int(_opt(data, "queue_size", 100000))

    subscription = str(_opt(data, "subscription", ""))

    tw = _req(data, "tick_write")

    url = _req(tw, "url")
    server_ip = _req(tw, "server_ip")

    workers = int(_opt(tw, "workers", 4))
    queue_max = int(_opt(tw, "queue_max", 5000))
    timeout_sec = float(_opt(tw, "timeout_sec", 2.0))
    max_retries = int(_opt(tw, "max_retries", 3))
    drop_on_full = bool(_opt(tw, "drop_on_full", False))

    # --- NOVO formato ---
    latency_raw = _opt(tw, "ppa_map_latency", None)
    frames_raw = _opt(tw, "ppa_map_frames", None)

    # --- Compatibilidade com formato antigo (ppa_map) ---
    legacy_raw = _opt(tw, "ppa_map", None)

    if latency_raw is None and frames_raw is None and legacy_raw is not None:
        # mantém latência do mapa antigo
        ppa_map_latency = _to_int_map(legacy_raw, "tick_write.ppa_map")

        # frames: por padrão exige que você defina explicitamente,
        # mas para não quebrar execução, criamos um placeholder igual ao latency.
        # Melhor: você trocar no YAML para um mapa real de frames.
        ppa_map_frames = dict(ppa_map_latency)

    else:
        if latency_raw is None:
            raise ValueError("Config inválida: campo obrigatório 'tick_write.ppa_map_latency' ausente.")
        if frames_raw is None:
            raise ValueError("Config inválida: campo obrigatório 'tick_write.ppa_map_frames' ausente.")

        ppa_map_latency = _to_int_map(latency_raw, "tick_write.ppa_map_latency")
        ppa_map_frames = _to_int_map(frames_raw, "tick_write.ppa_map_frames")

    tick_write = TickWriteConfig(
        url=str(url),
        server_ip=str(server_ip),
        workers=workers,
        queue_max=queue_max,
        timeout_sec=timeout_sec,
        max_retries=max_retries,
        drop_on_full=drop_on_full,
        ppa_map_latency=ppa_map_latency,
        ppa_map_frames=ppa_map_frames,
    )

    return AppConfig(
        hostname=str(hostname),
        port=port,
        window_sec=window_sec,
        top_n=top_n,
        shards=shards,
        queue_size=queue_size,
        subscription=subscription,
        tick_write=tick_write,
    )
