from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Mapping


# -----------------------------
# Tick write config (src -> dst)
# -----------------------------

@dataclass(frozen=True)
class TickWriteConfig:
    url: str
    server_ip: str
    ppa_map: Mapping[int, int]  # src -> dst

    workers: int = 4
    queue_max: int = 5000
    timeout_sec: float = 2.0
    max_retries: int = 3
    drop_on_full: bool = False


# fallback local (se quiser manter)
DEFAULT_PPA_MAP: dict[int, int] = {
    477: 2397,
    886: 2398,
}


@dataclass(frozen=True)
class AppConfig:
    hostname: str
    port: int

    # Agora é gerado automaticamente (srcs do ppa_map)
    subscription: str

    window_sec: float
    shards: int
    queue_size: int
    top_n: int

    tick_write: TickWriteConfig


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return default if v is None or v.strip() == "" else int(v)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    return default if v is None or v.strip() == "" else float(v)


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _load_ppa_map_env_or_default(default_map: Mapping[int, int]) -> dict[int, int]:
    """
    Lê TICK_WRITE_PPA_MAP (JSON dict). Se não existir, usa default_map.
    """
    raw = os.getenv("TICK_WRITE_PPA_MAP", "").strip()
    if not raw:
        return dict(default_map)

    data = json.loads(raw)
    if not isinstance(data, dict):
        raise SystemExit("TICK_WRITE_PPA_MAP deve ser um JSON dict, ex: {\"477\":2397}")

    out: dict[int, int] = {}
    for k, v in data.items():
        out[int(k)] = int(v)
    return out


def _build_subscription_from_map(ppa_map: Mapping[int, int]) -> str:
    # ordena pra ser estável
    srcs = sorted(int(k) for k in ppa_map.keys())
    # formato compatível com seu default: "PPA:477; PPA:479; ..."
    return "; ".join(f"PPA:{ppa}" for ppa in srcs)


def _parse_subscription_ppas(subscription: str) -> set[int]:
    """
    Extrai os PPAs de um texto estilo: "PPA:477; PPA:479; PPA:931"
    Sem regex, simples e robusto.
    """
    out: set[int] = set()
    for part in subscription.split(";"):
        part = part.strip()
        if not part:
            continue
        # aceita "PPA:477" ou "PPA: 477"
        if ":" in part:
            _, tail = part.split(":", 1)
            tail = tail.strip()
            if tail.isdigit():
                out.add(int(tail))
    return out


def load_config() -> AppConfig:
    # -----------------------------
    # Base (latência)
    # -----------------------------
    hostname = os.getenv("HOSTNAME", "localhost")
    port = int(os.getenv("PORT", "7165"))

    window_sec = float(os.getenv("WINDOW_SEC", "1.0"))
    top_n = int(os.getenv("TOP_N", "10"))

    default_shards = max(4, min(32, (os.cpu_count() or 4) * 2))
    shards = int(os.getenv("SHARDS", str(default_shards)))
    queue_size = int(os.getenv("QUEUE_SIZE", "100000"))

    # -----------------------------
    # Tick write (sempre ligado)
    # -----------------------------
    tick_url = os.getenv("TICK_WRITE_URL", "http://localhost:8010/write")
    tick_server_ip = os.getenv("TICK_WRITE_SERVER_IP", hostname)

    tick_workers = _env_int("TICK_WRITE_WORKERS", 4)
    tick_queue_max = _env_int("TICK_WRITE_QUEUE_MAX", 5000)
    tick_timeout = _env_float("TICK_WRITE_TIMEOUT_SEC", 2.0)
    tick_retries = _env_int("TICK_WRITE_MAX_RETRIES", 3)
    tick_drop = _env_bool("TICK_WRITE_DROP_ON_FULL", False)

    tick_ppa_map = _load_ppa_map_env_or_default(DEFAULT_PPA_MAP)

    # Fail-fast: sem mapa não tem como saber o que calcular nem onde salvar
    if not tick_ppa_map:
        raise SystemExit("ppa_map vazio. Defina TICK_WRITE_PPA_MAP ou preencha DEFAULT_PPA_MAP.")

    # subscription gerada a partir do mapa (reduz erro humano)
    subscription_auto = _build_subscription_from_map(tick_ppa_map)

    # Se o usuário definiu SUBSCRIPTION manualmente, validamos 100% contra o mapa:
    subscription_env = os.getenv("SUBSCRIPTION", "").strip()
    if subscription_env:
        ppas_env = _parse_subscription_ppas(subscription_env)
        ppas_map = set(int(k) for k in tick_ppa_map.keys())
        if ppas_env != ppas_map:
            raise SystemExit(
                "SUBSCRIPTION não bate com as chaves de TICK_WRITE_PPA_MAP.\n"
                f"SUBSCRIPTION={sorted(ppas_env)}\n"
                f"PPA_MAP_KEYS={sorted(ppas_map)}\n"
                "Remova SUBSCRIPTION do env ou ajuste para coincidir exatamente."
            )
        subscription = subscription_env
    else:
        subscription = subscription_auto

    tick_write = TickWriteConfig(
        url=tick_url,
        server_ip=tick_server_ip,
        ppa_map=tick_ppa_map,
        workers=tick_workers,
        queue_max=tick_queue_max,
        timeout_sec=tick_timeout,
        max_retries=tick_retries,
        drop_on_full=tick_drop,
    )

    return AppConfig(
        hostname=hostname,
        port=port,
        subscription=subscription,
        window_sec=window_sec,
        shards=shards,
        queue_size=queue_size,
        top_n=top_n,
        tick_write=tick_write,
    )
