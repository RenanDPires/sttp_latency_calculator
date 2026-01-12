from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Mapping, Any
from infra.sttp_subscriber import _build_subscription_from_map
import yaml

@dataclass(frozen=True)
class TickWriteConfig:
    url: str
    server_ip: str
    ppa_map: Mapping[int, int]
    workers: int
    queue_max: int
    timeout_sec: float
    max_retries: int
    drop_on_full: bool


@dataclass(frozen=True)
class AppConfig:
    hostname: str
    port: int
    subscription: str
    window_sec: float
    shards: int
    queue_size: int
    top_n: int
    tick_write: TickWriteConfig


def _require(obj: dict, key: str, ctx: str):
    if key not in obj:
        raise SystemExit(f"Config inválida: campo obrigatório '{ctx}.{key}' ausente.")
    return obj[key]


def _coerce_ppa_map(obj: Any) -> dict[int, int]:
    if not isinstance(obj, dict) or not obj:
        raise SystemExit("tick_write.ppa_map deve ser um dict não vazio (src -> dst).")
    out: dict[int, int] = {}
    for k, v in obj.items():
        try:
            out[int(k)] = int(v)
        except Exception:
            raise SystemExit(f"ppa_map inválido: chave={k}, valor={v}")
    return out





def _app_dir() -> str:
    """
    Diretório 'real' do app:
    - em PyInstaller: sys.executable
    - em Python normal: arquivo atual
    """
    if getattr(sys, "frozen", False):  # PyInstaller
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def _find_config_path() -> str:
    tried: list[str] = []

    env_path = os.getenv("CONFIG_PATH", "").strip()
    if env_path:
        tried.append(env_path)
        if os.path.exists(env_path):
            return env_path

    cwd = os.getcwd()
    candidates_cwd = [
        os.path.join(cwd, "config.yaml"),
        os.path.join(cwd, "config.yml"),
        os.path.join(cwd, "config.json"),
    ]
    for p in candidates_cwd:
        tried.append(p)
        if os.path.exists(p):
            return p

    ad = _app_dir()
    candidates_app = [
        os.path.join(ad, "config.yaml"),
        os.path.join(ad, "config.yml"),
        os.path.join(ad, "config.json"),
    ]
    for p in candidates_app:
        tried.append(p)
        if os.path.exists(p):
            return p

    msg = "Arquivo de configuração não encontrado. Procurei:\n" + "\n".join(f" - {p}" for p in tried)
    raise SystemExit(msg)


def _load_file_config(path: str) -> dict[str, Any]:
    ext = os.path.splitext(path)[1].lower()
    with open(path, "r", encoding="utf-8") as f:
        if ext in (".yaml", ".yml"):
            data = yaml.safe_load(f) or {}
        elif ext == ".json":
            data = json.load(f) or {}
        else:
            raise SystemExit("Config file deve ser .yaml/.yml ou .json")
    if not isinstance(data, dict):
        raise SystemExit("Config inválida: raiz deve ser um objeto.")
    return data


def load_config() -> AppConfig:
    cfg_path = _find_config_path()
    cfg = _load_file_config(cfg_path)

    # ---- base ----
    hostname = _require(cfg, "hostname", "root")
    port = int(_require(cfg, "port", "root"))
    window_sec = float(_require(cfg, "window_sec", "root"))
    top_n = int(_require(cfg, "top_n", "root"))
    shards = int(_require(cfg, "shards", "root"))
    queue_size = int(_require(cfg, "queue_size", "root"))

    # ---- tick_write ----
    tw = _require(cfg, "tick_write", "root")
    if not isinstance(tw, dict):
        raise SystemExit("tick_write deve ser um objeto.")

    tick_url = _require(tw, "url", "tick_write")
    tick_server_ip = _require(tw, "server_ip", "tick_write")
    tick_workers = int(_require(tw, "workers", "tick_write"))
    tick_queue_max = int(_require(tw, "queue_max", "tick_write"))
    tick_timeout = float(_require(tw, "timeout_sec", "tick_write"))
    tick_retries = int(_require(tw, "max_retries", "tick_write"))
    tick_drop = bool(_require(tw, "drop_on_full", "tick_write"))
    tick_ppa_map = _coerce_ppa_map(_require(tw, "ppa_map", "tick_write"))

    subscription = _build_subscription_from_map(tick_ppa_map)

    return AppConfig(
        hostname=hostname,
        port=port,
        subscription=subscription,
        window_sec=window_sec,
        shards=shards,
        queue_size=queue_size,
        top_n=top_n,
        tick_write=TickWriteConfig(
            url=tick_url,
            server_ip=tick_server_ip,
            ppa_map=tick_ppa_map,
            workers=tick_workers,
            queue_max=tick_queue_max,
            timeout_sec=tick_timeout,
            max_retries=tick_retries,
            drop_on_full=tick_drop,
        ),
    )
