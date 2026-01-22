from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from domain.thresholds import ThresholdRule


@dataclass(frozen=True)
class TickWriteConfig:
    url: str
    server_ip: str

    workers: int = 4
    queue_max: int = 5000
    timeout_sec: float = 2.0
    max_retries: int = 3
    drop_on_full: bool = False

    ppa_map_latency: dict[int, int] = None  # type: ignore
    ppa_map_frames: dict[int, int] = None   # type: ignore


@dataclass(frozen=True)
class ThresholdMonitorConfig:
    enabled: bool = False

    csv_path: str = "violations.csv"
    queue_max: int = 20000
    drop_on_full: bool = True
    flush_every_n: int = 200
    flush_every_sec: float = 2.0
    cooldown_sec: float = 0.0

    # dict[ppa_in] -> list[ThresholdRule]
    rules: dict[int, list[ThresholdRule]] = None  # type: ignore


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
    threshold_monitor: ThresholdMonitorConfig | None = None


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


def _to_rules_map(x: Any, path: str) -> dict[int, list[ThresholdRule]]:
    """
    Espera:
      threshold_monitor:
        rules:
          9999:
            - op: ">"
              value: 0
              rule_id: "PPA9999_GT0"
              atol: 0.0   # opcional
    """
    if x is None:
        return {}
    if not isinstance(x, Mapping):
        raise ValueError(f"Config inválida: '{path}' deve ser um mapa (dict).")

    out: dict[int, list[ThresholdRule]] = {}

    for ppa_k, rules_list in x.items():
        ppa = int(ppa_k)

        if not isinstance(rules_list, list):
            raise ValueError(f"Config inválida: '{path}.{ppa_k}' deve ser uma lista de regras.")

        parsed: list[ThresholdRule] = []
        for i, r in enumerate(rules_list):
            if not isinstance(r, Mapping):
                raise ValueError(f"Config inválida: '{path}.{ppa_k}[{i}]' deve ser um objeto.")

            op = r.get("op")
            val = r.get("value")
            rule_id = r.get("rule_id")
            atol = float(r.get("atol", 0.0))

            if op is None or val is None or rule_id is None:
                raise ValueError(
                    f"Config inválida: '{path}.{ppa_k}[{i}]' precisa de op, value, rule_id."
                )

            parsed.append(
                ThresholdRule(
                    op=str(op),          # validação final acontece no uso
                    value=float(val),
                    rule_id=str(rule_id),
                    atol=atol,
                )
            )

        out[ppa] = parsed

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

    # ---- tick_write (opcional) ----
    tw_raw = _opt(data, "tick_write", None)
    tick_write = None

    tw_enabled = bool(_opt(tw_raw, "enabled", True))  # default True p/ manter comportamento antigo

    if isinstance(tw_raw, Mapping):
        if tw_enabled:
            url = _req(tw_raw, "url")
            server_ip = _req(tw_raw, "server_ip")

            workers = int(_opt(tw_raw, "workers", 4))
            queue_max = int(_opt(tw_raw, "queue_max", 5000))
            timeout_sec = float(_opt(tw_raw, "timeout_sec", 2.0))
            max_retries = int(_opt(tw_raw, "max_retries", 3))
            drop_on_full = bool(_opt(tw_raw, "drop_on_full", False))

            latency_raw = _opt(tw_raw, "ppa_map_latency", None)
            frames_raw = _opt(tw_raw, "ppa_map_frames", None)
            legacy_raw = _opt(tw_raw, "ppa_map", None)

            if latency_raw is None and frames_raw is None and legacy_raw is not None:
                ppa_map_latency = _to_int_map(legacy_raw, "tick_write.ppa_map")
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


    # ---- threshold_monitor (opcional) ----
    tm_raw = _opt(data, "threshold_monitor", None)
    threshold_monitor = None

    if isinstance(tm_raw, Mapping):
        enabled = bool(_opt(tm_raw, "enabled", False))
        csv_path = str(_opt(tm_raw, "csv_path", "violations.csv"))
        tm_queue_max = int(_opt(tm_raw, "queue_max", 20000))
        tm_drop = bool(_opt(tm_raw, "drop_on_full", True))
        flush_n = int(_opt(tm_raw, "flush_every_n", 200))
        flush_s = float(_opt(tm_raw, "flush_every_sec", 2.0))
        cooldown = float(_opt(tm_raw, "cooldown_sec", 0.0))

        rules_raw = _opt(tm_raw, "rules", None)
        rules = _to_rules_map(rules_raw, "threshold_monitor.rules")

        threshold_monitor = ThresholdMonitorConfig(
            enabled=enabled,
            csv_path=csv_path,
            queue_max=tm_queue_max,
            drop_on_full=tm_drop,
            flush_every_n=flush_n,
            flush_every_sec=flush_s,
            cooldown_sec=cooldown,
            rules=rules,
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
        threshold_monitor=threshold_monitor,
    )
