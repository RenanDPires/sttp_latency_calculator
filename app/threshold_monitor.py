from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from domain.thresholds import ThresholdRule
from domain.violations import ViolationEvent


@dataclass
class ThresholdMonitorConfig:
    cooldown_sec: float = 0.0


class ThresholdMonitor:
    """
    Avalia regras por PPA em tempo real.
    Não escreve nada: apenas gera ViolationEvent.
    """

    def __init__(
        self,
        rules_by_ppa: Dict[int, List[ThresholdRule]],
        cfg: ThresholdMonitorConfig | None = None,
    ):
        self._rules_by_ppa = {int(k): list(v) for k, v in rules_by_ppa.items()}
        self._cfg = cfg or ThresholdMonitorConfig()
        self._last_emit: Dict[Tuple[int, str], float] = {}

    def check(self, now_epoch: float, ppa: int, value: float) -> List[ViolationEvent]:
        rules = self._rules_by_ppa.get(int(ppa))
        if not rules:
            return []

        out: List[ViolationEvent] = []
        for r in rules:
            if not r.violated(value):
                continue

            key = (int(ppa), r.rule_id)
            if self._cfg.cooldown_sec > 0:
                last = self._last_emit.get(key)
                if last is not None and (now_epoch - last) < self._cfg.cooldown_sec:
                    continue
                self._last_emit[key] = now_epoch

            out.append(
                ViolationEvent(
                    t_epoch=now_epoch,
                    ppa=int(ppa),
                    value=float(value),
                    rule_id=r.rule_id,
                    rule=r.label(),
                )
            )

        return out
