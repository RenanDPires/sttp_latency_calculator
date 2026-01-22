from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class MeasurementEvent:
    """
    Evento de MEDIDA (não estatístico).
    Não entra no pipeline de latência.
    """
    ppa: int
    value: float
    t_meas_epoch: float
    t_arrival_epoch: float


@dataclass(frozen=True)
class ViolationEvent:
    """
    Evento de violação detectada.
    """
    t_epoch: float
    ppa: int
    value: float
    rule_id: str
    rule: str
