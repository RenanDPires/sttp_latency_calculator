from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
import math

Op = Literal[">", "<", ">=", "<=", "==", "!="]


@dataclass(frozen=True)
class ThresholdRule:
    op: Op
    value: float
    rule_id: str
    atol: float = 0.0  # tolerância para comparações float

    def violated(self, x: float) -> bool:
        if self.op == ">":
            return x > self.value
        if self.op == "<":
            return x < self.value
        if self.op == ">=":
            return x >= self.value
        if self.op == "<=":
            return x <= self.value
        if self.op == "==":
            return math.isclose(x, self.value, abs_tol=self.atol) if self.atol else x == self.value
        if self.op == "!=":
            return not math.isclose(x, self.value, abs_tol=self.atol) if self.atol else x != self.value
        return False

    def label(self) -> str:
        return f"{self.op} {self.value}"
