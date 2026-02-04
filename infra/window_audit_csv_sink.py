from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from typing import List

from core import LatencyAuditEvent


class WindowCsvAuditWriter:
    """
    Grava um CSV por janela quando solicitado.
    """

    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    @staticmethod
    def _fmt_epoch(epoch: float) -> str:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    @staticmethod
    def _fmt_filename(epoch: float) -> str:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        return dt.strftime("%Y%m%d_%H%M%S")

    def write_window(
        self,
        window_start_epoch: float,
        window_end_epoch: float,
        events: List[LatencyAuditEvent],
    ) -> None:
        if not events:
            return

        os.makedirs(self.output_dir, exist_ok=True)
        start_tag = self._fmt_filename(window_start_epoch)
        end_tag = self._fmt_filename(window_end_epoch)
        filename = f"window_{start_tag}_to_{end_tag}.csv"
        path = os.path.join(self.output_dir, filename)

        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "window_start_utc",
                "window_end_utc",
                "ppa",
                "utc_measurement",
                "utc_arrival",
                "delta_ms",
                "value",
                "flags",
            ])
            window_start_str = self._fmt_epoch(window_start_epoch)
            window_end_str = self._fmt_epoch(window_end_epoch)
            for ev in events:
                w.writerow([
                    window_start_str,
                    window_end_str,
                    ev.ppa,
                    self._fmt_epoch(ev.t_meas_epoch),
                    self._fmt_epoch(ev.t_arrival_epoch),
                    f"{ev.latency_ms:.3f}",
                    ev.value,
                    ev.flags,
                ])
