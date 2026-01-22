from __future__ import annotations

import csv
import os
import threading
import time
from queue import Queue, Full, Empty
from datetime import datetime, timezone
from typing import List

from domain.violations import ViolationEvent


class AsyncCsvViolationWriter:
    """
    Escrita assíncrona de violações em CSV.
    Não bloqueia o caminho crítico.
    """

    def __init__(
        self,
        csv_path: str,
        *,
        queue_max: int = 20000,
        drop_on_full: bool = True,
        flush_every_n: int = 200,
        flush_every_sec: float = 2.0,
    ):
        self.csv_path = csv_path
        self.drop_on_full = drop_on_full
        self.flush_every_n = flush_every_n
        self.flush_every_sec = flush_every_sec

        self._q: Queue[ViolationEvent] = Queue(maxsize=queue_max)
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._worker, daemon=True)

    def start(self) -> None:
        self._t.start()

    def stop(self) -> None:
        self._stop.set()
        self._t.join(timeout=5)

    def publish(self, ev: ViolationEvent) -> None:
        try:
            self._q.put_nowait(ev)
        except Full:
            if not self.drop_on_full:
                self._q.put(ev)

    @staticmethod
    def _fmt_epoch(epoch: float) -> str:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _ensure_header(self) -> None:
        if not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0:
            with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(
                    ["utc_time", "ppa", "value", "rule_id", "rule"]
                )

    def _flush(self, batch: List[ViolationEvent]) -> None:
        if not batch:
            return
        self._ensure_header()
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for ev in batch:
                w.writerow([
                    self._fmt_epoch(ev.t_epoch),
                    ev.ppa,
                    ev.value,
                    ev.rule_id,
                    ev.rule,
                ])

    def _worker(self) -> None:
        batch: List[ViolationEvent] = []
        last_flush = time.time()

        while not self._stop.is_set():
            try:
                ev = self._q.get(timeout=0.2)
                batch.append(ev)
            except Empty:
                pass

            now = time.time()
            if batch and (
                len(batch) >= self.flush_every_n
                or (now - last_flush) >= self.flush_every_sec
            ):
                self._flush(batch)
                batch.clear()
                last_flush = now

        if batch:
            self._flush(batch)
