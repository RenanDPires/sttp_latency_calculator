from __future__ import annotations

import csv
import os
import threading
import time
from datetime import datetime, timezone
from queue import Queue, Full, Empty
from typing import Any, Dict, Optional


class AsyncCsvRawMeasurementWriter:
    """
    Escreve o conteúdo bruto de cada measurement recebido em CSV.
    """

    def __init__(
        self,
        csv_path: str,
        *,
        queue_max: int = 50000,
        drop_on_full: bool = True,
        flush_every_n: int = 500,
        flush_every_sec: float = 2.0,
    ):
        self.csv_path = csv_path
        self.drop_on_full = drop_on_full
        self.flush_every_n = flush_every_n
        self.flush_every_sec = flush_every_sec

        self._q: Queue[Dict[str, Any]] = Queue(maxsize=queue_max)
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._worker, daemon=True)

    def start(self) -> None:
        self._t.start()

    def stop(self) -> None:
        self._stop.set()
        self._t.join(timeout=5)

    def publish(self, record: Dict[str, Any]) -> None:
        try:
            self._q.put_nowait(record)
        except Full:
            if not self.drop_on_full:
                self._q.put(record)

    @staticmethod
    def _fmt_epoch(epoch: float) -> str:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _ensure_header(self, columns: list[str]) -> None:
        if not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0:
            with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(columns)

    def _flush(self, batch: list[Dict[str, Any]]) -> None:
        if not batch:
            return

        columns = list(batch[0].keys())
        self._ensure_header(columns)

        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for row in batch:
                w.writerow([row.get(col) for col in columns])

    def _worker(self) -> None:
        batch: list[Dict[str, Any]] = []
        last_flush = time.time()

        while not self._stop.is_set():
            try:
                record = self._q.get(timeout=0.2)
                batch.append(record)
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


def build_raw_measurement_record(
    *,
    arrival_epoch: float,
    measurement: Any,
    metadata: Optional[Any],
    ppa_key: Optional[int],
) -> Dict[str, Any]:
    measurement_dict = None
    metadata_dict = None

    try:
        measurement_dict = dict(vars(measurement))
    except Exception:
        measurement_dict = None

    try:
        metadata_dict = dict(vars(metadata)) if metadata is not None else None
    except Exception:
        metadata_dict = None

    return {
        "utc_arrival": AsyncCsvRawMeasurementWriter._fmt_epoch(arrival_epoch),
        "arrival_epoch": f"{arrival_epoch:.6f}",
        "ppa_key": ppa_key,
        "measurement_datetime_raw": repr(getattr(measurement, "datetime", None)),
        "measurement_value_raw": repr(getattr(measurement, "value", None)),
        "measurement_flags_raw": repr(getattr(measurement, "flags", None)),
        "measurement_signalid_raw": repr(getattr(measurement, "signalid", None)),
        "measurement_repr": repr(measurement),
        "measurement_dict": repr(measurement_dict),
        "metadata_repr": repr(metadata),
        "metadata_dict": repr(metadata_dict),
    }
