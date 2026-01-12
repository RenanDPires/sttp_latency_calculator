from __future__ import annotations
import threading
from queue import Queue, Full
from typing import Dict, List, Tuple

from domain.models import LatencyEvent, WindowStats, WindowRow
from domain.services import latency_ms

class ShardedWindowProcessor:
    def __init__(self, shards: int, queue_size: int):
        self.shards = shards
        self.queues = [Queue(maxsize=queue_size) for _ in range(shards)]
        self.stop = threading.Event()
        self.threads: List[threading.Thread] = []

        self._locks = [threading.Lock() for _ in range(shards)]
        self._wstats: List[Dict[int, WindowStats]] = [dict() for _ in range(shards)]

        self._tot_lock = threading.Lock()
        self.total_enqueued = 0
        self.total_dropped = 0
        self.total_processed = 0

    @staticmethod
    def _shard_of(key: int, shards: int) -> int:
        return (key * 2654435761) % shards

    def start(self) -> None:
        for i in range(self.shards):
            t = threading.Thread(target=self._worker, args=(i,), daemon=True)
            t.start()
            self.threads.append(t)

    def submit(self, ev: LatencyEvent) -> bool:
        shard = self._shard_of(ev.key, self.shards)
        try:
            self.queues[shard].put_nowait(ev)
            with self._tot_lock:
                self.total_enqueued += 1
            return True
        except Full:
            with self._locks[shard]:
                st = self._wstats[shard].get(ev.key)
                if st is None:
                    st = WindowStats()
                    self._wstats[shard][ev.key] = st
                st.dropped += 1
            with self._tot_lock:
                self.total_dropped += 1
            return False

    def _worker(self, shard_idx: int) -> None:
        q = self.queues[shard_idx]
        d = self._wstats[shard_idx]
        lock = self._locks[shard_idx]

        while not self.stop.is_set():
            ev = q.get()
            if ev is None:
                return
            try:
                lm = latency_ms(ev.t_arrival_epoch, ev.t_meas_epoch)
                with lock:
                    st = d.get(ev.key)
                    if st is None:
                        st = WindowStats()
                        d[ev.key] = st
                    st.add(lm)
                with self._tot_lock:
                    self.total_processed += 1
            finally:
                q.task_done()

    def snapshot_and_reset(self) -> List[WindowRow]:
        rows: List[WindowRow] = []
        for i in range(self.shards):
            with self._locks[i]:
                d = self._wstats[i]
                for key, st in d.items():
                    if st.count == 0 and st.dropped == 0:
                        continue
                    rows.append(WindowRow(
                        key=key,
                        count=st.count,
                        mean_ms=st.mean_ms,
                        max_ms=st.max_ms,
                        last_ms=st.last_ms,
                        dropped=st.dropped,
                    ))
                d.clear()
        return rows

    def totals(self) -> Tuple[int, int, int]:
        with self._tot_lock:
            return self.total_enqueued, self.total_processed, self.total_dropped

    def shutdown(self) -> None:
        self.stop.set()
        for q in self.queues:
            q.put(None)

