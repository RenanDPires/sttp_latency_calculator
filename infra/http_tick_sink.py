from __future__ import annotations

import queue
import threading
import time
from dataclasses import asdict
from typing import Optional

import httpx

from domain.ports import TickSink, WriteJob


class HttpTickSink(TickSink):
    def __init__(
        self,
        url: str,
        *,
        workers: int = 4,
        queue_max: int = 5000,
        timeout_sec: float = 2.0,
        max_retries: int = 3,
        drop_on_full: bool = False,
    ) -> None:
        self._url = url
        self._timeout = timeout_sec
        self._max_retries = max_retries
        self._drop_on_full = drop_on_full

        self._q: queue.Queue[WriteJob | _Stop] = queue.Queue(maxsize=queue_max)
        self._workers = workers
        self._threads: list[threading.Thread] = []
        self._client: Optional[httpx.Client] = None
        self._started = False

        # métricas simples (opcional)
        self.total_published = 0
        self.total_dropped = 0
        self.total_failed = 0
        self.total_sent = 0

    def start(self) -> None:
        if self._started:
            return
        self._client = httpx.Client(timeout=self._timeout)
        self._threads = []
        for i in range(self._workers):
            t = threading.Thread(target=self._worker, args=(i,), daemon=True)
            t.start()
            self._threads.append(t)
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        # sinaliza parada
        for _ in self._threads:
            self._q.put(_Stop())
        # espera threads
        for t in self._threads:
            t.join(timeout=3)
        self._threads.clear()
        self._started = False
        if self._client:
            self._client.close()
            self._client = None

    def publish(self, job: WriteJob) -> None:
        if not self._started:
            raise RuntimeError("HttpTickSink.publish chamado antes de start()")

        self.total_published += 1

        if self._drop_on_full:
            try:
                self._q.put_nowait(job)
            except queue.Full:
                self.total_dropped += 1
        else:
            self._q.put(job)

    def _worker(self, wid: int) -> None:
        assert self._client is not None

        while True:
            item = self._q.get()
            try:
                if isinstance(item, _Stop):
                    return

                payload = asdict(item)

                attempt = 0
                while True:
                    try:
                        r = self._client.post(self._url, json=payload)
                        r.raise_for_status()
                        self.total_sent += 1
                        break
                    except Exception:
                        attempt += 1
                        if attempt > self._max_retries:
                            self.total_failed += 1
                            break
                        time.sleep(min(0.25 * (2 ** (attempt - 1)), 2.0))
            finally:
                self._q.task_done()


class _Stop:
    pass
