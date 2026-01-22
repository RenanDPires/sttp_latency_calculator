from __future__ import annotations

import time
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Tuple, Any


@dataclass
class PpaStats:
    count: int = 0
    sum_latency_ms: float = 0.0

    @property
    def mean_latency_ms(self) -> float:
        return (self.sum_latency_ms / self.count) if self.count else 0.0


class AlignedWindowBuffer:
    """
    - Inicia a janela no próximo segundo cheio (xx:xx:xx.00)
    - Por window_sec, acumula stats e bufferiza itens.
    - Ao final, libera o flush e passa a operar em "passthrough".
    """

    def __init__(self, window_sec: int):
        self.window_sec = int(window_sec)

        self._lock = threading.Lock()
        self._buffer: Deque[Tuple[int, Any]] = deque()  # (ppa_out, payload)
        self._stats: Dict[int, PpaStats] = defaultdict(PpaStats)

        self._start_epoch = self._next_whole_second_epoch()
        self._end_epoch = self._start_epoch + self.window_sec

        self._closed = False
        self._ready = threading.Event()

        t = threading.Thread(target=self._timer_thread, daemon=True)
        t.start()

    @staticmethod
    def _next_whole_second_epoch() -> float:
        now = time.time()
        return float(int(now) + 1)

    @property
    def start_epoch(self) -> float:
        return self._start_epoch

    @property
    def end_epoch(self) -> float:
        return self._end_epoch

    def is_ready(self) -> bool:
        return self._ready.is_set()

    def push(
        self,
        now_epoch: float,
        ppa_in: int,
        latency_ms: float,
        ppa_out: int,
        payload: Any,
    ) -> Optional[Tuple[int, Any]]:
        """
        Retorna:
          - None: item foi bufferizado (janela ativa) OU ainda não chegou na janela
          - (ppa_out, payload): item deve seguir direto (janela encerrada)
        """
        with self._lock:
            # antes da janela começar: não conta e não bufferiza (alinhamento estrito)
            if now_epoch < self._start_epoch:
                return None

            # durante a janela: acumula + bufferiza
            if now_epoch < self._end_epoch and not self._closed:
                st = self._stats[ppa_in]
                st.count += 1
                st.sum_latency_ms += float(latency_ms)
                self._buffer.append((int(ppa_out), payload))
                return None

            # após a janela: passthrough
            return (int(ppa_out), payload)

    def close_and_get_report(self) -> Dict[int, PpaStats]:
        """
        Força fechamento (idempotente) e devolve snapshot de stats.
        """
        with self._lock:
            self._closed = True
            snap = dict(self._stats)
            self._ready.set()
            return snap

    def pop_buffered(self) -> Optional[Tuple[int, Any]]:
        """
        Retira um item do buffer (para flush após o fechamento).
        """
        with self._lock:
            if self._buffer:
                return self._buffer.popleft()
            return None

    def _timer_thread(self) -> None:
        # Espera passar o fim da janela e fecha automaticamente
        while True:
            now = time.time()
            if now >= self._end_epoch:
                self.close_and_get_report()
                return
            time.sleep(0.01)
