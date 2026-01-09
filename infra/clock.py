from time import time
from domain.ports import Clock

class SystemClock(Clock):
    def now_epoch(self) -> float:
        return time()
