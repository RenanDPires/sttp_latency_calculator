from time import time
from core import Clock

# epoch seconds (float)
class SystemClock(Clock):
    def now_epoch(self) -> float:
        return time()
