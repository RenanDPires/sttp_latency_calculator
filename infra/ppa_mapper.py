from __future__ import annotations
from typing import Mapping, Optional
from domain.ports import PpaMapper

class DictPpaMapper(PpaMapper):
    def __init__(self, mapping: Mapping[int, int]) -> None:
        self._m = dict(mapping)

    def try_map(self, ppa_src: int) -> Optional[int]:
        return self._m.get(ppa_src)
