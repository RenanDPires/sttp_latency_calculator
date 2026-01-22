from dataclasses import dataclass
from typing import Mapping, Optional
from domain.ports import PpaMapper, PpaDestinations

class DictPpaMapper(PpaMapper):
    def __init__(
        self,
        latency_map: Mapping[int, int],
        frames_map: Mapping[int, int],
    ):
        self._lat = {int(k): int(v) for k, v in latency_map.items()}
        self._frm = {int(k): int(v) for k, v in frames_map.items()}

    def try_map(self, ppa_in: int) -> Optional[PpaDestinations]:
        ppa_in = int(ppa_in)
        lat = self._lat.get(ppa_in)
        frm = self._frm.get(ppa_in)
        if lat is None or frm is None:
            return None
        return PpaDestinations(latency_ppa=int(lat), frames_ppa=int(frm))
