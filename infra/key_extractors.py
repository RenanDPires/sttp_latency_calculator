from __future__ import annotations
from domain.ports import KeyExtractor
from sttp.transport.measurement import Measurement

class PpaKeyExtractor(KeyExtractor):
    def key_from(self, measurement: Measurement, metadata) -> int:
        # metadata.id aparece como "PPA"
        try:
            return int(metadata.id)
        except Exception:
            s = str(metadata.id)
            return int("".join(ch for ch in s if ch.isdigit()) or "0")


