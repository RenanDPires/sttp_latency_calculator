from typing import Mapping, Any

def _build_subscription_from_map(ppa_map: Mapping[int, int]) -> str:
    srcs = sorted(int(k) for k in ppa_map.keys())
    return "; ".join(f"PPA:{ppa}" for ppa in srcs)