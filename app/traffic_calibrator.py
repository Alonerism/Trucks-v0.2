"""Traffic calibrator providing hour-of-day multiplicative factors.

Learns simple multiplicative corrections mapping offline (straight-line / heuristic)
travel seconds to expected road travel seconds. Starts with seeded defaults when
insufficient historical data.
"""
from __future__ import annotations
from typing import Dict, List, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import math
import time as _time

# Seeded defaults by hour bucket (start inclusive, end exclusive)
SEEDED_FACTORS = [
    ((6,7), 0.95),
    ((7,9), 1.35),
    ((9,15), 1.10),
    ((15,18), 1.30),
    ((18,21), 1.10),
    ((21,24), 0.90),
    ((0,6), 0.90),
]

def hour_factor_default(hour: int) -> float:
    for (start,end), val in SEEDED_FACTORS:
        # handle wrap past midnight implicitly
        if start <= hour < end or (start > end and (hour >= start or hour < end)):
            return val
    return 1.10

@dataclass
class CalibrationSample:
    offline_seconds: float
    road_seconds: float
    hour: int
    area: str = "global"
    ts: float = field(default_factory=_time.time)

class TrafficCalibrator:
    """In-memory calibrator with exponential moving average per (area,hour)."""
    def __init__(self, alpha: float = 0.2):
        self.alpha = alpha
        self.factors: Dict[Tuple[str,int], float] = {}
        self.samples: List[CalibrationSample] = []

    def ingest(self, offline_seconds: float, road_seconds: float, hour: int, area: str = "global"):
        if offline_seconds <= 0 or road_seconds <= 0:
            return
        ratio = road_seconds / offline_seconds
        key = (area, hour)
        prev = self.factors.get(key, hour_factor_default(hour))
        updated = (1 - self.alpha) * prev + self.alpha * ratio
        self.factors[key] = max(0.2, min(5.0, updated))
        self.samples.append(CalibrationSample(offline_seconds, road_seconds, hour, area))

    def factor(self, hour: int, area: str = "global") -> float:
        return self.factors.get((area,hour), hour_factor_default(hour))

    def apply(self, offline_seconds: float, hour: int, area: str = "global") -> float:
        return offline_seconds * self.factor(hour, area)

# Global singleton (simple)
_calibrator = TrafficCalibrator()

def get_calibrator() -> TrafficCalibrator:
    return _calibrator
