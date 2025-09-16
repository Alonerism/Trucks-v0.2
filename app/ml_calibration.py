"""Lightweight ETA calibration using linear regression.

Collects (features -> actual_duration_seconds) pairs and trains a simple
sklearn-style LinearRegression model. Starts as a stub to keep things simple.
"""
from __future__ import annotations
from typing import List, Tuple, Optional

try:
    from sklearn.linear_model import LinearRegression
    import numpy as np
except Exception:  # optional dependency
    LinearRegression = None  # type: ignore
    np = None  # type: ignore


class EtaCalibrator:
    def __init__(self) -> None:
        self.model = LinearRegression() if LinearRegression else None
        self._X: List[List[float]] = []
        self._y: List[float] = []

    def add_observation(self, est_sec: float, service_min: float, actual_sec: float) -> None:
        if np is None or self.model is None:
            return
        self._X.append([est_sec, service_min])
        self._y.append(actual_sec)

    def train(self) -> None:
        if np is None or self.model is None:
            return
        if not self._X:
            return
        X = np.array(self._X, dtype=float)
        y = np.array(self._y, dtype=float)
        self.model.fit(X, y)

    def predict(self, est_sec: float, service_min: float) -> Optional[float]:
        if np is None or self.model is None or not self._X:
            return None
        X = np.array([[est_sec, service_min]], dtype=float)
        return float(self.model.predict(X)[0])
