from __future__ import annotations
from typing import Optional

class EtaCalibrator:
  """Optional ETA calibrator stub.
  Collect (estimated_seconds, actual_seconds, hour, dow) features and retrain periodically.
  No-op unless explicitly wired in.
  """
  def __init__(self, enabled: bool = True, min_batches: int = 10, retrain_every: int = 10):
    self.enabled = enabled
    self.min_batches = min_batches
    self.retrain_every = retrain_every
    self._obs: list[tuple[float, float, int, int]] = []

  def add(self, est_seconds: float, actual_seconds: float, hour: int, dow: int) -> None:
    if not self.enabled: return
    self._obs.append((est_seconds, actual_seconds, hour, dow))
    if len(self._obs) % self.retrain_every == 0 and len(self._obs) >= self.min_batches:
      self.train()

  def train(self) -> None:
    # Placeholder for linear regression
    pass

  def predict(self, est_seconds: float, service_minutes: float) -> Optional[float]:
    # No adjustment by default
    return None
