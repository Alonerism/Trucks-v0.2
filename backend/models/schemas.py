from __future__ import annotations
from pydantic import BaseModel


class OptimizeRequest(BaseModel):
    date: str
