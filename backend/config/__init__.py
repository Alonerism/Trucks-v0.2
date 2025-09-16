from __future__ import annotations
from pathlib import Path
import yaml

_SETTINGS_CACHE: dict | None = None

def load_settings() -> dict:
	global _SETTINGS_CACHE
	if _SETTINGS_CACHE is not None:
		return _SETTINGS_CACHE
	cfg_path = Path(__file__).with_name("settings.yaml")
	if not cfg_path.exists():
		_SETTINGS_CACHE = {}
		return _SETTINGS_CACHE
	with cfg_path.open("r") as f:
		data = yaml.safe_load(f) or {}
	_SETTINGS_CACHE = data
	return data
