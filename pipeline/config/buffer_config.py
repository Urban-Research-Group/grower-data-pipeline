"""
Load S3 retrieve buffer settings from retrieve_buffers.yaml (defaults + per-state overrides).
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Any, TypedDict

import yaml

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "retrieve_buffers.yaml")


class RetrieveParams(TypedDict):
    head_bytes: int
    tail_bytes: int
    threshold_bytes: int
    stale_threshold_days: int
    use_optimization: bool
    # Row-level: share of rows in the parsed head+tail chunk that match target date (0–1)
    utilization_threshold_high: float  # at or above → likely expand tail (see retrieve.py)
    utilization_log_low: float  # at or below → log for manual shrink review only
    # Which raw files to read: regex the S3 key must match, plus substrings that exclude a key
    file_pattern: str
    exclude_files: list[str]


DEFAULT_FILE_PATTERN = r"per[_]?count(y|ies)?"


def _load_merged_for_state(state_key: str) -> dict[str, Any]:
    with open(_CONFIG_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not data or "defaults" not in data:
        raise ValueError(f"Invalid retrieve buffer config: missing 'defaults' in {_CONFIG_PATH}")

    base = dict(data["defaults"])
    states = data.get("states") or {}
    if state_key in states and states[state_key] is not None:
        overrides = states[state_key]
        if not isinstance(overrides, dict):
            raise TypeError(f"states.{state_key} must be a mapping, got {type(overrides)}")
        base.update(overrides)
    return base


def get_state_retrieve_params(state: str) -> RetrieveParams:
    """
    Return merged retrieve parameters for a two-letter state code (e.g. 'al', 'GA').
    Caches by normalized state string.
    """
    return _get_state_retrieve_params_cached(state.strip().lower())


@lru_cache(maxsize=32)
def _get_state_retrieve_params_cached(state_key: str) -> RetrieveParams:
    merged = _load_merged_for_state(state_key)
    return {
        "head_bytes": int(merged["head_bytes"]),
        "tail_bytes": int(merged["tail_bytes"]),
        "threshold_bytes": int(merged["threshold_bytes"]),
        "stale_threshold_days": int(merged["stale_threshold_days"]),
        "use_optimization": bool(merged["use_optimization"]),
        "utilization_threshold_high": float(merged.get("utilization_threshold_high", 0.8)),
        "utilization_log_low": float(merged.get("utilization_log_low", 0.3)),
        "file_pattern": str(merged.get("file_pattern") or DEFAULT_FILE_PATTERN),
        "exclude_files": [str(s).lower() for s in (merged.get("exclude_files") or [])],
    }


def is_wanted_file(key: str, params: RetrieveParams) -> bool:
    """True if an S3 key is a CSV this state's pipeline should read."""
    lowered = key.lower()
    if not lowered.endswith(".csv"):
        return False
    if not re.search(params["file_pattern"], key, re.IGNORECASE):
        return False
    return not any(s in lowered for s in params["exclude_files"])


def clear_buffer_config_cache() -> None:
    """Call after tests or if you need to reload the YAML from disk in the same process."""
    _get_state_retrieve_params_cached.cache_clear()
