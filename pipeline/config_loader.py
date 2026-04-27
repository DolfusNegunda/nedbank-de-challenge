"""
pipeline/config_loader.py
─────────────────────────
Typed configuration loader for the Nedbank DE pipeline.

Resolution order for the config file path:
  1. PIPELINE_CONFIG environment variable
  2. /data/config/pipeline_config.yaml  (scoring system default mount)
  3. /app/config/pipeline_config.yaml   (image-baked fallback for local dev)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_CONFIG_SEARCH_PATHS: list[str] = [
    "/data/config/pipeline_config.yaml",
    "/app/config/pipeline_config.yaml",
]


def load(override_path: str | None = None) -> dict[str, Any]:
    """Load and return the pipeline configuration as a plain dict.

    Parameters
    ----------
    override_path:
        Explicit path to the YAML file.  When *None*, checks the
        ``PIPELINE_CONFIG`` environment variable, then the search path list.

    Returns
    -------
    dict[str, Any]
        Parsed YAML content.

    Raises
    ------
    FileNotFoundError
        When no config file can be located at any candidate path.
    """
    candidate: str | None = override_path or os.environ.get("PIPELINE_CONFIG")

    if candidate:
        path = Path(candidate)
        if not path.is_file():
            raise FileNotFoundError(
                f"Config file not found at explicit path: {candidate}"
            )
        logger.info("Loading config from explicit path: %s", path)
        return _parse(path)

    for search_path in _CONFIG_SEARCH_PATHS:
        path = Path(search_path)
        if path.is_file():
            logger.info("Loading config from: %s", path)
            return _parse(path)

    raise FileNotFoundError(
        "Pipeline config not found. Searched:\n"
        + "\n".join(f"  {p}" for p in _CONFIG_SEARCH_PATHS)
        + "\nSet PIPELINE_CONFIG env var to override."
    )


def _parse(path: Path) -> dict[str, Any]:
    """Read and parse a YAML file, returning its content as a dict."""
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(
            f"Config file {path} must contain a YAML mapping at the top level."
        )
    return data
