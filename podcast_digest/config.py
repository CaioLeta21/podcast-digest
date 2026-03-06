"""Configuration loading from config.yaml."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml


_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_config(config_path: Optional[str] = None) -> dict:
    """Load and validate configuration from YAML file."""
    if config_path is None:
        config_path = str(_PROJECT_ROOT / "config.yaml")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Resolve relative paths against project root
    for key in ("database.path", "profile.path", "logging.file"):
        parts = key.split(".")
        section = config[parts[0]]
        if not os.path.isabs(section[parts[1]]):
            section[parts[1]] = str(_PROJECT_ROOT / section[parts[1]])

    # Ensure data directory exists
    db_dir = os.path.dirname(config["database"]["path"])
    os.makedirs(db_dir, exist_ok=True)

    log_dir = os.path.dirname(config["logging"]["file"])
    os.makedirs(log_dir, exist_ok=True)

    # Inject environment variables
    config["email"]["address"] = os.environ.get("EMAIL_ADDRESS", "")
    config["email"]["password"] = os.environ.get("EMAIL_PASSWORD", "")
    config["youtube_api"]["api_key"] = os.environ.get("YOUTUBE_API_KEY", "")

    # AI provider keys
    config.setdefault("gemini", {})
    config["claude"]["api_key"] = os.environ.get("ANTHROPIC_API_KEY", "")
    config["gemini"]["api_key"] = os.environ.get("GEMINI_API_KEY", "")

    # Determine active AI provider (validation deferred to synthesis time)
    config["_ai_provider"] = config.get("ai_provider", "claude")

    return config


def get_channel_rss_url(channel_id: str) -> str:
    """Build YouTube RSS feed URL from channel ID."""
    return f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
