"""Search YouTube for interviews/podcasts featuring tracked people via yt-dlp."""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from .models import Video

logger = logging.getLogger(__name__)


def _find_yt_dlp() -> str:
    """Find yt-dlp binary: prefer the one in the current venv, fallback to PATH."""
    # Check alongside the running Python interpreter (same venv)
    venv_bin = Path(sys.executable).parent / "yt-dlp"
    if venv_bin.exists():
        return str(venv_bin)
    # Fallback to system PATH
    found = shutil.which("yt-dlp")
    if found:
        return found
    return "yt-dlp"


def search_person_videos(
    name: str,
    max_results: int = 5,
    lookback_days: int = 7,
    min_duration_minutes: int = 10,
) -> List[Tuple[str, Video]]:
    """Search YouTube for recent interviews/podcasts featuring a person.

    Returns list of (search_query, Video) tuples.
    """
    queries = [
        name,
        f'"{name}" interview podcast',
    ]

    cutoff = datetime.now() - timedelta(days=lookback_days)
    min_duration = min_duration_minutes * 60
    seen_ids: set = set()
    results: List[Tuple[str, Video]] = []

    for query in queries:
        videos = _yt_search(query, max_results)
        for video in videos:
            if video.video_id in seen_ids:
                continue
            seen_ids.add(video.video_id)

            if video.duration_seconds < min_duration:
                continue
            if video.published_at < cutoff:
                continue

            video.source = "person_search"
            results.append((query, video))

    logger.info(f"Person search '{name}': {len(results)} results from {len(queries)} queries")
    return results


def _yt_search(query: str, max_results: int) -> List[Video]:
    """Run yt-dlp search and parse results."""
    search_term = f"ytsearch{max_results}:{query}"

    yt_dlp_path = _find_yt_dlp()

    try:
        result = subprocess.run(
            [
                yt_dlp_path,
                search_term,
                "--dump-json",
                "--no-download",
                "--no-warnings",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except FileNotFoundError:
        logger.error("yt-dlp not found. Install with: pip install yt-dlp")
        return []
    except subprocess.TimeoutExpired:
        logger.warning(f"yt-dlp search timed out for: {query}")
        return []

    if result.returncode != 0:
        logger.warning(f"yt-dlp search failed for '{query}': {result.stderr[:200]}")
        return []

    videos = []
    for line in result.stdout.strip().splitlines():
        if not line.strip():
            continue
        try:
            data = json.loads(line)
            video = _parse_yt_result(data)
            if video:
                videos.append(video)
        except json.JSONDecodeError:
            continue

    return videos


def _parse_yt_result(data: dict) -> Video | None:
    """Parse a single yt-dlp JSON result into a Video."""
    video_id = data.get("id")
    if not video_id:
        return None

    title = data.get("title", "")
    duration = data.get("duration") or 0
    channel_id = data.get("channel_id", "unknown")
    upload_date = data.get("upload_date", "")

    if upload_date:
        try:
            published = datetime.strptime(upload_date, "%Y%m%d")
        except ValueError:
            published = datetime.now()
    else:
        published = datetime.now()

    url = data.get("webpage_url") or f"https://www.youtube.com/watch?v={video_id}"

    return Video(
        video_id=video_id,
        channel_id=channel_id,
        title=title,
        published_at=published,
        duration_seconds=int(duration),
        url=url,
        source="person_search",
    )


def search_all_people(config: dict) -> Dict[str, List[Tuple[str, Video]]]:
    """Search for all tracked people. Returns {person_name: [(query, Video), ...]}."""
    people = config.get("tracked_people", [])
    search_cfg = config.get("person_search", {})

    if not search_cfg.get("enabled", True):
        logger.info("Person search is disabled in config")
        return {}

    max_results = search_cfg.get("max_results_per_query", 5)
    lookback_days = search_cfg.get("lookback_days", 7)
    min_duration = search_cfg.get("min_duration_minutes", 10)

    all_results: Dict[str, List[Tuple[str, Video]]] = {}

    for person in people:
        name = person["name"]
        results = search_person_videos(
            name,
            max_results=max_results,
            lookback_days=lookback_days,
            min_duration_minutes=min_duration,
        )
        if results:
            all_results[name] = results

    return all_results
