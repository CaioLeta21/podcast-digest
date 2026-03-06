"""Channel monitoring via RSS (primary) and YouTube API (fallback)."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone, timedelta
from typing import List

import feedparser
from dateutil import parser as dateutil_parser

from .config import get_channel_rss_url
from .models import Video

logger = logging.getLogger(__name__)


def fetch_new_videos(config: dict) -> List[Video]:
    """Fetch new videos from all configured channels."""
    lookback_hours = config["processing"]["lookback_hours"]
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    all_videos = []
    for ch in config["channels"]:
        channel_id = ch["id"]
        name = ch["name"]
        rss_url = get_channel_rss_url(channel_id)

        try:
            videos = _fetch_from_rss(channel_id, rss_url, cutoff)
            logger.info(f"RSS: {len(videos)} new videos from {name}")
        except Exception as e:
            logger.warning(f"RSS failed for {name}: {e}")
            if config["youtube_api"]["enabled"] and config["youtube_api"]["api_key"]:
                try:
                    videos = _fetch_from_api(
                        channel_id, config["youtube_api"]["api_key"], cutoff
                    )
                    logger.info(f"API fallback: {len(videos)} videos from {name}")
                except Exception as e2:
                    logger.error(f"API also failed for {name}: {e2}")
                    continue
            else:
                continue

        all_videos.extend(videos)

    return all_videos


def _fetch_from_rss(channel_id: str, rss_url: str, cutoff: datetime) -> List[Video]:
    """Parse YouTube RSS feed for recent videos."""
    feed = feedparser.parse(rss_url)

    if feed.bozo and not feed.entries:
        raise RuntimeError(f"Failed to parse RSS: {feed.bozo_exception}")

    videos = []
    for entry in feed.entries:
        published = dateutil_parser.parse(entry.published)
        if published < cutoff:
            continue

        video_id = entry.yt_videoid
        url = entry.link

        videos.append(Video(
            video_id=video_id,
            channel_id=channel_id,
            title=entry.title,
            published_at=published,
            duration_seconds=0,  # RSS doesn't provide duration
            url=url,
        ))

    return videos


def _fetch_from_api(channel_id: str, api_key: str, cutoff: datetime) -> List[Video]:
    """Fallback: use YouTube Data API v3 to fetch recent videos."""
    from googleapiclient.discovery import build

    youtube = build("youtube", "v3", developerKey=api_key)

    search_resp = youtube.search().list(
        channelId=channel_id,
        type="video",
        order="date",
        publishedAfter=cutoff.isoformat(),
        maxResults=15,
        part="id,snippet",
    ).execute()

    if not search_resp.get("items"):
        return []

    video_ids = [item["id"]["videoId"] for item in search_resp["items"]]

    details_resp = youtube.videos().list(
        id=",".join(video_ids),
        part="contentDetails,snippet",
    ).execute()

    videos = []
    for item in details_resp.get("items", []):
        duration = _parse_iso_duration(item["contentDetails"]["duration"])
        published = dateutil_parser.parse(item["snippet"]["publishedAt"])

        videos.append(Video(
            video_id=item["id"],
            channel_id=channel_id,
            title=item["snippet"]["title"],
            published_at=published,
            duration_seconds=duration,
            url=f"https://www.youtube.com/watch?v={item['id']}",
        ))

    return videos


def _parse_iso_duration(duration_str: str) -> int:
    """Parse ISO 8601 duration (PT1H2M3S) to seconds."""
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration_str)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds
