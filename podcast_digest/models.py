"""Shared dataclasses for podcast-digest."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class Channel:
    channel_id: str
    name: str
    rss_url: str
    added_at: datetime = field(default_factory=datetime.now)


@dataclass
class Video:
    video_id: str
    channel_id: str
    title: str
    published_at: datetime
    duration_seconds: int = 0
    url: str = ""
    transcript: Optional[str] = None
    transcript_language: Optional[str] = None
    source: str = "channel"  # "channel" or "person_search"


@dataclass
class EpisodeSummary:
    video_id: str
    title: str
    channel_name: str
    url: str
    summary: str
    key_topics: List[str]
    relevance_score: int  # 1-10
    relevance_reason: str
    duration_seconds: int = 0
    transcript_available: bool = True


@dataclass
class CrossSynthesis:
    themes: List[Dict]  # [{"theme": str, "summary": str, "episodes": [video_id]}]
    generated_at: datetime = field(default_factory=datetime.now)


@dataclass
class DigestResult:
    date: str
    episodes: List[EpisodeSummary]
    cross_synthesis: Optional[CrossSynthesis]
    no_transcript: List[Video]
    total_channels_checked: int
    total_new_episodes: int
    person_episodes: List[EpisodeSummary] = field(default_factory=list)
    person_names: List[str] = field(default_factory=list)
    person_no_transcript: List[Video] = field(default_factory=list)


@dataclass
class FeedbackEntry:
    video_id: str
    user_score: int  # 1-10
    comment: str = ""
    created_at: datetime = field(default_factory=datetime.now)
