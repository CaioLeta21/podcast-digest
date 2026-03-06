"""SQLite database for persistence and deduplication."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .models import FeedbackEntry


SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    rss_url TEXT NOT NULL,
    added_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS videos (
    video_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    title TEXT NOT NULL,
    published_at TEXT NOT NULL,
    duration_seconds INTEGER DEFAULT 0,
    url TEXT NOT NULL,
    transcript TEXT,
    transcript_language TEXT,
    processed_at TEXT,
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
);

CREATE TABLE IF NOT EXISTS digests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    summary TEXT NOT NULL,
    key_topics TEXT NOT NULL,
    relevance_score INTEGER NOT NULL,
    relevance_reason TEXT NOT NULL,
    digest_date TEXT NOT NULL,
    FOREIGN KEY (video_id) REFERENCES videos(video_id)
);

CREATE TABLE IF NOT EXISTS feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    user_score INTEGER NOT NULL,
    comment TEXT DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (video_id) REFERENCES videos(video_id)
);

CREATE TABLE IF NOT EXISTS tracked_people (
    person_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    added_at TEXT NOT NULL DEFAULT (datetime('now')),
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS person_video_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    video_id TEXT NOT NULL,
    found_at TEXT NOT NULL DEFAULT (datetime('now')),
    search_query TEXT,
    FOREIGN KEY (person_id) REFERENCES tracked_people(person_id),
    FOREIGN KEY (video_id) REFERENCES videos(video_id),
    UNIQUE (person_id, video_id)
);

CREATE TABLE IF NOT EXISTS person_searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    searched_at TEXT NOT NULL DEFAULT (datetime('now')),
    results_found INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (person_id) REFERENCES tracked_people(person_id)
);

CREATE TABLE IF NOT EXISTS user_usage (
    user_uid TEXT PRIMARY KEY,
    analyses_count INTEGER NOT NULL DEFAULT 0,
    first_seen TEXT NOT NULL DEFAULT (datetime('now')),
    last_used TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_videos_published ON videos(published_at);
CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_digests_date ON digests(digest_date);
CREATE INDEX IF NOT EXISTS idx_feedback_created ON feedback(created_at);
"""

MIGRATION_SOURCE_COLUMN = """
ALTER TABLE videos ADD COLUMN source TEXT DEFAULT 'channel';
"""


class Database:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_schema(self):
        with self._connect() as conn:
            conn.executescript(SCHEMA)
            self._run_migrations(conn)

    def _run_migrations(self, conn: sqlite3.Connection):
        # Add source column to videos if missing
        columns = [row[1] for row in conn.execute("PRAGMA table_info(videos)").fetchall()]
        if "source" not in columns:
            conn.execute("ALTER TABLE videos ADD COLUMN source TEXT DEFAULT 'channel'")

    def video_exists(self, video_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM videos WHERE video_id = ?", (video_id,)
            ).fetchone()
            return row is not None

    def save_video(self, video_id: str, channel_id: str, title: str,
                   published_at: datetime, duration_seconds: int, url: str,
                   transcript: Optional[str] = None,
                   transcript_language: Optional[str] = None):
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO videos
                   (video_id, channel_id, title, published_at, duration_seconds,
                    url, transcript, transcript_language, processed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (video_id, channel_id, title, published_at.isoformat(),
                 duration_seconds, url, transcript, transcript_language,
                 datetime.now().isoformat())
            )

    def save_digest(self, video_id: str, summary: str, key_topics: List[str],
                    relevance_score: int, relevance_reason: str, digest_date: str):
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO digests
                   (video_id, summary, key_topics, relevance_score,
                    relevance_reason, digest_date)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (video_id, summary, ",".join(key_topics),
                 relevance_score, relevance_reason, digest_date)
            )

    def save_feedback(self, entry: FeedbackEntry):
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO feedback (video_id, user_score, comment, created_at)
                   VALUES (?, ?, ?, ?)""",
                (entry.video_id, entry.user_score, entry.comment,
                 entry.created_at.isoformat())
            )

    def get_recent_feedback(self, limit: int = 50) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT f.video_id, f.user_score, f.comment,
                          v.title, v.channel_id, d.relevance_score as ai_score,
                          d.summary
                   FROM feedback f
                   JOIN videos v ON f.video_id = v.video_id
                   LEFT JOIN digests d ON f.video_id = d.video_id
                   ORDER BY f.created_at DESC
                   LIMIT ?""",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_recent_digests(self, days: int = 7) -> List[Dict]:
        """Get recent digests for feedback session."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT d.video_id, v.title, v.channel_id, v.url,
                          d.summary, d.relevance_score, d.digest_date
                   FROM digests d
                   JOIN videos v ON d.video_id = v.video_id
                   WHERE d.digest_date >= date('now', ?)
                   ORDER BY d.relevance_score DESC""",
                (f"-{days} days",)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_digest_dates(self) -> List[str]:
        """Get all distinct digest dates, newest first."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT digest_date FROM digests ORDER BY digest_date DESC"
            ).fetchall()
            return [r["digest_date"] for r in rows]

    def get_digests_by_date(self, digest_date: str) -> List[Dict]:
        """Get all digests for a specific date."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT d.video_id, v.title, v.channel_id, v.url,
                          d.summary, d.key_topics, d.relevance_score,
                          d.relevance_reason, v.duration_seconds, d.digest_date
                   FROM digests d
                   JOIN videos v ON d.video_id = v.video_id
                   WHERE d.digest_date = ?
                   ORDER BY d.relevance_score DESC""",
                (digest_date,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_digests_for_period(self, lookback_hours: int, channel_ids: List[str]) -> List[Dict]:
        """Get all digests for videos published within the lookback window from given channels."""
        if not channel_ids:
            return []
        placeholders = ",".join("?" * len(channel_ids))
        with self._connect() as conn:
            rows = conn.execute(
                f"""SELECT d.video_id, v.title, v.channel_id, v.url,
                          d.summary, d.key_topics, d.relevance_score,
                          d.relevance_reason, v.duration_seconds
                   FROM digests d
                   JOIN videos v ON d.video_id = v.video_id
                   WHERE v.channel_id IN ({placeholders})
                     AND v.published_at >= datetime('now', ?)
                   ORDER BY d.relevance_score DESC""",
                (*channel_ids, f"-{lookback_hours} hours")
            ).fetchall()
            return [dict(r) for r in rows]

    def get_videos_without_transcript(self, lookback_hours: int) -> List[Dict]:
        """Get videos saved without transcript that might be retried."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT video_id, channel_id, title, published_at,
                          duration_seconds, url, source
                   FROM videos
                   WHERE transcript IS NULL
                     AND published_at >= datetime('now', ?)
                   ORDER BY published_at DESC""",
                (f"-{lookback_hours} hours",)
            ).fetchall()
            return [dict(r) for r in rows]

    def update_video_transcript(self, video_id: str, transcript: str, language: str):
        """Update transcript for an existing video."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE videos SET transcript = ?, transcript_language = ? WHERE video_id = ?",
                (transcript, language, video_id)
            )

    def upsert_channel(self, channel_id: str, name: str, rss_url: str):
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO channels (channel_id, name, rss_url)
                   VALUES (?, ?, ?)""",
                (channel_id, name, rss_url)
            )

    def get_channel_name(self, channel_id: str) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT name FROM channels WHERE channel_id = ?",
                (channel_id,)
            ).fetchone()
            return row["name"] if row else channel_id

    # --- Person tracking methods ---

    def add_person(self, name: str) -> int:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO tracked_people (name) VALUES (?)",
                (name,)
            )
            row = conn.execute(
                "SELECT person_id FROM tracked_people WHERE name = ?",
                (name,)
            ).fetchone()
            return row["person_id"]

    def remove_person(self, name: str):
        with self._connect() as conn:
            conn.execute(
                "UPDATE tracked_people SET active = 0 WHERE name = ?",
                (name,)
            )

    def get_active_people(self) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT person_id, name, added_at FROM tracked_people WHERE active = 1 ORDER BY name"
            ).fetchall()
            return [dict(r) for r in rows]

    def link_person_video(self, person_id: int, video_id: str, search_query: str = ""):
        with self._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO person_video_links
                   (person_id, video_id, search_query)
                   VALUES (?, ?, ?)""",
                (person_id, video_id, search_query)
            )

    def log_person_search(self, person_id: int, results_found: int):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO person_searches (person_id, results_found) VALUES (?, ?)",
                (person_id, results_found)
            )

    def get_person_recent_videos(self, person_id: int, limit: int = 20) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT v.video_id, v.title, v.url, v.published_at,
                          v.duration_seconds, v.channel_id, pvl.found_at, pvl.search_query
                   FROM person_video_links pvl
                   JOIN videos v ON pvl.video_id = v.video_id
                   WHERE pvl.person_id = ?
                   ORDER BY pvl.found_at DESC
                   LIMIT ?""",
                (person_id, limit)
            ).fetchall()
            return [dict(r) for r in rows]

    # --- User usage tracking ---

    def get_usage_count(self, user_uid: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT analyses_count FROM user_usage WHERE user_uid = ?",
                (user_uid,)
            ).fetchone()
            return row["analyses_count"] if row else 0

    def increment_usage(self, user_uid: str):
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO user_usage (user_uid, analyses_count, last_used)
                   VALUES (?, 1, datetime('now'))
                   ON CONFLICT(user_uid) DO UPDATE SET
                     analyses_count = analyses_count + 1,
                     last_used = datetime('now')""",
                (user_uid,)
            )

    def save_video_with_source(self, video_id: str, channel_id: str, title: str,
                               published_at: datetime, duration_seconds: int, url: str,
                               source: str = "channel",
                               transcript: Optional[str] = None,
                               transcript_language: Optional[str] = None):
        with self._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO videos
                   (video_id, channel_id, title, published_at, duration_seconds,
                    url, transcript, transcript_language, processed_at, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (video_id, channel_id, title, published_at.isoformat(),
                 duration_seconds, url, transcript, transcript_language,
                 datetime.now().isoformat(), source)
            )
