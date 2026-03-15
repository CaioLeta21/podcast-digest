"""Resolve Spotify and Apple Podcasts URLs to episode metadata + audio URL."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional, Tuple

import feedparser
import requests

logger = logging.getLogger(__name__)


@dataclass
class PodcastEpisode:
    title: str
    show_name: str
    audio_url: str
    duration_seconds: int = 0
    episode_url: str = ""


def detect_url_type(url: str) -> str:
    """Detect URL type: 'youtube', 'spotify', 'apple', or 'unknown'."""
    url = url.strip()
    if re.search(r"(youtube\.com|youtu\.be)", url):
        return "youtube"
    if "open.spotify.com/episode" in url:
        return "spotify"
    if "podcasts.apple.com" in url:
        return "apple"
    return "unknown"


def resolve_spotify(url: str) -> Optional[PodcastEpisode]:
    """Resolve a Spotify episode URL to audio via RSS feed lookup.

    Flow:
    1. Spotify oEmbed API → get episode title + show name
    2. iTunes Search API → find podcast RSS feed by show name
    3. Parse RSS feed → find episode by title match
    4. Get audio enclosure URL from RSS
    """
    try:
        # Step 1: Get metadata from Spotify oEmbed (no auth needed)
        oembed_url = f"https://open.spotify.com/oembed?url={url}"
        resp = requests.get(oembed_url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # oEmbed returns title like "Episode Title" and provider_name "Spotify"
        # The title field contains the episode title
        episode_title = data.get("title", "")
        # The HTML contains the show name in the iframe title
        html = data.get("html", "")

        # Try to extract show name from the Spotify page directly
        show_name = _get_spotify_show_name(url)

        if not episode_title:
            logger.error("Spotify oEmbed returned no title")
            return None

        if not show_name:
            logger.warning("Could not extract show name from Spotify, trying with episode title only")
            show_name = ""

        logger.info(f"Spotify episode: '{episode_title}' from '{show_name}'")

        # Step 2: Find RSS feed via iTunes Search
        rss_url = _find_rss_via_itunes(show_name or episode_title)
        if not rss_url:
            logger.error(f"Could not find RSS feed for show: {show_name}")
            return None

        # Step 3: Find episode in RSS
        return _find_episode_in_rss(rss_url, episode_title, url)

    except Exception as e:
        logger.error(f"Spotify resolution failed: {e}")
        return None


def _get_spotify_show_name(episode_url: str) -> str:
    """Scrape Spotify episode page to get the show/podcast name."""
    try:
        resp = requests.get(episode_url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131.0.0.0 Safari/537.36",
        })
        resp.raise_for_status()
        html = resp.text

        # Try og:description or meta tags that contain show name
        # Spotify pages have: <meta property="og:title" content="Episode Title">
        # and typically: <meta name="description" content="Listen to ... on Spotify. SHOW_NAME ...">
        # Or: <title>Episode Title | Podcast on Spotify</title>

        # Try to find show name from the page structure
        # Pattern: "podcast_name" in JSON-LD or similar
        m = re.search(r'"show"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"', html)
        if m:
            return m.group(1)

        # Try title format: "Episode - Show | Podcast on Spotify"
        m = re.search(r'<title>(.+?)\s*\|\s*Podcast on Spotify</title>', html)
        if m:
            parts = m.group(1).rsplit(" - ", 1)
            if len(parts) == 2:
                return parts[1].strip()

        # Try og:description which often has show name
        m = re.search(r'<meta\s+property="og:description"\s+content="([^"]+)"', html)
        if m:
            desc = m.group(1)
            # Often: "Listen to this episode from SHOW NAME on Spotify..."
            m2 = re.search(r"(?:from|de)\s+(.+?)\s+on\s+Spotify", desc)
            if m2:
                return m2.group(1)

    except Exception as e:
        logger.debug(f"Could not scrape Spotify page: {e}")

    return ""


def resolve_apple(url: str) -> Optional[PodcastEpisode]:
    """Resolve an Apple Podcasts URL to audio via RSS feed.

    Flow:
    1. Extract podcast ID from URL
    2. iTunes Lookup API → get RSS feed URL
    3. Parse RSS feed → find episode
    4. Get audio enclosure URL
    """
    try:
        # Extract podcast ID from URL
        # Format: https://podcasts.apple.com/COUNTRY/podcast/PODCAST-NAME/idNUMBER?i=EPISODE_NUMBER
        m = re.search(r"/id(\d+)", url)
        if not m:
            logger.error(f"Could not extract podcast ID from Apple URL: {url}")
            return None

        podcast_id = m.group(1)

        # Extract episode ID if present
        episode_id = None
        m_ep = re.search(r"[?&]i=(\d+)", url)
        if m_ep:
            episode_id = m_ep.group(1)

        # Step 1: iTunes Lookup to get RSS feed URL
        lookup_url = f"https://itunes.apple.com/lookup?id={podcast_id}&entity=podcast"
        resp = requests.get(lookup_url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            logger.error(f"iTunes Lookup returned no results for ID {podcast_id}")
            return None

        rss_url = results[0].get("feedUrl", "")
        show_name = results[0].get("collectionName", "")

        if not rss_url:
            logger.error(f"No RSS feed URL for podcast ID {podcast_id}")
            return None

        logger.info(f"Apple Podcasts: '{show_name}' RSS: {rss_url}")

        # Step 2: If we have an episode ID, get its title first
        episode_title = ""
        if episode_id:
            ep_lookup = f"https://itunes.apple.com/lookup?id={episode_id}&entity=podcastEpisode"
            try:
                resp = requests.get(ep_lookup, timeout=15)
                ep_data = resp.json()
                ep_results = ep_data.get("results", [])
                for r in ep_results:
                    if r.get("wrapperType") == "podcastEpisode":
                        episode_title = r.get("trackName", "")
                        break
            except Exception:
                pass

        # Step 3: Find episode in RSS
        return _find_episode_in_rss(rss_url, episode_title, url, episode_id)

    except Exception as e:
        logger.error(f"Apple Podcasts resolution failed: {e}")
        return None


def _find_rss_via_itunes(search_term: str) -> Optional[str]:
    """Search iTunes API to find podcast RSS feed URL."""
    try:
        search_url = "https://itunes.apple.com/search"
        params = {
            "term": search_term,
            "media": "podcast",
            "limit": 5,
        }
        resp = requests.get(search_url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            return None

        # Return the RSS feed of the first (most relevant) result
        # Try to match by name if we have a show name
        for r in results:
            feed_url = r.get("feedUrl", "")
            if feed_url:
                logger.info(f"Found RSS feed for '{r.get('collectionName', '')}': {feed_url}")
                return feed_url

        return None

    except Exception as e:
        logger.error(f"iTunes search failed: {e}")
        return None


def _find_episode_in_rss(
    rss_url: str,
    episode_title: str,
    original_url: str,
    apple_episode_id: str = None,
) -> Optional[PodcastEpisode]:
    """Parse RSS feed and find the matching episode."""
    try:
        feed = feedparser.parse(rss_url)

        if not feed.entries:
            logger.error(f"RSS feed has no entries: {rss_url}")
            return None

        show_name = feed.feed.get("title", "Unknown Podcast")

        # If no episode title, return the most recent episode
        if not episode_title and not apple_episode_id:
            entry = feed.entries[0]
            return _entry_to_episode(entry, show_name, original_url)

        # Try to match by title (fuzzy)
        best_match = None
        best_score = 0

        for entry in feed.entries:
            entry_title = entry.get("title", "")

            # Exact match
            if entry_title.lower().strip() == episode_title.lower().strip():
                return _entry_to_episode(entry, show_name, original_url)

            # Fuzzy: check word overlap
            score = _title_similarity(episode_title, entry_title)
            if score > best_score:
                best_score = score
                best_match = entry

        # Accept if similarity is good enough (>50%)
        if best_match and best_score > 0.5:
            logger.info(f"Fuzzy match (score={best_score:.2f}): {best_match.get('title', '')}")
            return _entry_to_episode(best_match, show_name, original_url)

        # Fallback: return the most recent episode with a warning
        logger.warning(f"No good match for '{episode_title}'. Using most recent episode.")
        return _entry_to_episode(feed.entries[0], show_name, original_url)

    except Exception as e:
        logger.error(f"RSS parsing failed: {e}")
        return None


def _entry_to_episode(entry: dict, show_name: str, original_url: str) -> Optional[PodcastEpisode]:
    """Convert an RSS feed entry to a PodcastEpisode."""
    # Get audio URL from enclosures
    audio_url = ""
    for enc in entry.get("enclosures", []):
        if enc.get("type", "").startswith("audio/") or enc.get("href", "").endswith(".mp3"):
            audio_url = enc.get("href", "")
            break

    # Fallback: check links
    if not audio_url:
        for link in entry.get("links", []):
            if link.get("type", "").startswith("audio/"):
                audio_url = link.get("href", "")
                break

    if not audio_url:
        logger.error(f"No audio URL found for episode: {entry.get('title', '')}")
        return None

    # Parse duration
    duration = 0
    dur_str = entry.get("itunes_duration", "")
    if dur_str:
        duration = _parse_duration(dur_str)

    return PodcastEpisode(
        title=entry.get("title", "Unknown Episode"),
        show_name=show_name,
        audio_url=audio_url,
        duration_seconds=duration,
        episode_url=original_url,
    )


def _title_similarity(a: str, b: str) -> float:
    """Simple word-overlap similarity between two titles."""
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    return len(intersection) / max(len(words_a), len(words_b))


def _parse_duration(dur_str: str) -> int:
    """Parse iTunes duration string to seconds. Accepts HH:MM:SS, MM:SS, or raw seconds."""
    dur_str = dur_str.strip()
    if ":" in dur_str:
        parts = dur_str.split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    try:
        return int(dur_str)
    except ValueError:
        return 0
