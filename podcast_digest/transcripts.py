"""Transcript extraction with automatic fallback chain and rate limit handling."""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import List, Optional
from urllib.parse import unquote

from .models import Video

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OAUTH_CACHE = PROJECT_ROOT / "__cache__"
TOKEN_FILE = str(OAUTH_CACHE / "tokens.json")

# Track rate limit state across videos in a batch
_timedtext_blocked = False


def _fetch_via_innertube(video: Video, languages: List[str], max_chars: int) -> bool:
    """Fetch transcript via YouTube innertube API (page scrape + get_transcript).

    This method does NOT use /api/timedtext, so it works even when that
    endpoint is returning 429. It fetches the video page, extracts the
    transcript params from the engagement panel, then calls get_transcript.
    """
    try:
        video_url = f"https://www.youtube.com/watch?v={video.video_id}"

        # Step 1: Fetch video page
        page_req = urllib.request.Request(video_url, headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
        })
        page_resp = urllib.request.urlopen(page_req, timeout=15)
        cookies = page_resp.headers.get_all("Set-Cookie") or []
        cookie_str = "; ".join(c.split(";")[0] for c in cookies)
        html = page_resp.read().decode("utf-8")

        # Extract session data
        m = re.search(r'"visitorData"\s*:\s*"([^"]+)"', html)
        visitor_data = m.group(1) if m else None

        m = re.search(r'"clientVersion"\s*:\s*"([^"]+)"', html)
        client_version = m.group(1) if m else "2.20260220.01.00"

        # Extract duration if not set
        if video.duration_seconds == 0:
            m = re.search(r'"lengthSeconds"\s*:\s*"(\d+)"', html)
            if m:
                video.duration_seconds = int(m.group(1))

        # Extract ytInitialData for transcript params
        m = re.search(r"ytInitialData\s*=\s*(\{.+?\});\s*</script>", html)
        if not m:
            m = re.search(r"ytInitialData\s*=\s*(\{.+?\});", html)
        if not m:
            logger.debug(f"innertube: no ytInitialData for {video.video_id}")
            return False

        initial_data = json.loads(m.group(1))

        # Find transcript panel params
        transcript_params = None
        for panel in initial_data.get("engagementPanels", []):
            renderer = panel.get("engagementPanelSectionListRenderer", {})
            if "transcript" in renderer.get("panelIdentifier", "").lower():
                content = renderer.get("content", {})
                cont = content.get("continuationItemRenderer", {})
                endpoint = cont.get("continuationEndpoint", {})
                transcript_params = endpoint.get(
                    "getTranscriptEndpoint", {}
                ).get("params", "")
                if transcript_params:
                    transcript_params = unquote(transcript_params)
                break

        if not transcript_params:
            logger.debug(f"innertube: no transcript params for {video.video_id}")
            return False

        # Step 2: Call get_transcript
        payload = {
            "context": {
                "client": {
                    "clientName": "WEB",
                    "clientVersion": client_version,
                    "visitorData": visitor_data,
                }
            },
            "params": transcript_params,
        }

        api_url = "https://www.youtube.com/youtubei/v1/get_transcript"
        data = json.dumps(payload).encode("utf-8")
        api_req = urllib.request.Request(api_url, data=data, headers={
            "Content-Type": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Cookie": cookie_str,
        })

        api_resp = urllib.request.urlopen(api_req, timeout=15)
        result = json.loads(api_resp.read().decode("utf-8"))

        # Parse transcript segments from response
        text_parts = []
        for action in result.get("actions", []):
            update = action.get("updateEngagementPanelAction", {})
            content = update.get("content", {})
            tr = content.get("transcriptRenderer", {}).get("content", {})
            body = tr.get("transcriptSearchPanelRenderer", {}).get("body", {})
            segments = body.get(
                "transcriptSegmentListRenderer", {}
            ).get("initialSegments", [])

            for seg in segments:
                snippet = seg.get("transcriptSegmentRenderer", {}).get("snippet", {})
                for run in snippet.get("runs", []):
                    t = run.get("text", "").strip()
                    if t:
                        text_parts.append(t)

        if not text_parts:
            logger.debug(f"innertube: empty transcript for {video.video_id}")
            return False

        full_text = " ".join(text_parts)

        if len(full_text.strip()) < 50:
            return False

        if len(full_text) > max_chars:
            full_text = full_text[:max_chars] + "\n[... transcricao truncada]"

        # Detect language from the transcript panel header or default to first preferred
        lang = languages[0] if languages else "unknown"
        video.transcript = full_text
        video.transcript_language = lang
        logger.info(
            f"Transcript via innertube ({lang}, {len(full_text)} chars): {video.title}"
        )
        return True

    except urllib.error.HTTPError as e:
        logger.debug(f"innertube HTTP error for {video.video_id}: {e.code}")
        return False
    except Exception as e:
        logger.debug(f"innertube failed for {video.video_id}: {type(e).__name__}: {e}")
        return False


def _fetch_via_pytubefix(video: Video, languages: List[str], max_chars: int) -> bool:
    """Fetch transcript using pytubefix. Uses cached OAuth if available."""
    global _timedtext_blocked
    if _timedtext_blocked:
        return False

    try:
        from pytubefix import YouTube
    except ImportError:
        return False

    try:
        has_oauth = Path(TOKEN_FILE).exists()

        yt = YouTube(
            f"https://www.youtube.com/watch?v={video.video_id}",
            use_oauth=has_oauth,
            allow_oauth_cache=True,
            token_file=TOKEN_FILE,
        )

        captions = yt.captions
        if not captions:
            return False

        # Update duration
        try:
            if video.duration_seconds == 0:
                video.duration_seconds = yt.length or 0
        except Exception:
            pass

        # Try preferred languages (exact and auto-generated)
        caption = None
        used_lang = None
        for lang in languages:
            for code in [lang, f"a.{lang}", f"{lang}-BR", f"{lang}-US"]:
                if code in captions:
                    caption = captions[code]
                    used_lang = lang
                    break
            if caption:
                break

        # Fallback: any available
        if not caption and captions:
            first_key = list(captions.keys())[0] if hasattr(captions, 'keys') else None
            if first_key is None:
                for c in captions:
                    caption = c
                    used_lang = getattr(c, 'code', 'unknown')
                    break
            else:
                caption = captions[first_key]
                used_lang = first_key

        if not caption:
            return False

        srt = caption.generate_srt_captions()
        full_text = _srt_to_text(srt)

        if not full_text or len(full_text.strip()) < 50:
            return False

        if len(full_text) > max_chars:
            full_text = full_text[:max_chars] + "\n[... transcricao truncada]"

        video.transcript = full_text
        video.transcript_language = used_lang
        logger.info(f"Transcript via pytubefix ({used_lang}, {len(full_text)} chars): {video.title}")
        return True

    except urllib.error.HTTPError as e:
        if e.code == 429:
            logger.warning("timedtext rate-limited (429). Switching to other methods.")
            _timedtext_blocked = True
        return False
    except Exception as e:
        if "429" in str(e):
            logger.warning("timedtext rate-limited (429). Switching to other methods.")
            _timedtext_blocked = True
        logger.debug(f"pytubefix failed for {video.video_id}: {type(e).__name__}: {e}")
        return False


def _fetch_via_api(video: Video, languages: List[str], max_chars: int) -> bool:
    """Fetch transcript using youtube-transcript-api (fallback)."""
    global _timedtext_blocked
    if _timedtext_blocked:
        return False

    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        from youtube_transcript_api._errors import NoTranscriptFound
    except ImportError:
        return False

    try:
        api = YouTubeTranscriptApi()
        transcript_list = api.list(video.video_id)

        transcript = None
        for lang in languages:
            try:
                transcript = transcript_list.find_transcript([lang])
                break
            except NoTranscriptFound:
                continue

        if transcript is None:
            try:
                transcript = transcript_list.find_generated_transcript(languages)
            except NoTranscriptFound:
                for t in transcript_list:
                    transcript = t
                    break

        if transcript is None:
            return False

        segments = list(transcript.fetch())
        full_text = " ".join(seg.text for seg in segments)

        if video.duration_seconds == 0 and segments:
            last = segments[-1]
            video.duration_seconds = int(last.start + last.duration)

        if len(full_text) > max_chars:
            full_text = full_text[:max_chars] + "\n[... transcricao truncada]"

        video.transcript = full_text
        video.transcript_language = transcript.language_code
        logger.info(f"Transcript via API ({transcript.language_code}, {len(full_text)} chars): {video.title}")
        return True

    except Exception as e:
        err_str = str(e)
        if "429" in err_str or "IpBlocked" in type(e).__name__:
            logger.warning("youtube-transcript-api rate-limited. Switching to other methods.")
            _timedtext_blocked = True
        else:
            logger.debug(f"youtube-transcript-api failed for {video.video_id}: {type(e).__name__}")
        return False


def _fetch_via_supadata(video: Video, languages: List[str], max_chars: int) -> bool:
    """Fetch transcript using Supadata API (if configured)."""
    api_key = os.environ.get("SUPADATA_API_KEY", "")
    if not api_key:
        return False

    try:
        import requests
    except ImportError:
        return False

    base = "https://api.supadata.ai/v1/transcript"
    url = f"{base}?url=https://youtu.be/{video.video_id}"

    for lang in languages + [None]:
        try:
            req_url = f"{url}&lang={lang}" if lang else url
            resp = requests.get(req_url, headers={"x-api-key": api_key}, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("content", [])
                if content:
                    full_text = " ".join(seg.get("text", "") for seg in content)
                    if len(full_text.strip()) < 50:
                        continue
                    if video.duration_seconds == 0 and content:
                        last = content[-1]
                        video.duration_seconds = int((last.get("offset", 0) + last.get("duration", 0)) / 1000)
                    if len(full_text) > max_chars:
                        full_text = full_text[:max_chars] + "\n[... transcricao truncada]"
                    video.transcript = full_text
                    video.transcript_language = data.get("lang", lang or "unknown")
                    logger.info(f"Transcript via Supadata ({video.transcript_language}, {len(full_text)} chars): {video.title}")
                    return True
        except Exception:
            continue

    return False


def _srt_to_text(srt_content: str) -> str:
    """Convert SRT to plain text."""
    lines = []
    for line in srt_content.splitlines():
        line = line.strip()
        if not line:
            continue
        if re.match(r"^\d+$", line):
            continue
        if re.match(r"^\d{2}:\d{2}:\d{2}", line):
            continue
        if "-->" in line:
            continue
        line = re.sub(r"<[^>]+>", "", line)
        if line:
            lines.append(line)

    deduped = []
    for line in lines:
        if not deduped or line != deduped[-1]:
            deduped.append(line)
    return " ".join(deduped)


def fetch_transcript(video: Video, languages: List[str],
                     max_chars: int = 80000) -> Video:
    """Fetch transcript trying multiple methods automatically.

    Order:
    1. pytubefix (with OAuth) - fastest when not rate-limited
    2. youtube-transcript-api - alternative direct method
    3. innertube get_transcript - uses different endpoint, bypasses timedtext 429
    4. Supadata API - external service, no IP issues
    """
    if _fetch_via_pytubefix(video, languages, max_chars):
        return video

    if _fetch_via_api(video, languages, max_chars):
        return video

    if _fetch_via_innertube(video, languages, max_chars):
        return video

    if _fetch_via_supadata(video, languages, max_chars):
        return video

    logger.warning(f"No transcript available for {video.video_id}: {video.title}")
    return video


def fetch_transcripts_batch(videos: List[Video], config: dict) -> List[Video]:
    """Fetch transcripts for a batch of videos with rate limiting."""
    global _timedtext_blocked
    _timedtext_blocked = False  # Reset at start of each batch

    languages = config["processing"]["transcript_languages"]
    max_chars = config["processing"].get("max_transcript_chars", 80000)

    for i, video in enumerate(videos):
        fetch_transcript(video, languages, max_chars)
        # Delay between requests to avoid triggering rate limits
        if i < len(videos) - 1:
            delay = 3 if _timedtext_blocked else 1
            time.sleep(delay)

    return videos
