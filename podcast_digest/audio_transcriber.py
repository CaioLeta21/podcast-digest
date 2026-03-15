"""Download podcast audio and transcribe via Gemini."""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)


def download_audio(audio_url: str, max_mb: int = 200) -> Optional[str]:
    """Download audio file to a temp path. Returns the file path or None."""
    try:
        resp = requests.get(audio_url, stream=True, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (podcast-digest)",
        })
        resp.raise_for_status()

        # Check content length if available
        content_length = resp.headers.get("Content-Length")
        if content_length and int(content_length) > max_mb * 1024 * 1024:
            logger.warning(f"Audio file too large: {int(content_length) / 1024 / 1024:.0f}MB (max {max_mb}MB)")

        # Determine extension from content type or URL
        content_type = resp.headers.get("Content-Type", "")
        if "mp3" in content_type or audio_url.split("?")[0].endswith(".mp3"):
            ext = ".mp3"
        elif "mp4" in content_type or "m4a" in content_type:
            ext = ".m4a"
        elif "ogg" in content_type:
            ext = ".ogg"
        elif "wav" in content_type:
            ext = ".wav"
        else:
            ext = ".mp3"  # default

        # Download to temp file
        tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
        total = 0
        for chunk in resp.iter_content(chunk_size=8192):
            tmp.write(chunk)
            total += len(chunk)
            if total > max_mb * 1024 * 1024:
                tmp.close()
                os.unlink(tmp.name)
                logger.error(f"Audio download exceeded {max_mb}MB limit, aborting")
                return None

        tmp.close()
        logger.info(f"Downloaded audio: {total / 1024 / 1024:.1f}MB → {tmp.name}")
        return tmp.name

    except Exception as e:
        logger.error(f"Audio download failed: {e}")
        return None


def transcribe_audio(audio_path: str, config: dict, user_api_key: str = "", user_provider: str = "") -> Optional[str]:
    """Transcribe audio file using available AI provider.

    Tries in order:
    1. Gemini (native audio support)
    2. OpenAI Whisper API
    """
    # Determine which provider/key to use
    if user_api_key:
        if user_provider == "gemini" or (not user_provider and user_api_key.startswith("AIza")):
            return _transcribe_via_gemini(audio_path, user_api_key)
        elif user_provider in ("openai", "deepseek") or (not user_provider and user_api_key.startswith("sk-")):
            return _transcribe_via_openai(audio_path, user_api_key)

    # Try configured providers
    gemini_key = config.get("gemini", {}).get("api_key", "") or os.environ.get("GEMINI_API_KEY", "")
    if gemini_key:
        result = _transcribe_via_gemini(audio_path, gemini_key)
        if result:
            return result

    # Try OpenAI
    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if openai_key:
        result = _transcribe_via_openai(audio_path, openai_key)
        if result:
            return result

    # Try Anthropic Claude (no native audio, but can work with text extracted by other means)
    logger.error("No provider available for audio transcription. Need Gemini or OpenAI API key.")
    return None


def _transcribe_via_gemini(audio_path: str, api_key: str) -> Optional[str]:
    """Transcribe audio using Google Gemini (native audio support)."""
    try:
        from google import genai

        client = genai.Client(api_key=api_key)

        # Upload the audio file
        logger.info(f"Uploading audio to Gemini for transcription...")
        uploaded_file = client.files.upload(file=audio_path)

        logger.info(f"Transcribing with Gemini...")
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[
                uploaded_file,
                "Transcreva o audio acima de forma completa e fiel. "
                "Retorne APENAS o texto da transcricao, sem timestamps, "
                "sem formatacao especial, sem comentarios. "
                "Se o audio estiver em portugues, mantenha em portugues. "
                "Se estiver em ingles ou outro idioma, mantenha no idioma original.",
            ],
        )

        transcript = response.text.strip()

        if len(transcript) < 50:
            logger.warning(f"Gemini transcription too short: {len(transcript)} chars")
            return None

        logger.info(f"Transcription complete: {len(transcript)} chars")

        # Clean up uploaded file
        try:
            client.files.delete(name=uploaded_file.name)
        except Exception:
            pass

        return transcript

    except Exception as e:
        logger.error(f"Gemini transcription failed: {e}")
        return None


def _transcribe_via_openai(audio_path: str, api_key: str) -> Optional[str]:
    """Transcribe audio using OpenAI Whisper API."""
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)

        file_size = os.path.getsize(audio_path)
        if file_size > 25 * 1024 * 1024:
            logger.warning(f"Audio file too large for OpenAI Whisper ({file_size / 1024 / 1024:.0f}MB > 25MB limit)")
            return None

        logger.info(f"Transcribing with OpenAI Whisper...")
        with open(audio_path, "rb") as f:
            response = client.audio.transcriptions.create(
                model="whisper-1",
                file=f,
            )

        transcript = response.text.strip()

        if len(transcript) < 50:
            logger.warning(f"OpenAI transcription too short: {len(transcript)} chars")
            return None

        logger.info(f"Transcription complete: {len(transcript)} chars")
        return transcript

    except Exception as e:
        logger.error(f"OpenAI transcription failed: {e}")
        return None


def cleanup_audio(audio_path: str):
    """Remove temporary audio file."""
    try:
        if audio_path and os.path.exists(audio_path):
            os.unlink(audio_path)
            logger.debug(f"Cleaned up audio file: {audio_path}")
    except Exception:
        pass
