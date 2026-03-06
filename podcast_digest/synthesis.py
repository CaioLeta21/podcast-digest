"""AI synthesis: 2-pass (per-episode + cross-synthesis). Supports Claude and Gemini."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .models import EpisodeSummary, CrossSynthesis, Video

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prompt builders (shared across providers)
# ---------------------------------------------------------------------------

def _build_episode_prompt(video: Video, profile: str, calibration: str) -> str:
    return f"""Você é um assistente que analisa episódios de podcast/vídeo para uma curadoria personalizada.

## Perfil de interesses da usuária
{profile}

{calibration}

## Tarefa

Analise a transcrição abaixo e produza (SEMPRE em português brasileiro, mesmo que a transcrição esteja em outro idioma):
1. Um resumo conciso (3-5 parágrafos) dos pontos principais
2. Lista de 3-7 tópicos-chave (em português)
3. Score de relevância de 1 a 10 para o perfil da usuária
4. Justificativa breve do score (em português)

Responda EXCLUSIVAMENTE em JSON válido, sem markdown:
{{
  "summary": "...",
  "key_topics": ["...", "..."],
  "relevance_score": N,
  "relevance_reason": "..."
}}

## Episódio
Título: {video.title}
Canal: {video.channel_id}

## Transcrição
{video.transcript}"""


def _build_single_video_prompt(video: Video, deep: bool = False) -> str:
    if deep:
        task = """Analise a transcrição abaixo EM PROFUNDIDADE e produza (SEMPRE em português brasileiro, mesmo que a transcrição esteja em outro idioma):
1. Um resumo detalhado e abrangente (6-10 parágrafos) cobrindo todos os pontos relevantes, argumentos, dados citados e nuances da discussão
2. Lista de 5-12 tópicos-chave (em português)"""
    else:
        task = """Analise a transcrição abaixo e produza (SEMPRE em português brasileiro, mesmo que a transcrição esteja em outro idioma):
1. Um resumo conciso (3-5 parágrafos) dos pontos principais
2. Lista de 3-7 tópicos-chave (em português)"""

    return f"""Você é um assistente que analisa episódios de podcast/vídeo.

## Tarefa

{task}

Responda EXCLUSIVAMENTE em JSON válido, sem markdown:
{{
  "summary": "...",
  "key_topics": ["...", "..."]
}}

## Episódio
Título: {video.title}
Canal: {video.channel_id}

## Transcrição
{video.transcript}"""


def _build_cross_synthesis_prompt(summaries: List[EpisodeSummary], profile: str) -> str:
    episodes_text = ""
    for s in summaries:
        episodes_text += f"""
### {s.title} (canal: {s.channel_name}, score: {s.relevance_score}/10)
{s.summary}
Tópicos: {', '.join(s.key_topics)}
---
"""
    return f"""Você é um assistente que sintetiza múltiplos episódios de podcast em temas transversais.

## Perfil de interesses da usuária
{profile}

## Episódios do dia
{episodes_text}

## Tarefa

Identifique temas transversais que conectam 2 ou mais episódios. Para cada tema:
1. Nome do tema
2. Síntese de como os episódios se conectam nesse tema
3. IDs dos episódios relevantes

Responda EXCLUSIVAMENTE em JSON válido, sem markdown:
{{
  "themes": [
    {{
      "theme": "Nome do tema",
      "summary": "Como os episódios se conectam...",
      "episodes": ["titulo1", "titulo2"]
    }}
  ]
}}

Se não houver temas transversais claros, retorne {{"themes": []}}."""


def _parse_json_response(text: str) -> dict:
    """Parse JSON from AI response, handling markdown code blocks."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return json.loads(text)


# ---------------------------------------------------------------------------
# Claude backend
# ---------------------------------------------------------------------------

async def _claude_generate(api_key: str, model: str, prompt: str, max_tokens: int) -> str:
    import anthropic
    client = anthropic.AsyncAnthropic(api_key=api_key)
    response = await client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


# ---------------------------------------------------------------------------
# Gemini backend
# ---------------------------------------------------------------------------

async def _gemini_generate(api_key: str, model: str, prompt: str, max_tokens: int) -> str:
    """Call Gemini API via google-genai SDK (async-compatible via thread)."""
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key)

    # google-genai SDK is sync; run in executor to avoid blocking
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(
        None,
        lambda: client.models.generate_content(
            model=model,
            contents=prompt,
            config=types.GenerateContentConfig(
                max_output_tokens=max_tokens,
                temperature=0.3,
            ),
        ),
    )
    return response.text


# ---------------------------------------------------------------------------
# Provider dispatcher
# ---------------------------------------------------------------------------

async def _ai_generate(config: dict, prompt: str, max_tokens: int) -> str:
    """Route to the configured AI provider."""
    provider = config["_ai_provider"]

    if provider == "gemini":
        api_key = config["gemini"].get("api_key", "")
        if not api_key:
            raise RuntimeError(
                "GEMINI_API_KEY not set. Export it as an environment variable."
            )
        model = config["gemini"].get("model", "gemini-2.0-flash")
        return await _gemini_generate(api_key, model, prompt, max_tokens)
    else:
        api_key = config["claude"].get("api_key", "")
        if not api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY not set. Export it as an environment variable."
            )
        model = config["claude"]["model"]
        return await _claude_generate(api_key, model, prompt, max_tokens)


# ---------------------------------------------------------------------------
# Synthesis pipeline
# ---------------------------------------------------------------------------

async def synthesize_episode(
    video: Video,
    profile: str,
    calibration: str,
    config: dict,
    channel_name: str,
) -> Optional[EpisodeSummary]:
    """Pass 1: Summarize and score a single episode with retry on rate limit."""
    if not video.transcript:
        return None

    prompt = _build_episode_prompt(video, profile, calibration)
    max_tokens = config["claude"]["max_tokens_summary"]

    max_retries = 3
    for attempt in range(max_retries + 1):
        try:
            text = await _ai_generate(config, prompt, max_tokens)
            data = _parse_json_response(text)

            return EpisodeSummary(
                video_id=video.video_id,
                title=video.title,
                channel_name=channel_name,
                url=video.url,
                summary=data["summary"],
                key_topics=data["key_topics"],
                relevance_score=int(data["relevance_score"]),
                relevance_reason=data["relevance_reason"],
                duration_seconds=video.duration_seconds,
                transcript_available=True,
            )

        except json.JSONDecodeError as e:
            if attempt < max_retries:
                logger.warning(f"JSON parse error for {video.video_id}, retrying ({attempt+1}/{max_retries})...")
                await asyncio.sleep(5)
                continue
            logger.error(f"JSON parse error for {video.video_id}: {e}")
            return None
        except Exception as e:
            err_str = str(e)
            if "429" in err_str and attempt < max_retries:
                wait = 60 * (attempt + 1)
                logger.warning(f"Rate limited for {video.video_id}, waiting {wait}s ({attempt+1}/{max_retries})...")
                await asyncio.sleep(wait)
                continue
            logger.error(f"Synthesis error for {video.video_id}: {e}")
            return None
    return None


async def synthesize_single_video(
    video: Video,
    config: dict,
    channel_name: str,
    deep: bool = False,
) -> Optional[dict]:
    """Synthesize a single video without profile/relevance scoring."""
    if not video.transcript:
        return None

    prompt = _build_single_video_prompt(video, deep=deep)
    max_tokens = config["claude"]["max_tokens_summary"]
    if deep:
        max_tokens = max_tokens * 2

    try:
        text = await _ai_generate(config, prompt, max_tokens)
        data = _parse_json_response(text)
        return {
            "title": video.title,
            "channel_name": channel_name,
            "summary": data["summary"],
            "key_topics": data["key_topics"],
            "duration_seconds": video.duration_seconds,
        }
    except Exception as e:
        logger.error(f"Single video synthesis error for {video.video_id}: {e}")
        raise


async def synthesize_cross(
    summaries: List[EpisodeSummary],
    profile: str,
    config: dict,
) -> Optional[CrossSynthesis]:
    """Pass 2: Cross-episode thematic synthesis."""
    if len(summaries) < 2:
        return None

    prompt = _build_cross_synthesis_prompt(summaries, profile)
    max_tokens = config["claude"]["max_tokens_synthesis"]

    try:
        text = await _ai_generate(config, prompt, max_tokens)
        data = _parse_json_response(text)

        return CrossSynthesis(
            themes=data["themes"],
            generated_at=datetime.now(),
        )

    except Exception as e:
        logger.error(f"Cross-synthesis error: {e}")
        return None


async def run_synthesis(
    videos: List[Video],
    profile: str,
    calibration: str,
    config: dict,
    channel_names: Dict[str, str],
) -> Tuple[List[EpisodeSummary], Optional[CrossSynthesis]]:
    """Run full 2-pass synthesis pipeline."""
    # Pass 1: sequential with rate limiting for free tier APIs
    provider = config["_ai_provider"]
    eligible = [v for v in videos if v.transcript]
    logger.info(f"Pass 1: synthesizing {len(eligible)} episodes via {provider}...")
    summaries = []
    for i, video in enumerate(eligible):
        name = channel_names.get(video.channel_id, video.channel_id)
        result = await synthesize_episode(
            video, profile, calibration, config, name
        )
        if result:
            summaries.append(result)
            logger.info(f"  [{i+1}/{len(eligible)}] OK: {video.title[:60]}")
        else:
            logger.warning(f"  [{i+1}/{len(eligible)}] FAIL: {video.title[:60]}")
        # Rate limit: wait between requests (free tier = 5 req/min)
        if i < len(eligible) - 1:
            await asyncio.sleep(15)
    logger.info(f"Pass 1 complete: {len(summaries)} summaries generated")

    # Pass 2: cross-episode synthesis
    cross = None
    if len(summaries) >= 2:
        logger.info("Pass 2: cross-episode synthesis...")
        cross = await synthesize_cross(summaries, profile, config)
        if cross:
            logger.info(f"Pass 2 complete: {len(cross.themes)} themes found")

    return summaries, cross
