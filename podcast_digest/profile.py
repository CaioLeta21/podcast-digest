"""User profile and feedback loop management."""

import logging
from pathlib import Path

from .database import Database

logger = logging.getLogger(__name__)


def load_profile(config: dict) -> str:
    """Load user interest profile from markdown file."""
    profile_path = config["profile"]["path"]
    path = Path(profile_path)

    if not path.exists():
        logger.warning(f"Profile not found at {profile_path}, using empty profile")
        return ""

    text = path.read_text(encoding="utf-8")
    logger.info(f"Loaded profile ({len(text)} chars) from {profile_path}")
    return text


def build_calibration_context(db: Database, config: dict) -> str:
    """Build calibration examples from recent feedback for the Claude prompt.

    Returns a formatted string with user ratings vs AI ratings.
    """
    limit = config["profile"].get("feedback_context_size", 50)
    feedback = db.get_recent_feedback(limit=limit)

    if not feedback:
        return ""

    lines = ["## Calibração de relevância (feedback recente da usuária)\n"]
    lines.append(
        "Use estes exemplos para calibrar seus scores. "
        "O score da usuária é a referência correta.\n"
    )

    for fb in feedback:
        ai_score = fb.get("ai_score", "?")
        user_score = fb["user_score"]
        title = fb["title"]
        comment = fb.get("comment", "")

        line = f"- \"{title}\" | AI: {ai_score} → Usuária: {user_score}"
        if comment:
            line += f" ({comment})"
        lines.append(line)

    return "\n".join(lines)
