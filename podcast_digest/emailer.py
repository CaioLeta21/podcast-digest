"""Email HTML digest via SMTP."""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from .models import DigestResult

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"


def render_digest_html(digest: DigestResult) -> str:
    """Render digest result to HTML using Jinja2 template."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    template = env.get_template("digest_email.html")

    return template.render(
        date=digest.date,
        total_channels=digest.total_channels_checked,
        total_episodes=digest.total_new_episodes,
        episodes=digest.episodes,
        cross_synthesis=digest.cross_synthesis,
        no_transcript=digest.no_transcript,
        person_episodes=digest.person_episodes,
        person_names=digest.person_names,
    )


def send_digest_email(digest: DigestResult, config: dict):
    """Send digest email via SMTP."""
    email_cfg = config["email"]

    from_addr = email_cfg["address"]
    password = email_cfg["password"]
    to_addr = email_cfg["to_address"] or from_addr
    smtp_server = email_cfg["smtp_server"]
    smtp_port = email_cfg["smtp_port"]

    if not from_addr or not password:
        raise RuntimeError(
            "Email credentials not configured. "
            "Set EMAIL_ADDRESS and EMAIL_PASSWORD environment variables."
        )

    html = render_digest_html(digest)

    # Count high-relevance episodes for subject line
    high_count = sum(1 for e in digest.episodes if e.relevance_score >= 7)

    subject = f"{email_cfg['subject_prefix']} {digest.date}"
    if high_count > 0:
        subject += f" ({high_count} alta relevância)"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr

    # Plain text fallback
    plain = _render_plain_text(digest)
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    logger.info(f"Sending digest to {to_addr}...")

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        if email_cfg.get("use_tls", True):
            server.starttls()
        server.login(from_addr, password)
        server.send_message(msg)

    logger.info("Email sent successfully")


def _render_plain_text(digest: DigestResult) -> str:
    """Simple plain-text fallback."""
    lines = [f"PODCAST DIGEST - {digest.date}", ""]
    lines.append(
        f"{digest.total_channels_checked} canais | "
        f"{digest.total_new_episodes} episódios novos"
    )
    lines.append("")

    if digest.person_episodes:
        lines.append("PESSOAS RASTREADAS")
        lines.append(f"({', '.join(digest.person_names)})")
        lines.append("")
        for ep in sorted(digest.person_episodes, key=lambda e: e.relevance_score, reverse=True):
            lines.append(f"[{ep.relevance_score}/10] {ep.title}")
            lines.append(f"  Canal: {ep.channel_name}")
            lines.append(f"  {ep.url}")
            lines.append(f"  {ep.summary[:200]}...")
            lines.append("")

    for ep in sorted(digest.episodes, key=lambda e: e.relevance_score, reverse=True):
        lines.append(f"[{ep.relevance_score}/10] {ep.title}")
        lines.append(f"  Canal: {ep.channel_name}")
        lines.append(f"  {ep.url}")
        lines.append(f"  {ep.summary[:200]}...")
        lines.append("")

    if digest.no_transcript:
        lines.append("SEM TRANSCRIÇÃO:")
        for v in digest.no_transcript:
            lines.append(f"  - {v.title}: {v.url}")

    return "\n".join(lines)
