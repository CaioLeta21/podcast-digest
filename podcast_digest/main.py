"""Orchestrator and CLI for podcast-digest."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

from .channels import fetch_new_videos
from .config import get_channel_rss_url, load_config
from .database import Database
from .docx_writer import render_digest_docx
from .emailer import render_digest_html, send_digest_email
from .models import DigestResult, EpisodeSummary, FeedbackEntry, Video
from .person_search import search_all_people
from .profile import build_calibration_context, load_profile
from .synthesis import run_synthesis
from .transcripts import fetch_transcripts_batch

logger = logging.getLogger("podcast_digest")


def setup_logging(config: dict):
    level = getattr(logging, config["logging"]["level"].upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    handlers = [logging.StreamHandler(sys.stdout)]
    log_file = config["logging"]["file"]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(level=level, format=fmt, handlers=handlers)


def run_pipeline(config: dict, dry_run: bool = False, lookback_days: int = None):
    """Run the full digest pipeline."""
    # Override lookback if specified
    if lookback_days is not None:
        config["processing"]["lookback_hours"] = lookback_days * 24
        logger.info(f"Lookback window: {lookback_days} days ({lookback_days * 24}h)")

    db = Database(config["database"]["path"])

    # Register channels
    channel_names = {}
    for ch in config["channels"]:
        rss_url = get_channel_rss_url(ch["id"])
        db.upsert_channel(ch["id"], ch["name"], rss_url)
        channel_names[ch["id"]] = ch["name"]

    # Step 1: Fetch new videos
    logger.info(f"Checking {len(config['channels'])} channels...")
    videos = fetch_new_videos(config)
    logger.info(f"Found {len(videos)} new videos")

    # Step 2: Deduplicate against database
    new_videos = [v for v in videos if not db.video_exists(v.video_id)]
    logger.info(f"{len(new_videos)} videos not yet processed")

    if not new_videos:
        logger.info("No new videos to process.")

        # Retry transcripts for videos previously saved without one
        lookback_hours = config["processing"]["lookback_hours"]
        retry_rows = db.get_videos_without_transcript(lookback_hours)
        if retry_rows:
            retry_videos = [
                Video(
                    video_id=r["video_id"], channel_id=r["channel_id"],
                    title=r["title"], published_at=datetime.fromisoformat(r["published_at"]),
                    duration_seconds=r["duration_seconds"], url=r["url"],
                    source=r.get("source", "channel"),
                )
                for r in retry_rows
            ]
            logger.info(f"Retrying transcripts for {len(retry_videos)} previously failed videos...")
            retry_videos = fetch_transcripts_batch(retry_videos, config)
            retry_ok = [v for v in retry_videos if v.transcript]
            if retry_ok:
                logger.info(f"Recovered {len(retry_ok)} transcripts on retry")
                for v in retry_ok:
                    db.update_video_transcript(v.video_id, v.transcript, v.transcript_language)
                # Synthesize recovered videos
                profile = load_profile(config)
                calibration = build_calibration_context(db, config)
                new_summaries, _ = asyncio.run(
                    run_synthesis(retry_ok, profile, calibration, config, channel_names)
                )
                today = datetime.now().strftime("%Y-%m-%d")
                for s in new_summaries:
                    db.save_digest(
                        s.video_id, s.summary, s.key_topics,
                        s.relevance_score, s.relevance_reason, today,
                    )
                if new_summaries:
                    logger.info(f"Synthesized {len(new_summaries)} recovered episodes")

        # Generate report with all existing digests from this period
        valid_channel_ids = [ch["id"] for ch in config["channels"]]
        existing_digests = db.get_digests_for_period(lookback_hours, valid_channel_ids)

        if not existing_digests:
            logger.info("No existing digests for this period either. Done.")
            return

        logger.info(f"Found {len(existing_digests)} previously processed episodes for this period")
        summaries = []
        for d in existing_digests:
            topics = d["key_topics"].split(",") if d["key_topics"] else []
            summaries.append(EpisodeSummary(
                video_id=d["video_id"],
                title=d["title"],
                channel_name=channel_names.get(d["channel_id"], d["channel_id"]),
                url=d["url"],
                summary=d["summary"],
                key_topics=topics,
                relevance_score=d["relevance_score"],
                relevance_reason=d["relevance_reason"],
                duration_seconds=d.get("duration_seconds", 0),
                transcript_available=True,
            ))

        digest = DigestResult(
            date=datetime.now().strftime("%Y-%m-%d"),
            episodes=summaries,
            cross_synthesis=None,
            no_transcript=[],
            total_channels_checked=len(config["channels"]),
            total_new_episodes=0,
            person_episodes=[],
            person_names=[],
        )

        days_label = lookback_days or lookback_hours // 24 or 1
        docx_filename = f"podcast_digest_{digest.date}_{days_label}d.docx"
        docx_path = str(Path(config["database"]["path"]).parent / docx_filename)
        min_rel = config["processing"].get("min_relevance_score", 0)
        render_digest_docx(digest, docx_path, min_relevance=min_rel)

        html = render_digest_html(digest)
        preview_path = config["database"]["path"].replace(
            "podcast_digest.db", "preview.html"
        )
        with open(preview_path, "w", encoding="utf-8") as f:
            f.write(html)

        for s in sorted(summaries, key=lambda x: x.relevance_score, reverse=True):
            logger.info(f"  [{s.relevance_score}/10] {s.title[:60]} ({s.channel_name})")
        logger.info(f"DOCX salvo em: {docx_path}")
        logger.info("Pipeline complete.")
        return

    # Step 3: Fetch transcripts
    logger.info("Fetching transcripts...")
    new_videos = fetch_transcripts_batch(new_videos, config)

    # Filter by minimum duration (now that we have duration from transcripts)
    min_duration = config["processing"]["min_duration_minutes"] * 60
    short_videos = [v for v in new_videos if 0 < v.duration_seconds < min_duration]
    new_videos = [v for v in new_videos if v.duration_seconds == 0 or v.duration_seconds >= min_duration]

    if short_videos:
        logger.info(f"Filtered out {len(short_videos)} short videos (<{config['processing']['min_duration_minutes']}min)")

    with_transcript = [v for v in new_videos if v.transcript]
    no_transcript = [v for v in new_videos if not v.transcript]

    logger.info(
        f"Transcripts: {len(with_transcript)} OK, "
        f"{len(no_transcript)} unavailable"
    )

    # Step 4: Save videos to DB
    for v in new_videos:
        db.save_video(
            v.video_id, v.channel_id, v.title, v.published_at,
            v.duration_seconds, v.url, v.transcript, v.transcript_language,
        )

    # Step 4b: Retry transcripts for videos previously saved without one
    retry_rows = db.get_videos_without_transcript(config["processing"]["lookback_hours"])
    if retry_rows:
        retry_videos = []
        for r in retry_rows:
            # Skip videos we just processed (already in new_videos)
            if any(v.video_id == r["video_id"] for v in new_videos):
                continue
            retry_videos.append(Video(
                video_id=r["video_id"],
                channel_id=r["channel_id"],
                title=r["title"],
                published_at=datetime.fromisoformat(r["published_at"]),
                duration_seconds=r["duration_seconds"],
                url=r["url"],
                source=r.get("source", "channel"),
            ))

        if retry_videos:
            logger.info(f"Retrying transcripts for {len(retry_videos)} previously failed videos...")
            retry_videos = fetch_transcripts_batch(retry_videos, config)
            retry_ok = [v for v in retry_videos if v.transcript]
            if retry_ok:
                logger.info(f"Recovered {len(retry_ok)} transcripts on retry")
                for v in retry_ok:
                    db.update_video_transcript(v.video_id, v.transcript, v.transcript_language)
                with_transcript.extend(retry_ok)

    # Step 5: AI synthesis
    summaries = []
    cross_synthesis = None

    if with_transcript:
        profile = load_profile(config)
        calibration = build_calibration_context(db, config)

        summaries, cross_synthesis = asyncio.run(
            run_synthesis(with_transcript, profile, calibration, config, channel_names)
        )

        # Save digests to DB
        today = datetime.now().strftime("%Y-%m-%d")
        for s in summaries:
            db.save_digest(
                s.video_id, s.summary, s.key_topics,
                s.relevance_score, s.relevance_reason, today,
            )

    # Step 5b: Person search (if today matches configured day)
    person_summaries = []
    person_names = []
    person_no_transcript = []
    search_cfg = config.get("person_search", {})
    if search_cfg.get("enabled", True):
        target_day = search_cfg.get("day_of_week", 0)
        if datetime.now().weekday() == target_day or dry_run:
            person_summaries, person_names, person_no_transcript = _run_person_search(config, db, channel_names)

    # Step 6: Include previously processed digests from the same period
    valid_channel_ids = [ch["id"] for ch in config["channels"]]
    lookback_hours = config["processing"]["lookback_hours"]
    existing_digests = db.get_digests_for_period(lookback_hours, valid_channel_ids)

    # Merge: add existing digests not already in current summaries
    current_video_ids = {s.video_id for s in summaries}
    for d in existing_digests:
        if d["video_id"] not in current_video_ids:
            topics = d["key_topics"].split(",") if d["key_topics"] else []
            summaries.append(EpisodeSummary(
                video_id=d["video_id"],
                title=d["title"],
                channel_name=channel_names.get(d["channel_id"], d["channel_id"]),
                url=d["url"],
                summary=d["summary"],
                key_topics=topics,
                relevance_score=d["relevance_score"],
                relevance_reason=d["relevance_reason"],
                duration_seconds=d.get("duration_seconds", 0),
                transcript_available=True,
            ))

    if len(summaries) > len(current_video_ids):
        logger.info(
            f"Including {len(summaries) - len(current_video_ids)} previously processed episodes from this period"
        )

    # Build result
    digest = DigestResult(
        date=datetime.now().strftime("%Y-%m-%d"),
        episodes=summaries,
        cross_synthesis=cross_synthesis,
        no_transcript=no_transcript,
        total_channels_checked=len(config["channels"]),
        total_new_episodes=len(new_videos),
        person_episodes=person_summaries,
        person_names=person_names,
        person_no_transcript=person_no_transcript,
    )

    # Step 7: Generate output
    # .docx report (default)
    days_label = lookback_days or config["processing"]["lookback_hours"] // 24 or 1
    docx_filename = f"podcast_digest_{digest.date}_{days_label}d.docx"
    docx_path = str(Path(config["database"]["path"]).parent / docx_filename)
    min_rel = config["processing"].get("min_relevance_score", 0)
    render_digest_docx(digest, docx_path, min_relevance=min_rel)

    # Also save HTML preview
    html = render_digest_html(digest)
    preview_path = config["database"]["path"].replace(
        "podcast_digest.db", "preview.html"
    )
    with open(preview_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"Episodes: {len(summaries)} summarized, {len(no_transcript)} without transcript")
    if person_summaries:
        logger.info(f"Person search: {len(person_summaries)} episodes")
    for s in sorted(summaries, key=lambda x: x.relevance_score, reverse=True):
        logger.info(f"  [{s.relevance_score}/10] {s.title} ({s.channel_name})")

    logger.info(f"DOCX salvo em: {docx_path}")
    logger.info("Pipeline complete.")


def run_feedback(config: dict):
    """Interactive feedback session."""
    db = Database(config["database"]["path"])
    digests = db.get_recent_digests(days=7)

    if not digests:
        print("Nenhum digest recente encontrado (últimos 7 dias).")
        return

    print(f"\nFeedback session: {len(digests)} episódios dos últimos 7 dias")
    print("Para cada episódio, dê uma nota de 1-10 (ou Enter para pular, 'q' para sair)\n")

    count = 0
    for d in digests:
        print(f"[AI: {d['relevance_score']}/10] {d['title']}")
        print(f"  {d['url']}")
        print(f"  {d['summary'][:150]}...")

        while True:
            response = input("  Sua nota (1-10, Enter=pular, q=sair): ").strip()

            if response == "":
                break
            if response.lower() == "q":
                print(f"\n{count} ratings salvos.")
                return

            try:
                score = int(response)
                if not 1 <= score <= 10:
                    print("  Use um número de 1 a 10.")
                    continue
            except ValueError:
                print("  Use um número de 1 a 10.")
                continue

            comment = input("  Comentário (opcional, Enter=pular): ").strip()

            entry = FeedbackEntry(
                video_id=d["video_id"],
                user_score=score,
                comment=comment,
            )
            db.save_feedback(entry)
            count += 1
            break

        print()

    print(f"\n{count} ratings salvos. Obrigado pelo feedback!")


def _run_person_search(config: dict, db: Database, channel_names: dict):
    """Run person search, transcribe, synthesize, and return summaries + unsummarized videos."""
    logger.info("Running person search...")
    all_results = search_all_people(config)

    if not all_results:
        logger.info("No person search results found.")
        return [], [], []

    # Collect all unique videos, dedup against DB
    person_videos = []
    person_names = list(all_results.keys())

    for name, results in all_results.items():
        person_id = db.add_person(name)
        new_count = 0
        for query, video in results:
            if db.video_exists(video.video_id):
                # Still link person to existing video
                db.link_person_video(person_id, video.video_id, query)
                continue

            person_videos.append(video)
            # Save video to DB
            db.save_video_with_source(
                video.video_id, video.channel_id, video.title,
                video.published_at, video.duration_seconds, video.url,
                source="person_search",
            )
            db.link_person_video(person_id, video.video_id, query)
            new_count += 1

        db.log_person_search(person_id, new_count)
        logger.info(f"  {name}: {new_count} new videos")

    if not person_videos:
        logger.info("All person search videos already processed.")
        return [], person_names, []

    # Fetch transcripts
    logger.info(f"Fetching transcripts for {len(person_videos)} person search videos...")
    person_videos = fetch_transcripts_batch(person_videos, config)
    with_transcript = [v for v in person_videos if v.transcript]
    no_transcript = [v for v in person_videos if not v.transcript]

    if not with_transcript:
        logger.info("No transcripts available for person search videos.")
        return [], person_names, no_transcript

    # Synthesize
    profile = load_profile(config)
    calibration = build_calibration_context(db, config)

    # Build channel_names for person videos (use channel_id as fallback)
    for v in with_transcript:
        if v.channel_id not in channel_names:
            channel_names[v.channel_id] = db.get_channel_name(v.channel_id)

    summaries, _ = asyncio.run(
        run_synthesis(with_transcript, profile, calibration, config, channel_names)
    )

    # Save digests
    today = datetime.now().strftime("%Y-%m-%d")
    for s in summaries:
        db.save_digest(
            s.video_id, s.summary, s.key_topics,
            s.relevance_score, s.relevance_reason, today,
        )

    logger.info(f"Person search: {len(summaries)} episodes synthesized")
    return summaries, person_names, no_transcript


def run_scan_people(config: dict, dry_run: bool = False):
    """Standalone person search (CLI command)."""
    db = Database(config["database"]["path"])

    # Register channels for name resolution
    channel_names = {}
    for ch in config.get("channels", []):
        channel_names[ch["id"]] = ch["name"]

    logger.info("Scanning for tracked people...")
    all_results = search_all_people(config)

    if not all_results:
        logger.info("No results found.")
        return

    for name, results in all_results.items():
        logger.info(f"\n{name}: {len(results)} videos found")
        person_id = db.add_person(name)
        new_count = 0

        for query, video in results:
            already = db.video_exists(video.video_id)
            status = "(already in DB)" if already else "(NEW)"
            logger.info(f"  [{video.duration_seconds // 60}min] {video.title} {status}")

            if not already:
                new_count += 1
                if not dry_run:
                    db.save_video_with_source(
                        video.video_id, video.channel_id, video.title,
                        video.published_at, video.duration_seconds, video.url,
                        source="person_search",
                    )
                    db.link_person_video(person_id, video.video_id, query)

        if not dry_run:
            db.log_person_search(person_id, new_count)

        logger.info(f"  {new_count} new, {len(results) - new_count} already in DB")

    if dry_run:
        logger.info("\nDry run: no changes saved to database.")


def main():
    parser = argparse.ArgumentParser(
        prog="podcast_digest",
        description="Daily AI-powered podcast monitoring and synthesis",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run the full digest pipeline")
    run_parser.add_argument(
        "--dry-run", action="store_true",
        help="Process everything but don't send email (saves HTML preview)",
    )
    run_parser.add_argument(
        "--lookback-days", type=int, default=None,
        choices=[1, 3, 5, 7],
        help="Time window for analysis in days (1, 3, 5, or 7)",
    )
    run_parser.add_argument(
        "--config", type=str, default=None,
        help="Path to config.yaml (default: project root)",
    )

    # Feedback command
    fb_parser = subparsers.add_parser("feedback", help="Interactive feedback session")
    fb_parser.add_argument(
        "--config", type=str, default=None,
        help="Path to config.yaml",
    )

    # Scan-people command
    sp_parser = subparsers.add_parser("scan-people", help="Search for tracked people on YouTube")
    sp_parser.add_argument(
        "--dry-run", action="store_true",
        help="Show results without saving to database",
    )
    sp_parser.add_argument(
        "--config", type=str, default=None,
        help="Path to config.yaml",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    config = load_config(args.config)
    setup_logging(config)

    if args.command == "run":
        run_pipeline(config, dry_run=args.dry_run, lookback_days=args.lookback_days)
    elif args.command == "feedback":
        run_feedback(config)
    elif args.command == "scan-people":
        run_scan_people(config, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
