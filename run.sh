#!/bin/bash
# podcast-digest daily runner
# Used by launchd for scheduled execution

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment variables (API keys, email credentials)
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

# Activate virtual environment if it exists
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
fi

# Run the pipeline
python -m podcast_digest run 2>&1

echo "$(date): Pipeline completed" >> "$SCRIPT_DIR/data/cron.log"
