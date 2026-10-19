"""Load credentials for local verification without displaying or executing .env content."""
import os
from pathlib import Path


def api_key():
    token = os.getenv("JOB_API_KEY") or os.getenv("JOB_BOOTSTRAP_API_KEY")
    if token:
        return token
    path = Path(__file__).resolve().parent.parent / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip().startswith("JOB_BOOTSTRAP_API_KEY="):
                return line.strip().split("=", 1)[1].strip().strip("\"'")
    raise SystemExit("No API key found. Run python3 scripts/init-auth.py or set JOB_API_KEY.")
