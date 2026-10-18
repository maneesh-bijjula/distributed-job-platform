#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
docker compose -p job-platform --profile test run --rm test
