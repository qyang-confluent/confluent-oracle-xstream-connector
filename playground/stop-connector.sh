#!/bin/bash
# Stop and remove the XStream connector container. Standalone offsets live inside the
# container, so removing it makes the next start begin from a fresh offset.
# Usage: ./stop-connector.sh [--keep]   (--keep: stop only, so `docker start` resumes with offsets)
set -euo pipefail
CONTAINER=${CONTAINER:-xstream-connect}

if ! docker container inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "Container $CONTAINER not found"; exit 0
fi
docker stop "$CONTAINER" >/dev/null
[[ "${1:-}" == "--keep" ]] || docker rm "$CONTAINER" >/dev/null
echo "Stopped $CONTAINER"
