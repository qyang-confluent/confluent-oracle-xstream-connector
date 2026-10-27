#!/bin/bash
# Build (if needed) and start the XStream connector in a standalone Connect container.
# Usage: ./start-connector.sh [--rebuild]
# Env overrides: OUT_SERVER (override database.out.server.name), CONFIG_DIR, CONTAINER, PORT
set -euo pipefail
cd "$(dirname "$0")"

CONTAINER=${CONTAINER:-xstream-connect}
CONFIG_DIR=${CONFIG_DIR:-$PWD/terraform/generated}
PORT=${PORT:-8083}
OUT_SERVER=${OUT_SERVER:-}

# Pick Instant Client zip matching the host architecture
case "$(uname -m)" in
  arm64|aarch64)
    PLATFORM=linux/arm64; TAG=arm64
    IC_ZIP=instantclient-basic-linux.arm64-19.32.0.0.0dbru.zip; IC_DIR=instantclient_19_32 ;;
  *)
    PLATFORM=linux/amd64; TAG=amd64
    IC_ZIP=instantclient-basic-linux.x64-19.26.0.0.0dbru.zip; IC_DIR=instantclient_19_26 ;;
esac
IMAGE=xstream-connect:$TAG

if [[ "${1:-}" == "--rebuild" ]] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "Building $IMAGE ..."
  docker build --platform "$PLATFORM" -t "$IMAGE" \
    --build-arg IC_ZIP="$IC_ZIP" --build-arg IC_DIR="$IC_DIR" -f Dockerfile asset
fi

for f in cp-local-worker.config oracle-xstream-rac.config; do
  [[ -f "$CONFIG_DIR/$f" ]] || { echo "Missing $CONFIG_DIR/$f (run terraform apply first)"; exit 1; }
done

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" --platform "$PLATFORM" -p "$PORT:8083" \
  -v "$CONFIG_DIR:/cfg:ro" -e OUT_SERVER="$OUT_SERVER" \
  --entrypoint bash "$IMAGE" -c '
    mkdir -p /tmp/cfg && cp /cfg/* /tmp/cfg/ && cd /tmp/cfg
    sed -i "s#^plugin.path=.*#plugin.path=/usr/share/java,/usr/share/confluent-hub-components#" cp-local-worker.config
    if [ -n "$OUT_SERVER" ]; then
      sed -i -E "s#(\"database.out.server.name\" *: *)\"[^\"]*\"#\1\"$OUT_SERVER\"#" oracle-xstream-rac.config
    fi
    exec connect-standalone cp-local-worker.config oracle-xstream-rac.config' >/dev/null

echo "Started container $CONTAINER ($IMAGE). Logs: docker logs -f $CONTAINER"
echo "Status:  curl -s localhost:$PORT/connectors/<name>/status"
