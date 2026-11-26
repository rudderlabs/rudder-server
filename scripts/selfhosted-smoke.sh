#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

WRITE_KEY="1samplewritekeyplaceholder123"
HEALTH_URL="http://localhost:8080/health"
TRACK_URL="http://localhost:8080/v1/track"

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker-compose"
else
  echo "ERROR: neither 'docker compose' nor 'docker-compose' found in PATH." >&2
  exit 1
fi

COMPOSE_FILES=("-f" "docker-compose.yml" "-f" "docker-compose.selfhosted.yml")

cleanup() {
  echo "Cleaning up containers..."
  $DOCKER_COMPOSE "${COMPOSE_FILES[@]}" down -v || true
}
trap cleanup EXIT

echo "Starting self-hosted RudderStack stack..."
$DOCKER_COMPOSE "${COMPOSE_FILES[@]}" up -d --build

echo "Waiting for health check at $HEALTH_URL..."
READY=0
for _ in $(seq 1 60); do
  if RES=$(curl -sf "$HEALTH_URL" 2>/dev/null); then
    if echo "$RES" | grep -q '"backendConfigMode":"JSON"'; then
      echo "Health check passed: $RES"
      READY=1
      break
    fi
  fi
  sleep 2
done

if [ "$READY" -ne 1 ]; then
  echo "ERROR: Health check timed out or did not return backendConfigMode: JSON" >&2
  $DOCKER_COMPOSE "${COMPOSE_FILES[@]}" logs backend || true
  exit 1
fi

echo "Sending track event to $TRACK_URL..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$TRACK_URL" \
  -u "${WRITE_KEY}:" \
  -H "Content-Type: application/json" \
  -d '{"userId":"selfhosted-test-user","event":"SelfHosted Smoke Test Event","properties":{"status":"testing"}}')

if [ "$HTTP_CODE" -eq 200 ]; then
  echo "Track event accepted: HTTP $HTTP_CODE"
else
  echo "ERROR: Expected HTTP 200 from /v1/track, got $HTTP_CODE" >&2
  exit 1
fi

echo "Self-hosted smoke test completed successfully!"
