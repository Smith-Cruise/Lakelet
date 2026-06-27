#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/integration-tests/docker-compose.yml"

cd "${REPO_ROOT}"

cleanup() {
  if [[ "${KEEP_COMPOSE:-0}" != "1" ]]; then
    docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans
  else
    echo "KEEP_COMPOSE=1, leaving integration test containers running"
  fi
}
trap cleanup EXIT

python3 -m pip install -r integration-tests/requirements.txt
integration-tests/download-jars.sh

docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans

if lsof -nP -iTCP:5050 -sTCP:LISTEN >/tmp/dobbydb-integration-port-5050.txt 2>/dev/null; then
  echo "Port 5050 is already in use:" >&2
  cat /tmp/dobbydb-integration-port-5050.txt >&2
  exit 1
fi

docker compose -f "${COMPOSE_FILE}" up -d --wait
cargo build -p dobbydb-app --bin dobbydb
python3 integration-tests/provision.py
python3 -m pytest -s integration-tests
