#!/usr/bin/env bash
# Starts the mock exchange and Stocky and keeps them running; open the console in a browser.
#   ./scripts/run.sh            prices drift on their own (with occasional gaps)
#   TICK=0 ./scripts/run.sh     prices move only when you move them
set -euo pipefail
cd "$(dirname "$0")/.."
API=${STOCKY_HTTP:-127.0.0.1:8111}
mkdir -p bin && go build -o bin/ ./cmd/...
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
bin/exchange -tick "${TICK:-1s}" -gap-prob 0.05 &
sleep 0.4
bin/stocky -http "$API" &
sleep 0.5
echo
echo "  Stocky console:  http://$API/"
echo "  Ctrl-C to stop."
wait
