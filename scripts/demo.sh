#!/usr/bin/env bash
# Guided demo: starts the mock exchange and Stocky, then walks through each rule from the brief
# with real HTTP calls, real TCP and a live WebSocket stream. Needs curl and jq.
#   ./scripts/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

API=${STOCKY_HTTP:-127.0.0.1:8111}
ADMIN=127.0.0.1:9003
LOGS=$(mktemp -d)

# Refuse to start if another instance is already listening: a demo must never mix with a stale run.
for addr in "$API" 127.0.0.1:9001 127.0.0.1:9002 127.0.0.1:9003; do
  if lsof -nP -iTCP:"${addr##*:}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port ${addr##*:} is already in use (an earlier run still going?). Stop it first:"
    echo "  pkill -f bin/stocky; pkill -f bin/exchange"
    exit 1
  fi
done
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

say()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
price(){ curl -s -X POST "$ADMIN/price" -d "{\"symbol\":\"$1\",\"price\":$2}" >/dev/null; note "exchange: $1 -> ₹$(rupees "$2")"; }
chaos(){ curl -s -X PUT "$ADMIN/chaos" -d "$1" >/dev/null; note "exchange chaos: $1"; }
rupees(){ printf '%d.%02d' $(( $1 / 100 )) $(( $1 % 100 )); }
create(){ curl -s "$API/strategies" -d "$1"; }
show() {
  curl -s "$API/strategies/$1" | jq -r '"   \(.id)  \(.spec.user) \(.spec.side) \(.spec.symbol) x\(.spec.qty)  state=\(.state)" +
    (if .entry_price then "  entry=₹\(.entry_price/100)" else "" end) +
    (if .exit_reason then "  exit=\(.exit_reason) @ ₹\(.exit_price/100)" else "" end) +
    (if .note then "  (\(.note))" else "" end)'
}
account(){ curl -s "$API/accounts/$1" | jq -r --arg u "$1" '"   \($u): margin limit ₹\(.limit/100)  used ₹\(.used/100)  reserved ₹\(.reserved/100)  positions \(.positions)"'; }

say "Building and starting the mock exchange and Stocky"
mkdir -p bin && go build -o bin/ ./cmd/...
bin/exchange -tick 0 2>"$LOGS/exchange.log" &
sleep 0.4
bin/stocky -http "$API" -users "u1=10000000,u2=200000" 2>"$LOGS/stocky.log" &
sleep 0.6
bin/watch -url "ws://$API/ws" -user u1 >"$LOGS/u1-stream.jsonl" 2>/dev/null &
sleep 0.3
note "exchange: orders :9001, market data :9002, admin :9003   stocky: http://$API"
account u1; account u2

say "1. A strategy: buy INFY on a breakout to ₹1,605, target ₹1,620, stoploss ₹1,590"
A=$(create '{"user":"u1","symbol":"INFY","side":"LONG","entry":"MARKET","trigger":160500,"qty":10,"target":162000,"stoploss":159000}' | jq -r .id)
show "$A"
note "the price gaps from ₹1,600 straight past the trigger in one tick:"
price INFY 161200; sleep 0.3
show "$A"
note "market entry went out as a protected limit (last price + 3%) so margin can't be exceeded"
account u1

say "2. Target vs stoploss under a hostile exchange (every message duplicated and delayed)"
chaos '{"duplicate":1,"delay":150000000}'
price INFY 163000      # through the target...
sleep 0.05
price INFY 158000      # ...and straight through the stoploss before the exit's fill arrives
sleep 1.2
show "$A"
note "exchange fills for this strategy:"
curl -s "$ADMIN/fills" | jq -r --arg id "$A" '.[] | select(.ClientID|startswith($id)) | "     \(.ClientID)  \(.Side) \(.Qty) @ ₹\(.Price/100)  exec=\(.ExecID)"'
chaos '{}'

say "3. Margin: u2 has ₹2,000; a ~₹16,000 order is refused by the RMS before it reaches the exchange"
B=$(create '{"user":"u2","symbol":"INFY","side":"LONG","entry":"MARKET","trigger":157000,"qty":10}' | jq -r .id)
price INFY 156900; sleep 0.3
show "$B"; account u2

say "4. Connection drops after the exchange fills, before the fill reaches Stocky"
chaos '{"symbol_delay":{"SBIN":400000000}}'
C=$(create '{"user":"u1","symbol":"SBIN","side":"SHORT","entry":"MARKET","trigger":80000,"qty":25,"target":78000}' | jq -r .id)
sleep 0.15
curl -s -X POST "$ADMIN/drop" >/dev/null; note "exchange: order connection cut while the ACK/FILL were in flight"
chaos '{}'
sleep 2
show "$C"
note "stocky log:"; grep -E 'oms: (dis)?connected' "$LOGS/stocky.log" | tail -2 | sed 's/^/     /'
note "exchange holds $(curl -s "$ADMIN/orders" | jq --arg id "$C" '[keys[] | select(startswith($id))] | length') order(s) for this strategy: resent after reconnect, not duplicated"

say "5. A slow symbol does not hold up others (TCS delayed 3s at the exchange)"
chaos '{"symbol_delay":{"TCS":3000000000}}'
D=$(create '{"user":"u1","symbol":"TCS","side":"LONG","entry":"MARKET","trigger":410000,"qty":1}' | jq -r .id)
E=$(create '{"user":"u1","symbol":"HDFCBANK","side":"LONG","entry":"MARKET","trigger":170000,"qty":5,"target":175000}' | jq -r .id)
sleep 0.3
show "$D"; show "$E"
sleep 3.2
note "3 seconds later:"; show "$D"
chaos '{}'

say "6. Cancel with an open position squares it off (no orphaned position)"
curl -s -X DELETE "$API/strategies/$E" >/dev/null; sleep 0.4
show "$E"

say "7. Partial fills: the stoploss is hit while only part of the entry has filled"
chaos '{"partial":1}'
F=$(create '{"user":"u1","symbol":"RELIANCE","side":"LONG","entry":"MARKET","trigger":290000,"qty":20,"target":300000,"stoploss":285000}' | jq -r .id)
sleep 0.3
curl -s "$API/strategies/$F" | jq -r '"   \(.id)  filled \(.filled_qty) of \(.spec.qty) so far, state=\(.state)"'
chaos '{}'
price RELIANCE 284000; sleep 0.5
show "$F"
curl -s "$API/strategies/$F" | jq -r '"   entered \(.filled_qty), exited \(.exited_qty): the exit covered exactly what was held"'

say "8. Retrying a create with the same client_ref returns the same strategy (no duplicate order)"
R1=$(create '{"user":"u1","symbol":"TCS","side":"LONG","entry":"MARKET","trigger":420000,"qty":1,"client_ref":"form-123"}' | jq -r .id)
R2=$(create '{"user":"u1","symbol":"TCS","side":"LONG","entry":"MARKET","trigger":420000,"qty":1,"client_ref":"form-123"}' | jq -r .id)
note "first: $R1   retry: $R2"

say "9. Reconciliation: Stocky's positions vs the exchange's own fill log"
account u1
curl -s "$ADMIN/fills" | jq -r 'group_by(.Symbol)[] | "   exchange \(.[0].Symbol): net \( map(if .Side=="BUY" then .Qty else -.Qty end) | add )"'

say "10. u1's WebSocket stream ($(wc -l <"$LOGS/u1-stream.jsonl" | tr -d ' ') events); the last few:"
tail -4 "$LOGS/u1-stream.jsonl" | jq -c '{type, id:(.strategy.id // .order.client_id), state:(.strategy.state // .order.kind)}' | sed 's/^/   /'
echo; note "logs: $LOGS"
