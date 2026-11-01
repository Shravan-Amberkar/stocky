# Stocky — automated entry/exit strategies

![ci](https://github.com/Shravan-Amberkar/stocky/actions/workflows/ci.yml/badge.svg)

A user defines a strategy: symbol, long or short, a market or limit entry fired at a trigger price,
a quantity, and an optional target and/or stoploss. Stocky watches prices, enters when the trigger
is crossed, and exits at the target or the stoploss (never both).

Go 1.26, standard library plus one dependency (`github.com/coder/websocket`). Everything is in memory.

```
                       ┌──────────────────────── stocky ─────────────────────────┐
 HTTP  /strategies ───▶│ engine: one actor per symbol ── orders ──▶ rms.Gateway ──┼─▶ oms ══TCP══╗
 WS    /ws?user= ◀─────│   (owns its strategies,       ◀─ validated ─  (margin,    │   (only rms  ║
                       │    single-threaded)              executions   exec check) │    can import)║
                       │        ▲ ticks                                            │              ║
                       │   feed ═╪═══════════════════════ TCP ═══════════════════════════════╗     ║
                       └────────┴──────────────────────────────────────────────────┘        ║     ║
                       ┌──────────────────── mock exchange (separate process) ──────────────╨─────╨─┐
                       │ price simulator (gaps) · idempotent order book · matcher · chaos toggles   │
                       └────────────────────────────────────────────────────────────────────────────┘
```

## Run

**Demo console**: `./scripts/run.sh`, then open http://127.0.0.1:8111/. It is a small page served by
Stocky (optional, not part of the brief) showing live prices with buttons to move and gap them,
switches for every exchange misbehaviour, a create-strategy form, the strategy table, the margin
bar, Stocky's positions checked against the exchange's fills, and the user's live WebSocket stream.

**Guided demo** (needs `curl` and `jq`): `./scripts/demo.sh` builds both binaries, starts them, and
walks through every rule with real HTTP calls, real TCP and a live WebSocket stream: a gap entry,
target vs stoploss under duplicated and delayed messages, an RMS margin refusal, a fill lost in a
disconnect and recovered, a slow symbol, cancel-with-square-off, and a final reconciliation against
the exchange's fill log.

Or by hand:

```sh
go run ./cmd/exchange                      # orders :9001, market data :9002, admin :9003
go run ./cmd/stocky                        # API + WebSocket :8080; users u1/u2/u3 with fixed margin
go run ./cmd/watch -user u1                # in another terminal: u1's live event stream
```

Create, view and cancel a strategy (prices are **paise**, so 160000 = ₹1,600.00):

```sh
curl -s localhost:8080/strategies -d '{"user":"u1","symbol":"INFY","side":"LONG","entry":"MARKET",
  "trigger":160500,"qty":10,"target":162000,"stoploss":159000,"client_ref":"my-retry-key-1"}'
curl -s localhost:8080/strategies?user=u1
curl -s localhost:8080/strategies/<id>
curl -s -X DELETE localhost:8080/strategies/<id>
curl -s localhost:8080/accounts/u1         # margin limit / used / reserved, positions
```

Turn exchange misbehaviour on and off while it runs:

```sh
curl -s -X PUT localhost:9003/chaos -d '{"drop":0.02,"delay":50000000,"duplicate":0.2,"reorder":0.2,"reject":0.1,"partial":0.5}'
curl -s -X PUT localhost:9003/chaos -d '{"symbol_delay":{"INFY":3000000000}}'   # one slow symbol
curl -s -X POST localhost:9003/price -d '{"symbol":"INFY","price":170000}'      # gap the price
curl -s -X POST localhost:9003/drop                                            # cut the connection now
curl -s -X PUT localhost:9003/chaos -d '{}'                                    # perfect exchange again
```

`go run ./cmd/exchange -h` shows the same toggles as flags; `-tick 0` stops the random walk so prices move only when you set them.

## Test

```sh
go test -race ./...
```

| Rule (brief §2) | How it holds | Test |
|---|---|---|
| Target and stoploss never both execute | An exit is chosen only in `OPEN`; choosing one moves the strategy to `EXIT_PENDING`, where no exit is evaluated. Independently, the RMS refuses a second exit while one is working, and sizes every exit to exactly the open position | `TestTargetAndStoplossAreMutuallyExclusive`, `TestRejectedExitReArmsAndOnlyOneLegExecutes`, `TestOnlyOneExitPerStrategy`, `TestGapThroughTargetThenStopExitsOnce` |
| An order is never duplicated or silently lost | Each order has a client id and is recorded as in flight before it is written. It is resent after every reconnect and whenever unanswered. The exchange treats `NEW`/`CANCEL` as idempotent by client id. The RMS drops duplicate executions by exec id. Strategy creation is idempotent too: a retried `POST /strategies` with the same `client_ref` returns the original | `TestFillLostInDisconnectIsRecovered`, `TestDuplicateLateAndUnknownExecutionsAreDropped`, `TestRetriedCreateDoesNotDuplicate`, `TestInvariantsUnderChaos` |
| Positions always equal what the exchange filled | Positions change only on validated fills, in one place (the RMS). A cancel confirmation carries the exchange's cumulative filled quantity (like FIX `CumQty`), so a fill that arrives *after* its own order's cancel is still booked | `TestCancelThatOvertakesAFillDoesNotLoseTheFill`, `TestPartialFillsKeepPositionsAndMarginExact`, and `TestInvariantsUnderChaos`, which compares every user's positions and every strategy's filled quantities with the exchange's own fill log |
| A user never exceeds margin | Checked and reserved under one lock, so concurrent orders across symbols can't jointly exceed it. Entries always carry a price cap (market entries become protected limits), and margin in use is capped at what was reserved, so a fill through a gap can't grow it | `TestMarginNeverExceededUnderConcurrency`, `TestConcurrentTriggersNeverExceedMargin`, `TestShortFilledAboveLimitDoesNotGrowMargin`, plus a hook asserting the limit after **every** change in every end-to-end test |
| A slow order on one symbol doesn't hold up others | One actor per symbol with an unbounded mailbox. Nothing upstream (feed, OMS reader) ever waits on an actor, and actors never touch the socket: orders go onto a queue drained by a single writer | `TestSlowSymbolDoesNotBlockOthers` (3 s delay on one symbol; the other opens in ~12 ms) |
| No path to the OMS without the RMS | The OMS lives at `internal/rms/internal/oms`. Go's internal-package rule means **only `rms` can import it**, so a bypass does not compile | `TestBypassingTheRMSDoesNotCompile`, `TestOnlyRMSImportsTheOMS` |

`TestInvariantsUnderChaos` is the main proof:
- every misbehaviour is on at once (drops, delays, duplicates, reordering, rejections, partial fills), plus forced disconnects;
- 200 strategies across 4 symbols and 3 users trade through 300 gapping ticks, with random cancels;
- three seeds;
- afterwards Stocky's books are checked against the exchange's records: every order sent exists on the exchange exactly once, positions equal fills, every strategy's filled quantities agree with the exchange, at most one exit filled per strategy and it closed exactly what was entered, and margin was never exceeded.

How I know the tests bite: I deliberately broke duplicate-fill handling in the RMS and re-ran the chaos test, and it failed straight away with margin breaches and wrong positions. Running the suite repeatedly is also how I found, and fixed, a cancel overtaking its own fill on the wire.

## Design decisions

- **One actor per symbol.** A goroutine is the only code that touches its symbol's strategies. Ticks, commands and executions are applied one at a time, so strategy state has no locks and no races. The strategy itself is a pure state machine (`internal/strategy`), unit-tested without any I/O.
- **Margin is the only shared state**, and it lives in the RMS under one lock. No I/O happens under that lock.
- **The RMS is in both directions.** The brief says every order *and trade* must be RMS-validated before other modules use it. Inbound exchange messages go to the RMS first. Unknown orders, duplicate exec ids, overfills and messages for finished orders are dropped there, so the engine only ever sees each execution once.
- **Trigger direction** follows Zerodha-style GTT semantics: a trigger above the price at creation fires when the price rises to it, a trigger below fires when it falls to it. Conditions are inequalities, so a tick that jumps several levels still fires.
- **Market entries are sent as protected limit orders** (last price ± 3%). This is what lets margin be reserved for an amount a fill can never exceed. It is also how Indian brokers already guard market orders.
- **Cancel never leaves an orphan position.**
  - Before entry: the strategy is cancelled.
  - While the entry order is working: that order is cancelled; if the fill wins the race, the position is squared off.
  - With a position open: it is squared off at market.
  - During an exit: refused, because the exit is already flattening the position.
- **A rejected exit re-arms both legs.** The next tick that meets either condition tries again.
- **Partial fills.** The position is whatever has filled. If an exit level is hit while the entry is only partly filled, the rest of the entry is cancelled first, and the exit then covers exactly what filled: never an accidental reverse position. Exits that fill in pieces stay working until the position is flat. Margin is released in proportion.
- **Exits are market orders.** A stoploss has to execute; a protected limit could sit unfilled while the price runs away. The cost is slippage in a gap, which is the normal trade-off for a stop-market.
- **Margin is notional and checked at order time**, exactly as the brief defines it: each order's value must fit the user's fixed available margin. P&L, mark-to-market and leverage (SPAN) are a different product and out of scope.

## Known gaps and shortcuts

- **State is in memory (as the brief allows).** If Stocky restarts, strategies are lost. If the exchange restarts, its order book is lost, and resends would then place fresh orders. Persistence (a journal of orders and fills) is the first thing I'd add for production. Client ids carry a per-run prefix, so a restarted Stocky never collides with old ids.
- **Chaos applies to the order session, not market data.** A gap in market data would delay triggers but cannot break a rule, because every decision is re-checked against the RMS's books.
- **The mock exchange's liquidity is idealised.** Fills happen at the current price; partial fills are random. There is no order-book depth or queue position.
- **The WebSocket is in-process,** behind the `engine.Publisher` interface. In production I'd put a dedicated realtime server there (for example Centrifugo, with user-limited channels, history for reconnecting clients and multi-node fan-out) without touching the engine. Slow WebSocket clients are disconnected rather than allowed to slow trading.
- **Single node.** One order session to the exchange; scaling out would shard symbols across nodes, with the per-user margin ledger becoming the one shared service.
- **No authentication;** the user id is taken from the request, as the brief allows. The admin port has no auth either.
- **The demo console is a convenience,** not part of the brief: one embedded HTML file over the same API.
