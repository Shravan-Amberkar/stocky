# Stocky — automated entry/exit strategies

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

```sh
go run ./cmd/exchange                      # orders :9001, market data :9002, admin :9003
go run ./cmd/stocky                        # API + WebSocket :8080; users u1/u2/u3 with fixed margin
go run ./cmd/watch -user u1                # in another terminal: u1's live event stream
```

Create, view and cancel a strategy (prices are **paise**, so 160000 = ₹1,600.00):

```sh
curl -s localhost:8080/strategies -d '{"user":"u1","symbol":"INFY","side":"LONG","entry":"MARKET",
  "trigger":160500,"qty":10,"target":162000,"stoploss":159000}'
curl -s localhost:8080/strategies?user=u1
curl -s localhost:8080/strategies/<id>
curl -s -X DELETE localhost:8080/strategies/<id>
curl -s localhost:8080/accounts/u1         # margin limit / used / reserved, positions
```

Turn exchange misbehaviour on and off while it runs:

```sh
curl -s -X PUT localhost:9003/chaos -d '{"drop":0.02,"delay":50000000,"duplicate":0.2,"reorder":0.2,"reject":0.1}'
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
| Target and stoploss never both execute | An exit is chosen only in `OPEN`; choosing one moves the strategy to `EXIT_PENDING`, where no exit is evaluated. Independently, the RMS refuses a second exit order for a strategy whose exit is working or done | `TestTargetAndStoplossAreMutuallyExclusive`, `TestRejectedExitReArmsAndOnlyOneLegExecutes`, `TestOnlyOneExitPerStrategy`, `TestGapThroughTargetThenStopExitsOnce` |
| An order is never duplicated or silently lost | Each order has a client id and is recorded as in flight before it is written. It is resent after every reconnect and whenever unanswered. The exchange treats `NEW`/`CANCEL` as idempotent by client id. The RMS drops duplicate executions by exec id | `TestFillLostInDisconnectIsRecovered`, `TestDuplicateLateAndUnknownExecutionsAreDropped`, `TestInvariantsUnderChaos` |
| Positions always equal what the exchange filled | Positions change only on validated fills, in one place (the RMS) | `TestInvariantsUnderChaos`, which compares every user's positions with the exchange's own fill log |
| A user never exceeds margin | Checked and reserved under one lock, so concurrent orders across symbols can't jointly exceed it. Entries always carry a price cap (market entries become protected limits), and margin in use is capped at what was reserved, so a fill through a gap can't grow it | `TestMarginNeverExceededUnderConcurrency`, `TestConcurrentTriggersNeverExceedMargin`, `TestShortFilledAboveLimitDoesNotGrowMargin`, plus a hook asserting the limit after **every** change in every end-to-end test |
| A slow order on one symbol doesn't hold up others | One actor per symbol with an unbounded mailbox. Nothing upstream (feed, OMS reader) ever waits on an actor | `TestSlowSymbolDoesNotBlockOthers` (3 s delay on one symbol; the other opens in ~12 ms) |
| No path to the OMS without the RMS | The OMS lives at `internal/rms/internal/oms`. Go's internal-package rule means **only `rms` can import it**, so a bypass does not compile | `TestBypassingTheRMSDoesNotCompile`, `TestOnlyRMSImportsTheOMS` |

`TestInvariantsUnderChaos` is the main proof:
- every misbehaviour is on at once, plus forced disconnects;
- 200 strategies across 4 symbols and 3 users trade through 300 gapping ticks, with random cancels;
- three seeds;
- afterwards Stocky's books are checked against the exchange's records: every order sent exists on the exchange exactly once, positions equal fills, every strategy's state agrees with its fills, at most one exit filled per strategy, and margin was never exceeded.

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

## Known gaps and shortcuts

- **Fills are all-or-nothing.** The mock exchange never partially fills. The code paths accumulate filled quantity, but partial-fill exits are not tested.
- **Margin is notional and blocked at order time.** There is no mark-to-market, P&L, leverage or exchange-specific margin (SPAN). A short's loss is not charged against margin. Exits are not margin-checked, since they reduce exposure.
- **Exits are market orders.** A real stoploss would be sent as a stop-limit with its own protection band.
- **Chaos applies only to the order session.** Market data is a clean stream. A gap in market data would delay triggers but couldn't break a rule, since every decision re-reads the order book.
- **State is in memory.** If Stocky restarts, strategies are lost. If the exchange restarts, its order book is lost and resends would create fresh orders. Client ids include a per-run prefix so a restarted Stocky never collides with old orders.
- **One order session, one writer.** A stalled TCP write would delay every symbol's outbound orders, though never their inbound processing.
- **The mock exchange fills at the current price with unlimited liquidity.** There is no queue position or order book depth.
- **The WebSocket is in-process,** behind the `engine.Publisher` interface. In production I'd put a dedicated realtime server there (for example Centrifugo, with user-limited channels, history for reconnecting clients and multi-node fan-out) without touching the engine. Slow WebSocket clients are disconnected rather than allowed to slow trading.
- **No authentication;** the user id is taken from the request, as the brief allows. The admin port has no auth either.
- **Strategy snapshots for the API are copied after each change.** That is fine here; at large scale it would become a read-model projection.
