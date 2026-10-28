package test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Shravan-Amberkar/stocky/internal/exchange"
	"github.com/Shravan-Amberkar/stocky/internal/strategy"
)

const lvl = 5 // tick size in paise

var prices = map[string]int64{"AAA": 10_000, "BBB": 20_000, "CCC": 5_000, "DDD": 40_000}

// TestInvariantsUnderChaos is the main proof. Every misbehaviour is on at once: connections drop
// at any point, replies are delayed, duplicated and reordered, and orders are rejected. Hundreds
// of strategies across four symbols and three users trade through gapping prices. Afterwards
// Stocky's books are compared with the exchange's own records.
func TestInvariantsUnderChaos(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3} {
		t.Run("seed", func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed*7))
			s := start(t, prices, map[string]int64{"u1": 3_000_000, "u2": 1_000_000, "u3": 50_000_000})
			s.ex.SetChaos(exchange.Chaos{Drop: 0.02, Delay: 20 * time.Millisecond, Duplicate: 0.15, Reorder: 0.15, Reject: 0.1})

			users := []string{"u1", "u2", "u3"}
			created := 0
			for sym, px := range prices {
				for range 50 {
					side := strategy.Long
					if rng.IntN(2) == 0 {
						side = strategy.Short
					}
					sp := strategy.Spec{User: users[rng.IntN(3)], Symbol: sym, Side: side, Entry: strategy.Market,
						Trigger: px + int64(rng.IntN(41)-20)*lvl, Qty: int64(rng.IntN(20) + 1)}
					ref := sp.Trigger
					if rng.IntN(3) == 0 {
						sp.Entry, sp.Limit = strategy.Limit, sp.Trigger+int64(rng.IntN(5)-2)*lvl
						ref = sp.Limit
					}
					dir := int64(1)
					if side == strategy.Short {
						dir = -1
					}
					if rng.IntN(4) > 0 {
						sp.Target = ref + dir*int64(rng.IntN(30)+3)*lvl
					}
					if rng.IntN(4) > 0 {
						sp.Stoploss = ref - dir*int64(rng.IntN(30)+3)*lvl
					}
					if _, err := s.create(sp); err == nil {
						created++
					}
				}
			}
			if created < 150 {
				t.Fatalf("only %d strategies created", created)
			}

			// Gapping random walk, with forced disconnects along the way.
			cur := map[string]int64{}
			for k, v := range prices {
				cur[k] = v
			}
			for step := range 300 {
				for sym := range cur {
					move := int64(rng.IntN(7) - 3)
					if rng.IntN(10) == 0 {
						move = int64(rng.IntN(50) - 25) // several levels in one tick
					}
					if p := cur[sym] + move*lvl; p > 100 {
						cur[sym] = p
						s.ex.SetPrice(sym, p)
					}
				}
				if step%60 == 30 {
					s.ex.DropActive()
				}
				if step%50 == 0 {
					for _, st := range s.eng.List("") { // cancel a few, in whatever state they are in
						if rng.IntN(15) == 0 {
							s.eng.Cancel(st.ID) //nolint:errcheck
						}
					}
				}
				time.Sleep(3 * time.Millisecond)
			}
			// Sweep each price far up and down so most open positions hit a target or stop.
			for _, f := range []float64{1.3, 0.7, 1.0} {
				for sym, px := range prices {
					s.ex.SetPrice(sym, int64(float64(px)*f))
				}
				time.Sleep(150 * time.Millisecond)
			}

			s.ex.SetChaos(exchange.Chaos{}) // let the last in-flight messages land
			s.settle(30 * time.Second)
			s.checkInvariants()

			var closed, open int
			for _, st := range s.eng.List("") {
				switch st.State {
				case strategy.Closed:
					closed++
				case strategy.Open:
					open++
				}
			}
			t.Logf("strategies=%d closed=%d open=%d exchange_orders=%d fills=%d", created, closed, open,
				len(s.ex.Orders()), len(s.ex.Fills()))
			if closed == 0 {
				t.Fatal("scenario exercised no exits")
			}
		})
	}
}

// A slow order on one symbol must not hold up others: every exchange message about SLOW is
// delayed by 3 seconds, while FAST must still trade at normal speed.
func TestSlowSymbolDoesNotBlockOthers(t *testing.T) {
	s := start(t, map[string]int64{"SLOW": 10_000, "FAST": 10_000}, map[string]int64{"u1": 10_000_000})
	s.ex.SetChaos(exchange.Chaos{SymbolDelay: map[string]time.Duration{"SLOW": 3 * time.Second}})

	slow, err := s.create(strategy.Spec{User: "u1", Symbol: "SLOW", Side: strategy.Long, Entry: strategy.Market, Trigger: 10_000, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	fast, err := s.create(strategy.Spec{User: "u1", Symbol: "FAST", Side: strategy.Long, Entry: strategy.Market, Trigger: 10_000, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	s.waitFor("FAST to open", 2*time.Second, func() bool { st, _ := s.eng.Get(fast.ID); return st.State == strategy.Open })
	took := time.Since(t0)
	if st, _ := s.eng.Get(slow.ID); st.State != strategy.EntryPending {
		t.Fatalf("SLOW should still be waiting on the exchange, is %s", st.State)
	}
	if took > 500*time.Millisecond {
		t.Fatalf("FAST took %v: it was held up by SLOW", took)
	}
	t.Logf("FAST opened in %v while SLOW was still pending", took.Round(time.Millisecond))
	s.waitFor("SLOW to open eventually", 10*time.Second, func() bool { st, _ := s.eng.Get(slow.ID); return st.State == strategy.Open })
}

// The connection drops after the exchange has filled an order but before the fill reaches Stocky.
// The order must neither be lost (position missing) nor duplicated (filled twice) after reconnect.
func TestFillLostInDisconnectIsRecovered(t *testing.T) {
	s := start(t, map[string]int64{"AAA": 10_000}, map[string]int64{"u1": 10_000_000})
	s.ex.SetChaos(exchange.Chaos{SymbolDelay: map[string]time.Duration{"AAA": 300 * time.Millisecond}})
	st, err := s.create(strategy.Spec{User: "u1", Symbol: "AAA", Side: strategy.Long, Entry: strategy.Market, Trigger: 10_000, Qty: 7})
	if err != nil {
		t.Fatal(err)
	}
	s.waitFor("exchange to fill", 2*time.Second, func() bool { return len(s.ex.Fills()) == 1 })
	s.ex.DropActive() // ACK and FILL are still sitting in the delay and die with the connection
	s.ex.SetChaos(exchange.Chaos{})
	s.waitFor("strategy to open", 5*time.Second, func() bool { x, _ := s.eng.Get(st.ID); return x.State == strategy.Open })
	s.settle(5 * time.Second)
	s.checkInvariants()
	if a, _ := s.gw.Account("u1"); a.Positions["AAA"] != 7 || len(s.ex.Orders()) != 1 {
		t.Fatalf("position=%d orders=%d", a.Positions["AAA"], len(s.ex.Orders()))
	}
}

// A gap straight through the target and then through the stop, while the target exit is still in
// flight, must produce exactly one exit.
func TestGapThroughTargetThenStopExitsOnce(t *testing.T) {
	s := start(t, map[string]int64{"AAA": 10_000}, map[string]int64{"u1": 10_000_000})
	st, err := s.create(strategy.Spec{User: "u1", Symbol: "AAA", Side: strategy.Long, Entry: strategy.Market, Trigger: 10_000,
		Qty: 3, Target: 11_000, Stoploss: 9_000})
	if err != nil {
		t.Fatal(err)
	}
	s.waitFor("open", 2*time.Second, func() bool { x, _ := s.eng.Get(st.ID); return x.State == strategy.Open })
	s.ex.SetChaos(exchange.Chaos{SymbolDelay: map[string]time.Duration{"AAA": 200 * time.Millisecond}, Duplicate: 1})
	s.ex.SetPrice("AAA", 12_000) // gaps past the target
	time.Sleep(20 * time.Millisecond)
	s.ex.SetPrice("AAA", 8_000) // and past the stop before the exit's fill has arrived
	s.ex.SetPrice("AAA", 13_000)
	s.ex.SetChaos(exchange.Chaos{})
	s.settle(5 * time.Second)
	s.checkInvariants()
	x, _ := s.eng.Get(st.ID)
	if x.State != strategy.Closed || x.ExitReason != "target" || len(s.ex.Orders()) != 2 {
		t.Fatalf("state=%s reason=%s orders=%d", x.State, x.ExitReason, len(s.ex.Orders()))
	}
	if a, _ := s.gw.Account("u1"); len(a.Positions) != 0 {
		t.Fatalf("flat position expected, got %+v", a.Positions)
	}
}

// Many strategies for one user trigger in the same tick across symbols: the RMS admits only what
// fits in the margin, and the rest are rejected before reaching the exchange.
func TestConcurrentTriggersNeverExceedMargin(t *testing.T) {
	ps := map[string]int64{"AAA": 10_000, "BBB": 10_000, "CCC": 10_000, "DDD": 10_000}
	s := start(t, ps, map[string]int64{"u1": 1_000_000}) // ₹10,000
	for sym := range ps {
		for range 25 { // each reserves ₹108.15 (₹105 + 3% protection); fills at ₹105 hand back the band, so ~93 of 100 fit
			if _, err := s.create(strategy.Spec{User: "u1", Symbol: sym, Side: strategy.Long, Entry: strategy.Market, Trigger: 10_500, Qty: 1}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for sym := range ps {
		go s.ex.SetPrice(sym, 10_500) // all four symbols trigger at once
	}
	s.settle(10 * time.Second)
	s.checkInvariants()
	var open, denied int
	for _, st := range s.eng.List("u1") {
		switch st.State {
		case strategy.Open:
			open++
		case strategy.Rejected:
			denied++
		}
	}
	if open == 0 || denied == 0 || open+denied != 100 {
		t.Fatalf("open=%d denied=%d", open, denied)
	}
	t.Logf("%d entries fit the margin, %d were refused by the RMS", open, denied)
}
