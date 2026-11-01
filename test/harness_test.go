// Package test runs the whole system in-process: the mock exchange on real TCP sockets, the OMS,
// RMS, symbol actors and market-data feed, with the exchange's misbehaviours switched on.
package test

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shravan-Amberkar/stocky/internal/engine"
	"github.com/Shravan-Amberkar/stocky/internal/exchange"
	"github.com/Shravan-Amberkar/stocky/internal/rms"
	"github.com/Shravan-Amberkar/stocky/internal/strategy"
	"github.com/Shravan-Amberkar/stocky/internal/wire"
)

type recorder struct {
	mu     sync.Mutex
	events map[string][]engine.Event
	fanout engine.Publisher // optional: also forward (the WebSocket hub, in API tests)
}

func (r *recorder) Publish(user string, e engine.Event) {
	r.mu.Lock()
	r.events[user] = append(r.events[user], e)
	f := r.fanout
	r.mu.Unlock()
	if f != nil {
		f.Publish(user, e)
	}
}

func strategySpec(user string) strategy.Spec {
	return strategy.Spec{User: user, Symbol: "AAA", Side: strategy.Long, Entry: strategy.Market, Trigger: 10_000, Qty: 2}
}

type system struct {
	t      *testing.T
	ex     *exchange.Exchange
	gw     *rms.Gateway
	eng    *engine.Engine
	pub    *recorder
	stop   chan struct{}
	breach atomic.Int64 // margin breaches seen by the RMS hook
	orders string       // exchange order-port address
}

func logger() *slog.Logger {
	if os.Getenv("STOCKY_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func start(t *testing.T, prices map[string]int64, limits map[string]int64) *system {
	t.Helper()
	log := logger()
	ex := exchange.New(prices, log)
	lo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lm, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go ex.ServeOrders(lo)     //nolint:errcheck
	go ex.ServeMarketData(lm) //nolint:errcheck

	var symbols []string
	for s := range prices {
		symbols = append(symbols, s)
	}
	s := &system{t: t, ex: ex, pub: &recorder{events: map[string][]engine.Event{}}, stop: make(chan struct{}), orders: lo.Addr().String()}
	s.eng = engine.New(symbols, s.pub, log)
	s.gw = rms.New(limits, lo.Addr().String(), s.eng.Deliver, log)
	s.gw.SetHook(func(user string, v rms.AccountView) {
		if v.Used+v.Reserved > v.Limit || v.Reserved < 0 || v.Used < 0 {
			s.breach.Add(1)
			t.Errorf("margin invariant broken for %s: %+v", user, v)
		}
	})
	s.eng.Start(s.gw)
	go engine.Feed(lm.Addr().String(), s.eng, log, s.stop)
	t.Cleanup(func() {
		close(s.stop)
		s.gw.Close()
		s.eng.Stop()
		lo.Close()
		lm.Close()
	})
	return s
}

// create retries until the actor has received its first price.
func (s *system) create(sp strategy.Spec) (strategy.Strategy, error) {
	deadline := time.Now().Add(3 * time.Second)
	for {
		st, _, err := s.eng.Create(sp)
		if !errors.Is(err, strategy.ErrNoPrice) || time.Now().After(deadline) {
			return st, err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *system) waitFor(what string, timeout time.Duration, cond func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// settle waits until every order is confirmed by the exchange, no exit is still working, and
// nothing has changed for a while.
func (s *system) settle(timeout time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	stableSince := time.Now()
	for {
		var b strings.Builder
		busy := s.gw.Unconfirmed() > 0
		for _, st := range s.eng.List("") {
			fmt.Fprintf(&b, "%s:%s:%d;", st.ID, st.State, st.FilledQty)
			busy = busy || st.State == strategy.ExitPending
		}
		if b.String() != last || busy {
			last, stableSince = b.String(), time.Now()
		}
		if time.Since(stableSince) > 400*time.Millisecond {
			return
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("system did not settle (unconfirmed=%d)", s.gw.Unconfirmed())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// checkInvariants compares Stocky's view with the exchange's own records.
func (s *system) checkInvariants() {
	t := s.t
	t.Helper()
	exOrders := s.ex.Orders()
	sent := s.gw.SentOrders()

	// 2. Never duplicated, never lost: the exchange holds exactly the orders the RMS let through.
	for id := range sent {
		if _, ok := exOrders[id]; !ok {
			t.Errorf("order %s was sent but never reached the exchange (lost)", id)
		}
	}
	for id := range exOrders {
		if _, ok := sent[id]; !ok {
			t.Errorf("exchange holds order %s that Stocky never sent (phantom/duplicate)", id)
		}
	}

	// 3. Positions equal exactly what the exchange filled.
	want := map[string]map[string]int64{}
	fillQty := map[string]int64{}
	for _, f := range s.ex.Fills() {
		if want[f.User] == nil {
			want[f.User] = map[string]int64{}
		}
		q := f.Qty
		if f.Side == wire.Sell {
			q = -q
		}
		want[f.User][f.Symbol] += q
		fillQty[f.ClientID] += f.Qty
	}
	for user, acct := range s.gw.Accounts() {
		for sym, q := range want[user] {
			if acct.Positions[sym] != q {
				t.Errorf("%s %s: position %d, exchange filled %d", user, sym, acct.Positions[sym], q)
			}
		}
		for sym, q := range acct.Positions {
			if want[user][sym] != q {
				t.Errorf("%s %s: position %d with no matching exchange fills", user, sym, q)
			}
		}
		// 4. Margin.
		if acct.Used+acct.Reserved > acct.Limit {
			t.Errorf("%s over margin: %+v", user, acct)
		}
	}

	// 1. Target and stoploss never both execute; and each strategy agrees with the exchange.
	for _, st := range s.eng.List("") {
		var entries, exitsFilled int
		var entryQty, exitQty int64
		for id, o := range sent {
			if o.StrategyID != st.ID {
				continue
			}
			if o.Role == rms.Entry {
				entries++
				entryQty += fillQty[id]
			} else {
				exitQty += fillQty[id]
				if fillQty[id] > 0 {
					exitsFilled++
				}
			}
		}
		if entries > 1 || exitsFilled > 1 || exitQty > entryQty {
			t.Errorf("%s: %d entry orders, %d exits filled, exited %d of %d", st.ID, entries, exitsFilled, exitQty, entryQty)
		}
		if st.FilledQty != entryQty || st.ExitedQty != exitQty {
			t.Errorf("%s: strategy says entry %d / exit %d, exchange says %d / %d", st.ID, st.FilledQty, st.ExitedQty, entryQty, exitQty)
		}
		switch st.State {
		case strategy.Closed:
			if exitsFilled != 1 || exitQty != entryQty {
				t.Errorf("%s CLOSED but exchange shows %d filled exits covering %d of %d", st.ID, exitsFilled, exitQty, entryQty)
			}
		case strategy.Open:
			if entryQty == 0 || exitQty != 0 {
				t.Errorf("%s OPEN but exchange shows entry=%d exit=%d", st.ID, entryQty, exitQty)
			}
		case strategy.WaitingEntry, strategy.Rejected, strategy.Cancelled:
			if entryQty != 0 {
				t.Errorf("%s %s but %d of its entry filled on the exchange", st.ID, st.State, entryQty)
			}
		}
	}
	if s.breach.Load() > 0 {
		t.Errorf("%d margin breaches observed", s.breach.Load())
	}
}
