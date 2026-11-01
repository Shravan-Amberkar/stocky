package rms

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/Shravan-Amberkar/stocky/internal/wire"
)

type fakeOMS struct {
	mu   sync.Mutex
	sent []wire.Msg
	done map[string]bool
}

func (f *fakeOMS) Send(m wire.Msg)       { f.mu.Lock(); f.sent = append(f.sent, m); f.mu.Unlock() }
func (f *fakeOMS) Cancel(string, string) {}
func (f *fakeOMS) Acked(string)          {}
func (f *fakeOMS) Done(id string)        { f.mu.Lock(); f.done[id] = true; f.mu.Unlock() }
func (f *fakeOMS) Unconfirmed() int      { return 0 }
func (f *fakeOMS) Close()                {}

func gateway(limit int64) (*Gateway, *fakeOMS, *[]Execution) {
	var mu sync.Mutex
	var got []Execution
	g := newGateway(map[string]int64{"u1": limit}, func(x Execution) { mu.Lock(); got = append(got, x); mu.Unlock() },
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	f := &fakeOMS{done: map[string]bool{}}
	g.t = f
	return g, f, &got
}

func entry(id string, qty, limit int64) Order {
	return Order{ClientID: id, StrategyID: "S-" + id, User: "u1", Symbol: "INFY", Role: Entry, Side: wire.Buy,
		Type: wire.Limit, Qty: qty, Limit: limit}
}

func TestMarginNeverExceededUnderConcurrency(t *testing.T) {
	g, f, _ := gateway(1_000_000)
	var worst int64
	g.SetHook(func(_ string, v AccountView) {
		if v.Used+v.Reserved > v.Limit {
			t.Errorf("margin exceeded: %+v", v)
		}
		if v.Used+v.Reserved > worst {
			worst = v.Used + v.Reserved
		}
	})
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	for i := range 300 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if g.Submit(entry(fmt.Sprint("o", i), 1, 10_000)) == nil {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if accepted != 100 || len(f.sent) != 100 || worst != 1_000_000 {
		t.Fatalf("accepted=%d sent=%d worst=%d; want exactly the 100 that fit", accepted, len(f.sent), worst)
	}
}

func TestFillConvertsReservationAndBetterPriceFreesMargin(t *testing.T) {
	g, _, _ := gateway(100_000)
	if err := g.Submit(entry("A", 10, 10_000)); err != nil {
		t.Fatal(err)
	}
	if err := g.Submit(entry("B", 1, 1)); !errors.Is(err, ErrMargin) {
		t.Fatal("limit is fully reserved")
	}
	g.onWire(wire.Msg{Type: wire.Ack, ClientID: "A"})
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "A", ExecID: "X1", Qty: 10, Price: 9_500})
	v, _ := g.Account("u1")
	if v.Used != 95_000 || v.Reserved != 0 || v.Positions["INFY"] != 10 {
		t.Fatalf("%+v", v)
	}
}

func TestDuplicateLateAndUnknownExecutionsAreDropped(t *testing.T) {
	g, f, got := gateway(1_000_000)
	g.Submit(entry("A", 10, 10_000)) //nolint:errcheck
	fill := wire.Msg{Type: wire.Fill, ClientID: "A", ExecID: "X1", Qty: 10, Price: 10_000}
	g.onWire(fill)
	g.onWire(fill)                                                                     // duplicated by the exchange
	g.onWire(wire.Msg{Type: wire.Ack, ClientID: "A"})                                  // arrives after the fill
	g.onWire(wire.Msg{Type: wire.Reject, ClientID: "A"})                               // late, impossible: order is filled
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "A", ExecID: "X9", Qty: 1, Price: 1}) // overfill
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "NOPE", ExecID: "X2", Qty: 1, Price: 1})
	v, _ := g.Account("u1")
	if v.Positions["INFY"] != 10 || v.Used != 100_000 {
		t.Fatalf("position/margin moved by a bad message: %+v", v)
	}
	if len(*got) != 1 || (*got)[0].Kind != Filled || !f.done["A"] {
		t.Fatalf("engine must see exactly one fill: %+v", *got)
	}
}

func TestRejectAndCancelReleaseMargin(t *testing.T) {
	g, _, _ := gateway(100_000)
	g.Submit(entry("A", 5, 10_000)) //nolint:errcheck
	g.Submit(entry("B", 5, 10_000)) //nolint:errcheck
	g.onWire(wire.Msg{Type: wire.Reject, ClientID: "A", Reason: "no"})
	g.onWire(wire.Msg{Type: wire.Cancelled, ClientID: "B"})
	if v, _ := g.Account("u1"); v.Reserved != 0 || v.Used != 0 {
		t.Fatalf("%+v", v)
	}
}

func TestOnlyOneExitPerStrategy(t *testing.T) {
	g, _, _ := gateway(1_000_000)
	exit := func(id string) Order {
		return Order{ClientID: id, StrategyID: "S-A", User: "u1", Symbol: "INFY", Role: Exit, Side: wire.Sell, Type: wire.Market, Qty: 10}
	}
	if err := g.Submit(exit("X0")); !errors.Is(err, ErrExit) {
		t.Fatal("exit with no position")
	}
	g.Submit(entry("A", 10, 10_000)) //nolint:errcheck
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "A", ExecID: "F1", Qty: 10, Price: 10_000})
	if err := g.Submit(exit("X1")); err != nil {
		t.Fatal(err)
	}
	if err := g.Submit(exit("X2")); !errors.Is(err, ErrExit) {
		t.Fatal("second exit while first is working (target AND stoploss) must be refused")
	}
	g.onWire(wire.Msg{Type: wire.Reject, ClientID: "X1"})
	if err := g.Submit(exit("X3")); err != nil {
		t.Fatal("after a rejected exit, a new exit is allowed:", err)
	}
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "X3", ExecID: "F2", Qty: 10, Price: 11_000})
	if err := g.Submit(exit("X4")); !errors.Is(err, ErrExit) {
		t.Fatal("exit after the position is closed")
	}
	if v, _ := g.Account("u1"); v.Positions["INFY"] != 0 || v.Used != 0 {
		t.Fatalf("%+v", v)
	}
}

// The OMS is only reachable through the RMS: it lives in rms/internal/oms, and Go refuses to
// compile an import of it from outside internal/rms. This test pins that layout.
func TestOnlyRMSImportsTheOMS(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", `{{.ImportPath}} {{join .Imports " "}}`, "github.com/Shravan-Amberkar/stocky/...").Output()
	if err != nil {
		t.Fatal(err)
	}
	const oms = "github.com/Shravan-Amberkar/stocky/internal/rms/internal/oms"
	for _, line := range strings.Split(string(out), "\n") {
		pkg, imports, _ := strings.Cut(line, " ")
		if strings.Contains(" "+imports+" ", " "+oms+" ") && pkg != "github.com/Shravan-Amberkar/stocky/internal/rms" {
			t.Errorf("%s imports the OMS directly", pkg)
		}
	}
}

// A program outside internal/rms that imports the OMS is rejected by the compiler.
func TestBypassingTheRMSDoesNotCompile(t *testing.T) {
	out, err := exec.Command("go", "build", "-o", "/dev/null", "github.com/Shravan-Amberkar/stocky/testdata/bypass").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "use of internal package") {
		t.Fatalf("expected the bypass to be rejected by the compiler, got err=%v\n%s", err, out)
	}
}

// A short entry can fill ABOVE its limit (a better price for a seller). Margin in use must stay
// what the RMS approved, or a gap would push the user over their limit.
func TestShortFilledAboveLimitDoesNotGrowMargin(t *testing.T) {
	g, _, _ := gateway(100_000)
	o := entry("A", 10, 10_000)
	o.Side = wire.Sell
	g.Submit(o) //nolint:errcheck
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "A", ExecID: "F1", Qty: 10, Price: 12_000})
	if v, _ := g.Account("u1"); v.Used != 100_000 || v.Reserved != 0 || v.Positions["INFY"] != -10 {
		t.Fatalf("%+v", v)
	}
}

func TestPartialFillsKeepPositionsAndMarginExact(t *testing.T) {
	g, _, _ := gateway(1_000_000)
	exit := func(id string, qty int64) Order {
		return Order{ClientID: id, StrategyID: "S-A", User: "u1", Symbol: "INFY", Role: Exit, Side: wire.Sell, Type: wire.Market, Qty: qty}
	}
	g.Submit(entry("A", 10, 10_000)) //nolint:errcheck
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "A", ExecID: "F1", Qty: 4, Price: 10_000})
	if v, _ := g.Account("u1"); v.Used != 40_000 || v.Reserved != 60_000 || v.Positions["INFY"] != 4 {
		t.Fatalf("after part fill: %+v", v)
	}
	if err := g.Submit(exit("X1", 4)); !errors.Is(err, ErrExit) {
		t.Fatal("exit must wait until the entry can no longer fill")
	}
	g.onWire(wire.Msg{Type: wire.Cancelled, ClientID: "A"}) // remainder cancelled
	if v, _ := g.Account("u1"); v.Reserved != 0 || v.Used != 40_000 {
		t.Fatalf("remainder's reservation must be released: %+v", v)
	}
	if err := g.Submit(exit("X1", 10)); !errors.Is(err, ErrExit) {
		t.Fatal("exit larger than the position")
	}
	if err := g.Submit(exit("X1", 4)); err != nil {
		t.Fatal(err)
	}
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "X1", ExecID: "F2", Qty: 1, Price: 11_000})
	if v, _ := g.Account("u1"); v.Used != 30_000 || v.Positions["INFY"] != 3 {
		t.Fatalf("a quarter of the margin is released: %+v", v)
	}
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "X1", ExecID: "F3", Qty: 3, Price: 11_000})
	if v, _ := g.Account("u1"); v.Used != 0 || len(v.Positions) != 0 {
		t.Fatalf("flat and all margin released: %+v", v)
	}
}

// The exchange fills 7 of 11 and then cancels the rest, but the CANCELLED overtakes the FILL on
// the wire. The fill must still be booked, and the strategy must hear "cancelled" only after it.
func TestCancelThatOvertakesAFillDoesNotLoseTheFill(t *testing.T) {
	g, f, got := gateway(1_000_000)
	g.Submit(entry("A", 11, 10_000)) //nolint:errcheck
	g.onWire(wire.Msg{Type: wire.Cancelled, ClientID: "A", CumQty: 7})
	if v, _ := g.Account("u1"); v.Reserved != 70_000 || f.done["A"] || len(*got) != 0 {
		t.Fatalf("cancel must release only the 4 that never filled and wait for the 7: %+v done=%v", v, f.done["A"])
	}
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "A", ExecID: "F1", Qty: 7, Price: 10_000})
	v, _ := g.Account("u1")
	if v.Positions["INFY"] != 7 || v.Used != 70_000 || v.Reserved != 0 || !f.done["A"] {
		t.Fatalf("late fill lost or margin wrong: %+v", v)
	}
	if len(*got) != 2 || (*got)[0].Kind != Filled || (*got)[1].Kind != Cancelled {
		t.Fatalf("engine must see the fill, then the cancel: %+v", *got)
	}
	g.onWire(wire.Msg{Type: wire.Fill, ClientID: "A", ExecID: "F9", Qty: 1, Price: 10_000})
	if v, _ := g.Account("u1"); v.Positions["INFY"] != 7 {
		t.Fatal("no fills beyond the exchange's cumulative quantity")
	}
}

// A duplicated PARTIAL fill is the case only exec-id de-duplication catches: "can't fill more
// than remains" would happily book 4 + 4 of a 10-lot.
func TestDuplicatedPartialFillIsBookedOnce(t *testing.T) {
	g, _, got := gateway(1_000_000)
	g.Submit(entry("A", 10, 10_000)) //nolint:errcheck
	f := wire.Msg{Type: wire.Fill, ClientID: "A", ExecID: "F1", Qty: 4, Price: 10_000}
	g.onWire(f)
	g.onWire(f)
	if v, _ := g.Account("u1"); v.Positions["INFY"] != 4 || len(*got) != 1 {
		t.Fatalf("duplicate partial fill double-booked: %+v", v)
	}
}
