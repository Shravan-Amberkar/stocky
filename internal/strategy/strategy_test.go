package strategy

import (
	"errors"
	"testing"

	"github.com/Shravan-Amberkar/stocky/internal/wire"
)

func longWithExits(t *testing.T) *Strategy {
	t.Helper()
	s, err := New("S1", Spec{User: "u1", Symbol: "INFY", Side: Long, Entry: Market, Trigger: 10_000, Qty: 5,
		Target: 12_000, Stoploss: 8_000}, 9_500)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// open drives a strategy to OPEN with entry filled at px.
func open(t *testing.T, s *Strategy, px int64) {
	t.Helper()
	in := s.OnTick(px)
	if len(in) != 1 || in[0].Place == nil || !in[0].Place.Entry {
		t.Fatalf("expected an entry order, got %+v", in)
	}
	s.OrderPlaced("E1", true)
	s.OnExecution("E1", "FILLED", s.Qty, px, "")
	if s.State != Open {
		t.Fatalf("state %s, want OPEN", s.State)
	}
}

func TestTriggerFiresThroughAGap(t *testing.T) {
	s := longWithExits(t) // trigger 10,000 above last 9,500: fires when price RISES to it
	if s.OnTick(9_995) != nil {
		t.Fatal("fired below trigger")
	}
	in := s.OnTick(11_500) // jumps 300 levels past the trigger in one tick
	if len(in) != 1 || s.State != EntryPending {
		t.Fatalf("gap tick did not fire: %+v", in)
	}
	p := in[0].Place
	if p.Type != wire.Limit || p.Limit != 11_500+11_500*MarketProtectionBps/10_000 || p.Side != wire.Buy {
		t.Fatalf("market entry must go out as a protected limit: %+v", p)
	}
}

func TestTriggerBelowFiresOnFall(t *testing.T) {
	s, _ := New("S1", Spec{User: "u", Symbol: "X", Side: Long, Entry: Limit, Trigger: 9_000, Limit: 9_000, Qty: 1}, 10_000)
	if s.TriggerAbove || s.OnTick(9_100) != nil {
		t.Fatal("dip-buy fired early")
	}
	if in := s.OnTick(8_000); len(in) != 1 || in[0].Place.Limit != 9_000 {
		t.Fatal("dip-buy did not fire at its limit price")
	}
}

func TestTargetAndStoplossAreMutuallyExclusive(t *testing.T) {
	s := longWithExits(t)
	open(t, s, 10_000)
	in := s.OnTick(12_500)
	if len(in) != 1 || s.ExitReason != "target" || in[0].Place.Side != wire.Sell || in[0].Place.Qty != 5 {
		t.Fatalf("target exit: %+v", in)
	}
	s.OrderPlaced("X2", false)
	// The price crashes through the stop before the target exit fills: nothing new may be sent.
	for _, px := range []int64{7_000, 5_000, 13_000} {
		if got := s.OnTick(px); got != nil {
			t.Fatalf("second exit sent while one is working: %+v", got)
		}
	}
	s.OnExecution("X2", "FILLED", 5, 12_400, "")
	if s.State != Closed || s.ExitReason != "target" {
		t.Fatal(s.State, s.ExitReason)
	}
	if s.OnTick(1) != nil || s.OnExecution("X2", "FILLED", 5, 12_400, "") != nil || s.State != Closed {
		t.Fatal("closed strategy reacted again")
	}
}

func TestRejectedExitReArmsAndOnlyOneLegExecutes(t *testing.T) {
	s := longWithExits(t)
	open(t, s, 10_000)
	s.OnTick(12_100)
	s.OrderPlaced("X2", false)
	s.OnExecution("X2", "REJECTED", 0, 0, "exchange busy")
	if s.State != Open || s.ExitOrder != "" {
		t.Fatalf("rejected exit should re-arm: %s", s.State)
	}
	in := s.OnTick(7_900) // now the stop is hit
	if len(in) != 1 || s.ExitReason != "stoploss" {
		t.Fatal("stoploss after rejected target")
	}
	s.OrderPlaced("X3", false)
	if s.OnExecution("X2", "FILLED", 5, 12_100, "") != nil || s.State != ExitPending {
		t.Fatal("an execution for the abandoned exit attempt must be ignored")
	}
	s.OnExecution("X3", "FILLED", 5, 7_900, "")
	if s.State != Closed || s.ExitReason != "stoploss" || s.ExitAttempts != 2 {
		t.Fatal(s.State, s.ExitReason)
	}
}

func TestShortSideExits(t *testing.T) {
	s, err := New("S1", Spec{User: "u", Symbol: "X", Side: Short, Entry: Market, Trigger: 9_000, Qty: 2, Target: 8_000, Stoploss: 9_500}, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	in := s.OnTick(8_900)
	if in[0].Place.Side != wire.Sell || in[0].Place.Limit != 8_900-8_900*MarketProtectionBps/10_000 {
		t.Fatalf("short entry: %+v", in[0].Place)
	}
	s.OrderPlaced("E1", true)
	s.OnExecution("E1", "FILLED", 2, 8_900, "")
	if in := s.OnTick(9_600); len(in) != 1 || s.ExitReason != "stoploss" || in[0].Place.Side != wire.Buy {
		t.Fatal("short stoploss")
	}
}

func TestCancel(t *testing.T) {
	s := longWithExits(t)
	if _, err := s.OnCancel(); err != nil || s.State != Cancelled {
		t.Fatal("cancel before entry")
	}

	s = longWithExits(t)
	s.OnTick(10_000)
	s.OrderPlaced("E1", true)
	in, _ := s.OnCancel()
	if len(in) != 1 || in[0].Cancel != "E1" {
		t.Fatal("cancel while entry working must cancel the entry order")
	}
	// The fill wins the race against the cancel: square off, never leave an orphan position.
	in = s.OnExecution("E1", "FILLED", 5, 10_000, "")
	if len(in) != 1 || in[0].Place == nil || s.ExitReason != "cancel" || s.State != ExitPending {
		t.Fatalf("fill after cancel must square off: %+v %s", in, s.State)
	}
	if _, err := s.OnCancel(); !errors.Is(err, ErrNotNow) {
		t.Fatal("cancel during exit")
	}

	s = longWithExits(t)
	s.OnTick(10_000)
	s.OrderPlaced("E1", true)
	s.OnCancel()
	s.OnExecution("E1", "CANCELLED", 0, 0, "")
	if s.State != Cancelled {
		t.Fatal(s.State)
	}
}

func TestValidation(t *testing.T) {
	bad := []Spec{
		{User: "u", Symbol: "X", Side: Long, Entry: Market, Trigger: 100, Qty: 1, Target: 90},    // target below entry
		{User: "u", Symbol: "X", Side: Short, Entry: Market, Trigger: 100, Qty: 1, Stoploss: 90}, // short stop below entry
		{User: "u", Symbol: "X", Side: Long, Entry: Limit, Trigger: 100, Qty: 1},                 // limit without price
		{User: "u", Symbol: "X", Side: Long, Entry: Market, Trigger: 100, Qty: 0},                // no qty
		{User: "u", Symbol: "X", Side: "SIDEWAYS", Entry: Market, Trigger: 100, Qty: 1},
	}
	for i, sp := range bad {
		if _, err := New("S", sp, 100); !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d accepted", i)
		}
	}
	if _, err := New("S", Spec{User: "u", Symbol: "X", Side: Long, Entry: Market, Trigger: 100, Qty: 1}, 0); !errors.Is(err, ErrNoPrice) {
		t.Fatal("no price yet")
	}
}
