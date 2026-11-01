// Package strategy is the per-strategy state machine. It is pure: methods take an event and
// return the orders to place or cancel, and never do I/O. The symbol actor that owns a strategy
// is the only caller, so no locking is needed and every rule can be unit-tested deterministically.
//
//	WAITING_ENTRY --trigger crossed--> ENTRY_PENDING --filled--> OPEN --target|stoploss crossed--> EXIT_PENDING --filled--> CLOSED
//	      | cancel                        | rejected / cancelled            ^                          | exit rejected
//	      v                               v                                 +--------------------------+ (re-armed; retried next tick)
//	  CANCELLED                     REJECTED / CANCELLED
//
// Target vs stoploss: an exit is only ever chosen in OPEN, and choosing one moves the strategy to
// EXIT_PENDING, where no exit condition is evaluated. Only one exit order can be working, and the
// RMS independently refuses a second exit for the same strategy.
package strategy

import (
	"errors"
	"fmt"

	"github.com/Shravan-Amberkar/stocky/internal/wire"
)

type Side string

const (
	Long  Side = "LONG"
	Short Side = "SHORT"
)

type EntryType string

const (
	Market EntryType = "MARKET"
	Limit  EntryType = "LIMIT"
)

type State string

const (
	WaitingEntry State = "WAITING_ENTRY"
	EntryPending State = "ENTRY_PENDING"
	Open         State = "OPEN"
	ExitPending  State = "EXIT_PENDING"
	Closed       State = "CLOSED"
	Cancelled    State = "CANCELLED"
	Rejected     State = "REJECTED"
)

func (s State) Terminal() bool { return s == Closed || s == Cancelled || s == Rejected }

// MarketProtectionBps caps how far a "market" entry may fill from the price that triggered it:
// market entries go to the exchange as limit orders at last price ± this band. That cap is what
// lets the RMS reserve margin that a fill can never exceed, even through a price gap.
const MarketProtectionBps = 300 // 3%

// Spec is what a user asks for. Prices are paise.
type Spec struct {
	User     string    `json:"user"`
	Symbol   string    `json:"symbol"`
	Side     Side      `json:"side"`
	Entry    EntryType `json:"entry"`
	Trigger  int64     `json:"trigger"`
	Limit    int64     `json:"limit,omitempty"` // limit entries only
	Qty      int64     `json:"qty"`
	Target   int64     `json:"target,omitempty"`   // 0 = none
	Stoploss int64     `json:"stoploss,omitempty"` // 0 = none
}

type Strategy struct {
	ID   string `json:"id"`
	Spec `json:"spec"`

	State        State  `json:"state"`
	TriggerAbove bool   `json:"trigger_above"` // fire when price rises to Trigger (else when it falls to it)
	EntryOrder   string `json:"entry_order,omitempty"`
	ExitOrder    string `json:"exit_order,omitempty"`
	ExitReason   string `json:"exit_reason,omitempty"` // target | stoploss | cancel
	FilledQty    int64  `json:"filled_qty"`
	EntryPrice   int64  `json:"entry_price,omitempty"`
	ExitPrice    int64  `json:"exit_price,omitempty"`
	CancelAsked  bool   `json:"cancel_requested,omitempty"`
	ExitAttempts int    `json:"exit_attempts,omitempty"`
	Note         string `json:"note,omitempty"`
}

// Intent is an order action the actor must carry out through the RMS.
type Intent struct {
	Place  *Place
	Cancel string // client order id to cancel
}

type Place struct {
	Entry bool // false = exit
	Side  wire.Side
	Type  wire.OrdType
	Qty   int64
	Limit int64
	Why   string
}

var (
	ErrInvalid = errors.New("invalid strategy")
	ErrNotNow  = errors.New("cannot cancel: exit already in progress or strategy finished")
	ErrNoPrice = errors.New("no price yet for symbol")
)

// New validates a spec against the current price and returns a WAITING_ENTRY strategy.
func New(id string, sp Spec, lastPrice int64) (*Strategy, error) {
	bad := func(f string, a ...any) error { return fmt.Errorf("%w: "+f, append([]any{ErrInvalid}, a...)...) }
	switch {
	case sp.User == "" || sp.Symbol == "":
		return nil, bad("user and symbol are required")
	case sp.Side != Long && sp.Side != Short:
		return nil, bad("side must be LONG or SHORT")
	case sp.Entry != Market && sp.Entry != Limit:
		return nil, bad("entry must be MARKET or LIMIT")
	case sp.Qty <= 0 || sp.Trigger <= 0 || sp.Target < 0 || sp.Stoploss < 0:
		return nil, bad("qty and trigger must be positive")
	case sp.Entry == Limit && sp.Limit <= 0:
		return nil, bad("limit entry needs a limit price")
	case sp.Entry == Market && sp.Limit != 0:
		return nil, bad("market entry takes no limit price")
	case lastPrice <= 0:
		return nil, ErrNoPrice
	}
	ref := sp.Trigger
	if sp.Entry == Limit {
		ref = sp.Limit
	}
	if sp.Side == Long && (sp.Target != 0 && sp.Target <= ref || sp.Stoploss != 0 && sp.Stoploss >= ref) ||
		sp.Side == Short && (sp.Target != 0 && sp.Target >= ref || sp.Stoploss != 0 && sp.Stoploss <= ref) {
		return nil, bad("target must be on the profit side and stoploss on the loss side of the entry price")
	}
	return &Strategy{ID: id, Spec: sp, State: WaitingEntry, TriggerAbove: sp.Trigger >= lastPrice}, nil
}

func (s *Strategy) entrySide() wire.Side {
	if s.Side == Long {
		return wire.Buy
	}
	return wire.Sell
}

func (s *Strategy) exitSide() wire.Side {
	if s.Side == Long {
		return wire.Sell
	}
	return wire.Buy
}

// OnTick evaluates entry and exit conditions at price px. Conditions are inequalities, so a tick
// that jumps several price levels past a trigger still fires it.
func (s *Strategy) OnTick(px int64) []Intent {
	switch s.State {
	case WaitingEntry:
		if s.TriggerAbove && px >= s.Trigger || !s.TriggerAbove && px <= s.Trigger {
			return s.placeEntry(px)
		}
	case Open:
		if reason := s.exitHit(px); reason != "" {
			return s.placeExit(reason)
		}
	}
	return nil
}

func (s *Strategy) exitHit(px int64) string {
	if s.Side == Long {
		switch {
		case s.Target != 0 && px >= s.Target:
			return "target"
		case s.Stoploss != 0 && px <= s.Stoploss:
			return "stoploss"
		}
	} else {
		switch {
		case s.Target != 0 && px <= s.Target:
			return "target"
		case s.Stoploss != 0 && px >= s.Stoploss:
			return "stoploss"
		}
	}
	return ""
}

func (s *Strategy) placeEntry(px int64) []Intent {
	limit := s.Spec.Limit
	if s.Entry == Market {
		band := px * MarketProtectionBps / 10_000
		if s.Side == Long {
			limit = px + band
		} else {
			limit = px - band
		}
	}
	s.State = EntryPending
	return []Intent{{Place: &Place{Entry: true, Side: s.entrySide(), Type: wire.Limit, Qty: s.Qty, Limit: limit,
		Why: "entry trigger"}}}
}

func (s *Strategy) placeExit(reason string) []Intent {
	s.State, s.ExitReason = ExitPending, reason
	s.ExitAttempts++
	return []Intent{{Place: &Place{Side: s.exitSide(), Type: wire.Market, Qty: s.FilledQty, Why: reason}}}
}

// OrderPlaced records the client order id the actor assigned to the order just placed.
func (s *Strategy) OrderPlaced(clientID string, entry bool) {
	if entry {
		s.EntryOrder = clientID
	} else {
		s.ExitOrder = clientID
	}
}

// OrderDenied: the RMS refused the order before it reached the exchange.
func (s *Strategy) OrderDenied(entry bool, reason string) {
	if entry {
		s.State, s.Note = Rejected, "entry denied by RMS: "+reason
		return
	}
	// The RMS only refuses an exit it believes is already working or done, which the state machine
	// should never produce. Never strand the strategy in EXIT_PENDING: go back to OPEN, so the next
	// tick re-evaluates and retries.
	s.State, s.ExitOrder, s.Note = Open, "", "exit denied by RMS: "+reason+"; re-armed"
}

// OnExecution applies a validated exchange event for one of this strategy's orders.
func (s *Strategy) OnExecution(clientID, kind string, qty, px int64, reason string) []Intent {
	entry := clientID == s.EntryOrder
	if !entry && clientID != s.ExitOrder {
		return nil // an older exit attempt; the RMS has already accounted for it
	}
	switch kind {
	case "FILLED":
		if entry {
			if s.State != EntryPending {
				return nil
			}
			s.FilledQty += qty
			s.EntryPrice = px
			if s.FilledQty < s.Qty {
				return nil
			}
			s.State = Open
			if s.CancelAsked {
				return s.placeExit("cancel") // the fill won the race with the cancel: square off
			}
			return nil
		}
		if s.State == ExitPending {
			s.State, s.ExitPrice = Closed, px
		}
	case "REJECTED", "CANCELLED":
		if entry && s.State == EntryPending && s.FilledQty == 0 {
			s.State, s.Note = Rejected, "entry "+kind+": "+reason
			if kind == "CANCELLED" {
				s.State, s.Note = Cancelled, "cancelled by user"
			}
		}
		if !entry && s.State == ExitPending {
			s.State, s.ExitOrder, s.Note = Open, "", "exit rejected ("+reason+"); re-armed"
			if s.CancelAsked {
				return s.placeExit("cancel")
			}
		}
	}
	return nil
}

// OnCancel handles a user's cancel. Before entry it simply cancels; while the entry order is
// working it cancels that order (and squares off if the fill wins); with an open position it
// squares off at market, so a cancelled strategy never leaves an orphaned position.
func (s *Strategy) OnCancel() ([]Intent, error) {
	switch s.State {
	case WaitingEntry:
		s.State, s.Note = Cancelled, "cancelled by user"
		return nil, nil
	case EntryPending:
		s.CancelAsked = true
		if s.EntryOrder == "" {
			return nil, nil
		}
		return []Intent{{Cancel: s.EntryOrder}}, nil
	case Open:
		s.CancelAsked = true
		return s.placeExit("cancel"), nil
	}
	return nil, ErrNotNow
}
