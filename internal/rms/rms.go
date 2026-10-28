// Package rms is the risk gateway. Every order leaving Stocky and every execution arriving from
// the exchange passes through here, and nothing else can reach the OMS: the oms package lives at
// rms/internal/oms, so the Go compiler rejects an import of it from anywhere else.
//
// Outbound, Submit checks per-user margin and reserves it under one lock, so concurrent orders
// across symbols can never jointly exceed a user's limit. Entry orders must carry a price cap
// (market entries arrive as protected limit orders), so the value of a fill can never exceed what
// was reserved.
//
// Inbound, every exchange message is validated against the order book before the engine sees it:
// unknown orders, duplicate executions (by exec id), overfills and messages for orders already
// finished are dropped. Positions are the sum of validated fills and nothing else.
//
// The RMS also refuses a second exit order for a strategy whose exit is working or filled: target
// and stoploss are mutually exclusive here as well as in the strategy state machine.
package rms

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Shravan-Amberkar/stocky/internal/rms/internal/oms"
	"github.com/Shravan-Amberkar/stocky/internal/wire"
)

type Role string

const (
	Entry Role = "ENTRY"
	Exit  Role = "EXIT"
)

// Order is what the engine asks to send.
type Order struct {
	ClientID   string
	StrategyID string
	User       string
	Symbol     string
	Role       Role
	Side       wire.Side
	Type       wire.OrdType
	Qty        int64
	Limit      int64 // paise; required for entries (the price cap margin is reserved at)
}

type Kind string

const (
	Acked          Kind = "ACKED"
	Filled         Kind = "FILLED"
	Rejected       Kind = "REJECTED"
	Cancelled      Kind = "CANCELLED"
	CancelRejected Kind = "CANCEL_REJECTED"
)

// Execution is a validated exchange event, delivered to the engine exactly once.
type Execution struct {
	ClientID   string    `json:"client_id"`
	StrategyID string    `json:"strategy_id"`
	User       string    `json:"user"`
	Symbol     string    `json:"symbol"`
	Role       Role      `json:"role"`
	Kind       Kind      `json:"kind"`
	Side       wire.Side `json:"side"`
	Qty        int64     `json:"qty,omitempty"`
	Price      int64     `json:"price,omitempty"`
	ExecID     string    `json:"exec_id,omitempty"`
	Reason     string    `json:"reason,omitempty"`
}

var (
	ErrMargin      = errors.New("insufficient margin")
	ErrUnknownUser = errors.New("unknown user")
	ErrInvalid     = errors.New("invalid order")
	ErrDuplicate   = errors.New("duplicate client order id")
	ErrExit        = errors.New("exit not allowed: no open position, or an exit is already working or done")
)

type orderState int

const (
	sent orderState = iota
	acked
	filled
	rejected
	cancelled
)

type ord struct {
	o         Order
	state     orderState
	remaining int64
	execs     map[string]bool
}

func (o *ord) live() bool { return o.state == sent || o.state == acked }

type strat struct {
	entrySide  wire.Side
	entryQty   int64 // filled entry quantity
	entryValue int64 // margin in use for this strategy's position
	exitLive   bool
	exitDone   bool
}

type account struct {
	limit, used, reserved int64
	pos                   map[string]int64
}

// AccountView is a snapshot of one user's risk state.
type AccountView struct {
	Limit     int64            `json:"limit"`
	Used      int64            `json:"used"`
	Reserved  int64            `json:"reserved"`
	Positions map[string]int64 `json:"positions"`
}

type transport interface {
	Send(wire.Msg)
	Cancel(clientID, symbol string)
	Acked(clientID string)
	Done(clientID string)
	Unconfirmed() int
	Close()
}

type Gateway struct {
	log     *slog.Logger
	t       transport
	deliver func(Execution)

	mu     sync.Mutex
	accts  map[string]*account
	orders map[string]*ord
	strats map[string]*strat
	hook   func(user string, v AccountView)
}

// New connects to the exchange order port. deliver receives every validated execution; it must
// not block.
func New(limits map[string]int64, exchangeAddr string, deliver func(Execution), log *slog.Logger) *Gateway {
	g := newGateway(limits, deliver, log)
	c := oms.New(exchangeAddr, g.onWire, log)
	g.t = c
	c.Start()
	return g
}

func newGateway(limits map[string]int64, deliver func(Execution), log *slog.Logger) *Gateway {
	g := &Gateway{log: log, deliver: deliver, accts: map[string]*account{}, orders: map[string]*ord{}, strats: map[string]*strat{}}
	for u, l := range limits {
		g.accts[u] = &account{limit: l, pos: map[string]int64{}}
	}
	return g
}

func (g *Gateway) Close() { g.t.Close() }

// SetHook registers a callback run (under the lock) after every change to an account.
func (g *Gateway) SetHook(f func(user string, v AccountView)) {
	g.mu.Lock()
	g.hook = f
	g.mu.Unlock()
}

// Submit validates o against risk limits and, only if it passes, hands it to the OMS.
func (g *Gateway) Submit(o Order) error {
	if o.ClientID == "" || o.StrategyID == "" || o.Qty <= 0 || (o.Side != wire.Buy && o.Side != wire.Sell) {
		return ErrInvalid
	}
	g.mu.Lock()
	a, ok := g.accts[o.User]
	if !ok {
		g.mu.Unlock()
		return ErrUnknownUser
	}
	if _, dup := g.orders[o.ClientID]; dup {
		g.mu.Unlock()
		return ErrDuplicate
	}
	s := g.strats[o.StrategyID]
	switch o.Role {
	case Entry:
		if o.Type != wire.Limit || o.Limit <= 0 {
			g.mu.Unlock()
			return fmt.Errorf("%w: entries need a price cap", ErrInvalid)
		}
		value := o.Qty * o.Limit
		if a.used+a.reserved+value > a.limit {
			g.mu.Unlock()
			return ErrMargin
		}
		a.reserved += value
		if s == nil {
			s = &strat{entrySide: o.Side}
			g.strats[o.StrategyID] = s
		}
	case Exit:
		if s == nil || s.entryQty == 0 || s.exitLive || s.exitDone || o.Qty != s.entryQty || o.Side == s.entrySide {
			g.mu.Unlock()
			return ErrExit
		}
		s.exitLive = true
	default:
		g.mu.Unlock()
		return ErrInvalid
	}
	g.orders[o.ClientID] = &ord{o: o, state: sent, remaining: o.Qty, execs: map[string]bool{}}
	g.notify(o.User, a)
	g.mu.Unlock()

	g.t.Send(wire.Msg{Type: wire.New, ClientID: o.ClientID, User: o.User, Symbol: o.Symbol, Side: o.Side,
		OrdType: o.Type, Qty: o.Qty, Price: o.Limit})
	return nil
}

// Cancel requests cancellation of a live order.
func (g *Gateway) Cancel(clientID string) error {
	g.mu.Lock()
	o, ok := g.orders[clientID]
	if !ok || !o.live() {
		g.mu.Unlock()
		return ErrInvalid
	}
	sym := o.o.Symbol
	g.mu.Unlock()
	g.t.Cancel(clientID, sym)
	return nil
}

// onWire validates one inbound exchange message. Runs on the OMS reader goroutine.
func (g *Gateway) onWire(m wire.Msg) {
	g.mu.Lock()
	o, ok := g.orders[m.ClientID]
	if !ok {
		g.mu.Unlock()
		g.log.Warn("rms: message for unknown order dropped", "client_id", m.ClientID, "type", m.Type)
		return
	}
	a, s := g.accts[o.o.User], g.strats[o.o.StrategyID]
	ex := Execution{ClientID: o.o.ClientID, StrategyID: o.o.StrategyID, User: o.o.User, Symbol: o.o.Symbol,
		Role: o.o.Role, Side: o.o.Side}
	emit, terminal := false, false

	switch m.Type {
	case wire.Ack:
		if o.state == sent {
			o.state, ex.Kind, emit = acked, Acked, true
		}
		g.t.Acked(o.o.ClientID)
	case wire.Fill:
		if o.execs[m.ExecID] || !o.live() || m.Qty <= 0 || m.Qty > o.remaining || m.Price <= 0 {
			break // duplicate, late or impossible: the books are not touched
		}
		if o.o.Role == Entry && (o.o.Side == wire.Buy && m.Price > o.o.Limit || o.o.Side == wire.Sell && m.Price < o.o.Limit) {
			g.log.Error("rms: exchange filled through the limit price", "client_id", m.ClientID, "price", m.Price)
		}
		o.execs[m.ExecID] = true
		o.remaining -= m.Qty
		g.t.Acked(o.o.ClientID)
		if o.o.Side == wire.Buy {
			a.pos[o.o.Symbol] += m.Qty
		} else {
			a.pos[o.o.Symbol] -= m.Qty
		}
		if o.o.Role == Entry {
			// Margin in use is what was blocked when the RMS approved the order, never more: a buy
			// fills at or below its cap (so less may be used), but a sell can fill above its limit
			// and must not silently grow the user's exposure past what was checked.
			reservedPart := m.Qty * o.o.Limit
			a.reserved -= reservedPart
			use := min(m.Qty*m.Price, reservedPart)
			a.used += use
			s.entryQty += m.Qty
			s.entryValue += use
		} else if o.remaining == 0 {
			a.used -= s.entryValue
			s.exitLive, s.exitDone = false, true
		}
		ex.Kind, ex.Qty, ex.Price, ex.ExecID, emit = Filled, m.Qty, m.Price, m.ExecID, true
		if o.remaining == 0 {
			o.state, terminal = filled, true
		}
	case wire.Reject, wire.Cancelled:
		if !o.live() {
			break
		}
		if o.o.Role == Entry {
			a.reserved -= o.remaining * o.o.Limit
		} else {
			s.exitLive = false
		}
		o.state, ex.Kind = rejected, Rejected
		if m.Type == wire.Cancelled {
			o.state, ex.Kind = cancelled, Cancelled
		}
		ex.Reason, emit, terminal = m.Reason, true, true
	case wire.CancelReject:
		if o.live() {
			ex.Kind, ex.Reason, emit = CancelRejected, m.Reason, true
		}
	}
	if emit {
		g.notify(o.o.User, a)
	}
	g.mu.Unlock()
	if terminal {
		g.t.Done(o.o.ClientID)
	}
	if emit {
		g.deliver(ex)
	}
}

func (g *Gateway) notify(user string, a *account) {
	if g.hook != nil {
		g.hook(user, view(a))
	}
}

func view(a *account) AccountView {
	p := make(map[string]int64, len(a.pos))
	for k, v := range a.pos {
		if v != 0 {
			p[k] = v
		}
	}
	return AccountView{Limit: a.limit, Used: a.used, Reserved: a.reserved, Positions: p}
}

func (g *Gateway) Account(user string) (AccountView, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	a, ok := g.accts[user]
	if !ok {
		return AccountView{}, false
	}
	return view(a), true
}

func (g *Gateway) Accounts() map[string]AccountView {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]AccountView, len(g.accts))
	for u, a := range g.accts {
		out[u] = view(a)
	}
	return out
}

// Unconfirmed is the number of orders the exchange has not yet acknowledged (or whose cancel is
// outstanding). Zero means Stocky's view of every order is confirmed by the exchange.
func (g *Gateway) Unconfirmed() int { return g.t.Unconfirmed() }

// SentOrders returns every client order id this gateway let through.
func (g *Gateway) SentOrders() map[string]Order {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]Order, len(g.orders))
	for id, o := range g.orders {
		out[id] = o.o
	}
	return out
}
