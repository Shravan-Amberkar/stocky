// Package exchange is a mock, deliberately imperfect exchange.
//
// It speaks the wire protocol on two TCP listeners: an order session and a market-data stream.
// NEW and CANCEL are idempotent by client order id, which is what lets the broker resend safely
// after a dropped connection. Every misbehaviour (dropped connections, delayed, duplicated and
// reordered messages, rejections, a slow symbol) is a runtime toggle in Chaos.
package exchange

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shravan-Amberkar/stocky/internal/wire"
)

// Chaos toggles. Zero value = a perfect exchange. Probabilities are per message.
type Chaos struct {
	Drop        float64                  `json:"drop"`         // close the order connection (before handling an inbound or writing an outbound message)
	Delay       time.Duration            `json:"delay"`        // random extra latency up to this, per outbound message
	Duplicate   float64                  `json:"duplicate"`    // send an outbound message twice
	Reorder     float64                  `json:"reorder"`      // hold an outbound message back so later ones overtake it
	Reject      float64                  `json:"reject"`       // reject a NEW order
	Partial     float64                  `json:"partial"`      // fill only part of an order; the rest rests and fills on later ticks
	SymbolDelay map[string]time.Duration `json:"symbol_delay"` // fixed extra latency for every message about one symbol
}

type orderState string

const (
	stOpen      orderState = "OPEN"
	stFilled    orderState = "FILLED"
	stRejected  orderState = "REJECTED"
	stCancelled orderState = "CANCELLED"
)

type order struct {
	m         wire.Msg
	state     orderState
	remaining int64
	fills     []wire.Msg
	owner     *session // the session that last spoke about this order; its fills go there
}

// FillRecord is the exchange's own ledger: the ground truth a broker's positions must match.
type FillRecord struct {
	ClientID, User, Symbol, ExecID string
	Side                           wire.Side
	Qty, Price                     int64
}

type Exchange struct {
	log   *slog.Logger
	chaos atomic.Pointer[Chaos]

	mu      sync.Mutex
	prices  map[string]int64
	orders  map[string]*order
	fills   []FillRecord
	execSeq int64
	active  *session // the latest order session; asynchronous fills go here
	md      map[*mdConn]struct{}

	tickSize int64
}

func New(prices map[string]int64, log *slog.Logger) *Exchange {
	e := &Exchange{log: log, prices: map[string]int64{}, orders: map[string]*order{}, md: map[*mdConn]struct{}{}, tickSize: 5}
	for s, p := range prices {
		e.prices[s] = p
	}
	e.chaos.Store(&Chaos{})
	return e
}

func (e *Exchange) SetChaos(c Chaos) { e.chaos.Store(&c) }
func (e *Exchange) Chaos() Chaos     { return *e.chaos.Load() }

func (e *Exchange) roll(p float64) bool { return p > 0 && rand.Float64() < p }

// ---------------------------------------------------------------- listeners

// ServeOrders accepts order sessions until the listener closes.
func (e *Exchange) ServeOrders(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		s := newSession(e, c)
		e.mu.Lock()
		e.active = s
		e.mu.Unlock()
		go s.readLoop()
	}
}

// ServeMarketData streams every price change to every subscriber.
func (e *Exchange) ServeMarketData(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		m := &mdConn{c: c, w: wire.NewWriter(c), q: make(chan wire.Msg, 4096)}
		e.mu.Lock()
		e.md[m] = struct{}{}
		snapshot := make([]wire.Msg, 0, len(e.prices))
		for s, p := range e.prices {
			snapshot = append(snapshot, wire.Msg{Type: wire.Tick, Symbol: s, Price: p})
		}
		e.mu.Unlock()
		for _, t := range snapshot {
			m.q <- t
		}
		go m.run(e)
	}
}

type mdConn struct {
	c net.Conn
	w *wire.Writer
	q chan wire.Msg
}

func (m *mdConn) run(e *Exchange) {
	defer func() {
		m.c.Close()
		e.mu.Lock()
		delete(e.md, m)
		e.mu.Unlock()
	}()
	for t := range m.q {
		if m.w.Write(t) != nil {
			return
		}
	}
}

// ---------------------------------------------------------------- prices

// SetPrice moves a symbol's price (any distance: gaps are allowed) and matches resting orders.
func (e *Exchange) SetPrice(symbol string, px int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.prices[symbol]; !ok || px <= 0 {
		return
	}
	e.prices[symbol] = px
	for m := range e.md {
		select {
		case m.q <- wire.Msg{Type: wire.Tick, Symbol: symbol, Price: px}:
		default:
			m.c.Close() // a subscriber too slow to keep up is cut off rather than slowing the exchange
		}
	}
	ids := make([]string, 0)
	for id, o := range e.orders {
		if o.state == stOpen && o.m.Symbol == symbol {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		e.match(e.orders[id])
	}
}

func (e *Exchange) Price(symbol string) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.prices[symbol]
}

// Simulate random-walks every symbol each interval; with probability gapProb a tick jumps up to
// gapMax price levels at once.
func (e *Exchange) Simulate(interval time.Duration, gapProb float64, gapMax int, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		e.mu.Lock()
		syms := make([]string, 0, len(e.prices))
		for s := range e.prices {
			syms = append(syms, s)
		}
		e.mu.Unlock()
		for _, s := range syms {
			levels := int64(rand.IntN(3) - 1)
			if e.roll(gapProb) {
				levels = int64(rand.IntN(gapMax) + 2)
				if rand.IntN(2) == 0 {
					levels = -levels
				}
			}
			if px := e.Price(s) + levels*e.tickSize; px > 0 {
				e.SetPrice(s, px)
			}
		}
	}
}

// ---------------------------------------------------------------- matching (caller holds e.mu)

// match fills o if it is marketable. A market order always completes, possibly in several pieces;
// a limit order may fill only partly (Chaos.Partial) and rest the remainder for later ticks.
func (e *Exchange) match(o *order) {
	px := e.prices[o.m.Symbol]
	switch {
	case o.m.OrdType == wire.Market:
	case o.m.Side == wire.Buy && px <= o.m.Price:
	case o.m.Side == wire.Sell && px >= o.m.Price:
	default:
		return // limit not marketable; rests
	}
	for o.remaining > 0 {
		qty := o.remaining
		if qty > 1 && e.roll(e.chaos.Load().Partial) {
			qty = 1 + rand.Int64N(qty-1)
		}
		e.execSeq++
		f := wire.Msg{Type: wire.Fill, ClientID: o.m.ClientID, Symbol: o.m.Symbol, Side: o.m.Side, Qty: qty, Price: px,
			ExecID: fmt.Sprintf("X%06d", e.execSeq)}
		o.fills = append(o.fills, f)
		o.remaining -= qty
		e.fills = append(e.fills, FillRecord{ClientID: o.m.ClientID, User: o.m.User, Symbol: o.m.Symbol, ExecID: f.ExecID,
			Side: o.m.Side, Qty: qty, Price: px})
		if s := o.owner; s != nil && !s.closed.Load() {
			s.send(f)
		} else if e.active != nil {
			e.active.send(f)
		}
		if o.m.OrdType == wire.Limit {
			break // a part-filled limit rests; later ticks fill the rest
		}
	}
	if o.remaining == 0 {
		o.state = stFilled
	}
}

// replay sends an order's full current state; this is the answer to a repeated NEW.
func (e *Exchange) replay(s *session, o *order) {
	switch o.state {
	case stRejected:
		s.send(wire.Msg{Type: wire.Reject, ClientID: o.m.ClientID, Symbol: o.m.Symbol, Reason: "rejected"})
		return
	}
	s.send(wire.Msg{Type: wire.Ack, ClientID: o.m.ClientID, Symbol: o.m.Symbol})
	for _, f := range o.fills {
		s.send(f)
	}
	if o.state == stCancelled {
		s.send(wire.Msg{Type: wire.Cancelled, ClientID: o.m.ClientID, Symbol: o.m.Symbol, CumQty: o.m.Qty - o.remaining})
	}
}

func (e *Exchange) handle(s *session, m wire.Msg) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch m.Type {
	case wire.New:
		if o, ok := e.orders[m.ClientID]; ok {
			o.owner = s
			e.replay(s, o)
			return
		}
		o := &order{m: m, state: stOpen, remaining: m.Qty, owner: s}
		e.orders[m.ClientID] = o
		_, known := e.prices[m.Symbol]
		switch {
		case !known || m.Qty <= 0 || (m.OrdType == wire.Limit && m.Price <= 0):
			o.state = stRejected
			s.send(wire.Msg{Type: wire.Reject, ClientID: m.ClientID, Symbol: m.Symbol, Reason: "invalid order"})
		case e.roll(e.chaos.Load().Reject):
			o.state = stRejected
			s.send(wire.Msg{Type: wire.Reject, ClientID: m.ClientID, Symbol: m.Symbol, Reason: "rejected by exchange"})
		default:
			s.send(wire.Msg{Type: wire.Ack, ClientID: m.ClientID, Symbol: m.Symbol})
			e.match(o)
		}
	case wire.Cancel:
		o, ok := e.orders[m.ClientID]
		if ok {
			o.owner = s
		}
		switch {
		case !ok:
			s.send(wire.Msg{Type: wire.CancelReject, ClientID: m.ClientID, Symbol: m.Symbol, Reason: "unknown order"})
		case o.state == stOpen:
			o.state = stCancelled
			s.send(wire.Msg{Type: wire.Cancelled, ClientID: m.ClientID, Symbol: o.m.Symbol, CumQty: o.m.Qty - o.remaining})
		case o.state == stCancelled:
			s.send(wire.Msg{Type: wire.Cancelled, ClientID: m.ClientID, Symbol: o.m.Symbol, CumQty: o.m.Qty - o.remaining})
		default:
			s.send(wire.Msg{Type: wire.CancelReject, ClientID: m.ClientID, Symbol: o.m.Symbol, Reason: "already " + string(o.state)})
		}
	}
}

// ---------------------------------------------------------------- order session with chaos

type session struct {
	e      *Exchange
	c      net.Conn
	w      *wire.Writer
	closed atomic.Bool
}

func newSession(e *Exchange, c net.Conn) *session { return &session{e: e, c: c, w: wire.NewWriter(c)} }

func (s *session) close() {
	if s.closed.CompareAndSwap(false, true) {
		s.c.Close()
	}
}

func (s *session) readLoop() {
	defer s.close()
	r := wire.NewReader(s.c)
	for {
		m, err := r.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) && !s.closed.Load() {
				s.e.log.Debug("exchange: read", "err", err)
			}
			return
		}
		if s.e.roll(s.e.chaos.Load().Drop) {
			return // connection drops before the message is processed
		}
		s.e.handle(s, m)
	}
}

// send applies the chaos toggles to one outbound message. Delayed messages are written by their
// own goroutine, so later messages can overtake them (reordering).
func (s *session) send(m wire.Msg) {
	c := s.e.chaos.Load()
	if s.closed.Load() {
		return
	}
	if s.e.roll(c.Drop) {
		s.close() // connection drops; this and any queued messages are lost
		return
	}
	d := c.SymbolDelay[m.Symbol]
	if c.Delay > 0 {
		d += time.Duration(rand.Int64N(int64(c.Delay)))
	}
	if s.e.roll(c.Reorder) {
		d += 30 * time.Millisecond
	}
	copies := 1
	if s.e.roll(c.Duplicate) {
		copies = 2
	}
	for range copies {
		if d == 0 {
			if s.w.Write(m) != nil {
				s.close()
			}
			continue
		}
		go func() {
			time.Sleep(d)
			if !s.closed.Load() && s.w.Write(m) != nil {
				s.close()
			}
		}()
	}
}

// ---------------------------------------------------------------- inspection (tests, admin)

func (e *Exchange) Fills() []FillRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]FillRecord(nil), e.fills...)
}

type OrderView struct {
	ClientID, User, Symbol, State string
	Side                          wire.Side
	Qty                           int64
}

func (e *Exchange) Orders() map[string]OrderView {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]OrderView, len(e.orders))
	for id, o := range e.orders {
		out[id] = OrderView{ClientID: id, User: o.m.User, Symbol: o.m.Symbol, State: string(o.state), Side: o.m.Side, Qty: o.m.Qty}
	}
	return out
}

// DropActive closes the current order session (lets tests force a disconnect at a chosen moment).
func (e *Exchange) DropActive() {
	e.mu.Lock()
	s := e.active
	e.mu.Unlock()
	if s != nil {
		s.close()
	}
}
