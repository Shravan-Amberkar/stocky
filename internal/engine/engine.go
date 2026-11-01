// Package engine runs one actor (goroutine) per symbol. An actor is the only code that touches its
// symbol's strategies: ticks, user commands and validated executions queue in its mailbox and are
// applied one at a time, so strategy state needs no locks. Mailboxes are unbounded and never block
// the sender, so a slow symbol (a delayed exchange reply, a burst of triggers) can never hold up
// the market-data reader, the OMS reader, or any other symbol.
package engine

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shravan-Amberkar/stocky/internal/rms"
	"github.com/Shravan-Amberkar/stocky/internal/strategy"
)

// Publisher pushes per-user events to clients (the WebSocket hub, or anything else).
type Publisher interface {
	Publish(user string, event Event)
}

type Event struct {
	Type     string             `json:"type"` // strategy | order
	Strategy *strategy.Strategy `json:"strategy,omitempty"`
	Order    *rms.Execution     `json:"order,omitempty"`
}

// Risk is the slice of the RMS the engine uses.
type Risk interface {
	Submit(rms.Order) error
	Cancel(clientID string) error
}

var ErrUnknownSymbol = errors.New("unknown symbol")
var ErrNotFound = errors.New("strategy not found")

type Engine struct {
	log    *slog.Logger
	pub    Publisher
	risk   Risk
	runID  string
	nextID atomic.Int64
	actors map[string]*actor

	mu    sync.RWMutex
	snaps map[string]strategy.Strategy // read model for the API, written by actors

	prices sync.Map // symbol -> last price, for display only
}

func New(symbols []string, pub Publisher, log *slog.Logger) *Engine {
	b := make([]byte, 3)
	rand.Read(b) //nolint:errcheck
	e := &Engine{log: log, pub: pub, runID: hex.EncodeToString(b), actors: map[string]*actor{},
		snaps: map[string]strategy.Strategy{}}
	for _, s := range symbols {
		e.actors[s] = &actor{e: e, symbol: s, strats: map[string]*strategy.Strategy{}, mb: newMailbox()}
	}
	return e
}

// Start attaches the risk gateway and starts the actors.
func (e *Engine) Start(r Risk) {
	e.risk = r
	for _, a := range e.actors {
		go a.run()
	}
}

func (e *Engine) Stop() {
	for _, a := range e.actors {
		a.mb.close()
	}
}

// OnTick and Deliver are called by the feed and the RMS. Both return immediately.
func (e *Engine) OnTick(symbol string, px int64) {
	if a := e.actors[symbol]; a != nil {
		e.prices.Store(symbol, px)
		a.mb.put(tick{px})
	}
}

// Prices returns the last price seen per symbol (display only; actors keep their own).
func (e *Engine) Prices() map[string]int64 {
	out := map[string]int64{}
	for s := range e.actors {
		if v, ok := e.prices.Load(s); ok {
			out[s] = v.(int64)
		} else {
			out[s] = 0
		}
	}
	return out
}

func (e *Engine) Deliver(x rms.Execution) {
	if a := e.actors[x.Symbol]; a != nil {
		a.mb.put(x)
	}
}

type reply struct {
	s   strategy.Strategy
	err error
}

type createCmd struct {
	spec  strategy.Spec
	reply chan reply
}

type cancelCmd struct {
	id    string
	reply chan reply
}

func (e *Engine) ask(symbol string, cmd any, ch chan reply) (strategy.Strategy, error) {
	a := e.actors[symbol]
	if a == nil {
		return strategy.Strategy{}, ErrUnknownSymbol
	}
	a.mb.put(cmd)
	select {
	case r := <-ch:
		return r.s, r.err
	case <-time.After(5 * time.Second):
		return strategy.Strategy{}, errors.New("timed out waiting for symbol actor")
	}
}

func (e *Engine) Create(sp strategy.Spec) (strategy.Strategy, error) {
	ch := make(chan reply, 1)
	return e.ask(sp.Symbol, createCmd{sp, ch}, ch)
}

func (e *Engine) Cancel(id string) (strategy.Strategy, error) {
	s, ok := e.Get(id)
	if !ok {
		return s, ErrNotFound
	}
	ch := make(chan reply, 1)
	return e.ask(s.Symbol, cancelCmd{id, ch}, ch)
}

func (e *Engine) Get(id string) (strategy.Strategy, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s, ok := e.snaps[id]
	return s, ok
}

// List returns a user's strategies ("" = all), oldest first.
func (e *Engine) List(user string) []strategy.Strategy {
	e.mu.RLock()
	out := make([]strategy.Strategy, 0, len(e.snaps))
	for _, s := range e.snaps {
		if user == "" || s.User == user {
			out = append(out, s)
		}
	}
	e.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (e *Engine) save(s *strategy.Strategy) {
	e.mu.Lock()
	e.snaps[s.ID] = *s
	e.mu.Unlock()
	cp := *s
	e.pub.Publish(s.User, Event{Type: "strategy", Strategy: &cp})
}

// ---------------------------------------------------------------- actor

type tick struct{ px int64 }

type actor struct {
	e       *Engine
	symbol  string
	mb      *mailbox
	lastPx  int64
	strats  map[string]*strategy.Strategy
	order   []string // strategy ids in creation order: deterministic evaluation
	nOrders int64
}

func (a *actor) run() {
	for {
		batch, ok := a.mb.take()
		if !ok {
			return
		}
		for _, m := range batch {
			a.handle(m)
		}
	}
}

func (a *actor) handle(m any) {
	defer func() {
		if r := recover(); r != nil {
			a.e.log.Error("actor panic; event skipped", "symbol", a.symbol, "panic", r)
		}
	}()
	switch m := m.(type) {
	case tick:
		a.lastPx = m.px
		for _, id := range a.order {
			s := a.strats[id]
			if s.State.Terminal() {
				continue
			}
			before := s.State
			intents := s.OnTick(m.px)
			if len(intents) > 0 || s.State != before {
				a.apply(s, intents)
			}
		}
	case rms.Execution:
		cp := m
		a.e.pub.Publish(m.User, Event{Type: "order", Order: &cp})
		s := a.strats[m.StrategyID]
		if s == nil {
			return
		}
		a.apply(s, s.OnExecution(m.ClientID, string(m.Kind), m.Qty, m.Price, m.Reason))
	case createCmd:
		id := fmt.Sprintf("S%s-%05d", a.e.runID, a.e.nextID.Add(1))
		s, err := strategy.New(id, m.spec, a.lastPx)
		if err != nil {
			m.reply <- reply{err: err}
			return
		}
		a.strats[id] = s
		a.order = append(a.order, id)
		a.e.save(s)
		m.reply <- reply{s: *s}
		if a.lastPx > 0 {
			a.apply(s, s.OnTick(a.lastPx)) // a trigger already crossed fires at once
		}
	case cancelCmd:
		s := a.strats[m.id]
		if s == nil {
			m.reply <- reply{err: ErrNotFound}
			return
		}
		intents, err := s.OnCancel()
		if err != nil {
			m.reply <- reply{s: *s, err: err}
			return
		}
		a.apply(s, intents)
		m.reply <- reply{s: *s}
	}
}

// apply carries out a strategy's intents through the RMS, feeding denials straight back.
func (a *actor) apply(s *strategy.Strategy, intents []strategy.Intent) {
	for _, in := range intents {
		if in.Cancel != "" {
			if err := a.e.risk.Cancel(in.Cancel); err != nil {
				a.e.log.Debug("cancel not sent", "client_id", in.Cancel, "err", err)
			}
			continue
		}
		p := in.Place
		a.nOrders++
		role, tag := rms.Exit, "X"
		if p.Entry {
			role, tag = rms.Entry, "E"
		}
		cid := fmt.Sprintf("%s-%s%d", s.ID, tag, a.nOrders)
		s.OrderPlaced(cid, p.Entry)
		err := a.e.risk.Submit(rms.Order{ClientID: cid, StrategyID: s.ID, User: s.User, Symbol: s.Symbol, Role: role,
			Side: p.Side, Type: p.Type, Qty: p.Qty, Limit: p.Limit})
		if err != nil {
			s.OrderDenied(p.Entry, err.Error())
		}
	}
	a.e.save(s)
}

// ---------------------------------------------------------------- mailbox

// mailbox is an unbounded FIFO: put never blocks, take blocks until there is work.
type mailbox struct {
	mu     sync.Mutex
	q      []any
	signal chan struct{}
	closed bool
}

func newMailbox() *mailbox { return &mailbox{signal: make(chan struct{}, 1)} }

func (m *mailbox) put(x any) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.q = append(m.q, x)
	m.mu.Unlock()
	select {
	case m.signal <- struct{}{}:
	default:
	}
}

func (m *mailbox) take() ([]any, bool) {
	for {
		m.mu.Lock()
		if len(m.q) > 0 {
			b := m.q
			m.q = nil
			m.mu.Unlock()
			return b, true
		}
		if m.closed {
			m.mu.Unlock()
			return nil, false
		}
		m.mu.Unlock()
		<-m.signal
	}
}

func (m *mailbox) close() {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	select {
	case m.signal <- struct{}{}:
	default:
	}
}
