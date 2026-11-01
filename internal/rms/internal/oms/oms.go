// Package oms owns the TCP order session with the exchange.
//
// It lives under rms/internal/, so Go's internal-package rule lets ONLY the rms package import
// it: there is no code path to the exchange that does not go through the RMS, and the compiler
// enforces that.
//
// Sending never blocks the caller: messages go onto an unbounded queue drained by a single writer
// goroutine, so a symbol actor submitting an order never waits on the socket, and a stalled
// connection cannot hold up any symbol's processing.
//
// Never lose an order: every order is recorded as in flight before it is written, and stays in
// flight until the RMS reports a terminal outcome. After every reconnect, and whenever an order
// goes unanswered, it is resent. That is safe because NEW and CANCEL are idempotent by client
// order id at the exchange, which answers a repeat with the order's current state.
package oms

import (
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/Shravan-Amberkar/stocky/internal/wire"
)

type entry struct {
	newMsg   wire.Msg
	cancel   *wire.Msg
	acked    bool
	lastSent time.Time
	seq      int64 // submission order, so resends preserve it
}

type Client struct {
	addr     string
	onMsg    func(wire.Msg) // called for every inbound message, from the reader goroutine; must not block
	log      *slog.Logger
	resendAt time.Duration // unanswered NEW/CANCEL is resent after this
	recheck  time.Duration // an acknowledged but unfinished order is re-queried after this

	mu       sync.Mutex
	inflight map[string]*entry
	nextSeq  int64
	w        *wire.Writer // nil while disconnected
	conn     net.Conn
	queue    []wire.Msg // outbound, for the current connection only
	qsig     chan struct{}
	stop     chan struct{}
	stopped  bool
}

func New(addr string, onMsg func(wire.Msg), log *slog.Logger) *Client {
	return &Client{addr: addr, onMsg: onMsg, log: log, resendAt: time.Second, recheck: 5 * time.Second,
		inflight: map[string]*entry{}, stop: make(chan struct{}), qsig: make(chan struct{}, 1)}
}

// Start connects and keeps reconnecting until Close.
func (c *Client) Start() {
	go c.connectLoop()
	go c.resendLoop()
	go c.writeLoop()
}

func (c *Client) Close() {
	c.mu.Lock()
	if !c.stopped {
		c.stopped = true
		close(c.stop)
		if c.conn != nil {
			c.conn.Close()
		}
	}
	c.mu.Unlock()
}

// Send records a NEW as in flight and queues it. If not connected, the reconnect resend delivers it.
func (c *Client) Send(m wire.Msg) {
	c.mu.Lock()
	c.nextSeq++
	c.inflight[m.ClientID] = &entry{newMsg: m, lastSent: time.Now(), seq: c.nextSeq}
	c.enqueueLocked(m)
	c.mu.Unlock()
}

// enqueueLocked queues m for the current connection (dropped if disconnected: in-flight orders
// are resent on reconnect anyway). Caller holds c.mu.
func (c *Client) enqueueLocked(ms ...wire.Msg) {
	if c.w == nil {
		return
	}
	c.queue = append(c.queue, ms...)
	select {
	case c.qsig <- struct{}{}:
	default:
	}
}

func (c *Client) writeLoop() {
	for {
		select {
		case <-c.stop:
			return
		case <-c.qsig:
		}
		c.mu.Lock()
		q, w := c.queue, c.w
		c.queue = nil
		c.mu.Unlock()
		for _, m := range q {
			if w == nil || w.Write(m) != nil {
				c.dropConn(w)
				break
			}
		}
	}
}

// Cancel asks the exchange to cancel an in-flight order.
func (c *Client) Cancel(clientID, symbol string) {
	m := wire.Msg{Type: wire.Cancel, ClientID: clientID, Symbol: symbol}
	c.mu.Lock()
	e, ok := c.inflight[clientID]
	if ok {
		e.cancel = &m
		e.lastSent = time.Now()
	}
	if ok {
		c.enqueueLocked(m)
	}
	c.mu.Unlock()
}

// Acked marks an order as having reached the exchange.
func (c *Client) Acked(clientID string) {
	c.mu.Lock()
	if e, ok := c.inflight[clientID]; ok {
		e.acked = true
	}
	c.mu.Unlock()
}

// Done removes an order that has reached a terminal state.
func (c *Client) Done(clientID string) {
	c.mu.Lock()
	delete(c.inflight, clientID)
	c.mu.Unlock()
}

// Unconfirmed counts in-flight orders not yet acknowledged, or with a cancel outstanding.
func (c *Client) Unconfirmed() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.inflight {
		if !e.acked || e.cancel != nil {
			n++
		}
	}
	return n
}

func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.w != nil
}

func (c *Client) dropConn(w *wire.Writer) {
	c.mu.Lock()
	if w != nil && c.w == w && c.conn != nil {
		c.conn.Close()
		c.w, c.conn = nil, nil
	}
	c.mu.Unlock()
}

// resendAll queues every in-flight order (and pending cancel) in submission order.
func (c *Client) resendAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	es := make([]*entry, 0, len(c.inflight))
	for _, e := range c.inflight {
		es = append(es, e)
		e.lastSent = time.Now()
	}
	sort.Slice(es, func(i, j int) bool { return es[i].seq < es[j].seq })
	for _, e := range es {
		c.enqueueLocked(e.newMsg)
		if e.cancel != nil {
			c.enqueueLocked(*e.cancel)
		}
	}
}

func (c *Client) connectLoop() {
	backoff := 50 * time.Millisecond
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		conn, err := net.DialTimeout("tcp", c.addr, 2*time.Second)
		if err != nil {
			select {
			case <-c.stop:
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 2*time.Second)
			continue
		}
		backoff = 50 * time.Millisecond
		w := wire.NewWriter(conn)
		c.mu.Lock()
		if c.stopped {
			c.mu.Unlock()
			conn.Close()
			return
		}
		c.conn, c.w, c.queue = conn, w, nil
		c.mu.Unlock()
		c.log.Info("oms: connected", "addr", c.addr)
		c.resendAll()

		r := wire.NewReader(conn)
		for {
			m, err := r.Read()
			if err != nil {
				break
			}
			c.onMsg(m)
		}
		c.dropConn(w)
		c.log.Warn("oms: disconnected; will reconnect and resend in-flight orders")
	}
}

// resendLoop re-sends anything unanswered for longer than resendAt: a delayed or swallowed reply
// must not leave an order in limbo.
func (c *Client) resendLoop() {
	t := time.NewTicker(c.resendAt / 2)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
		}
		now := time.Now()
		c.mu.Lock()
		for _, e := range c.inflight {
			if now.Sub(e.lastSent) < c.resendAt {
				continue
			}
			switch {
			case !e.acked:
				c.enqueueLocked(e.newMsg)
				e.lastSent = now
			case e.cancel != nil:
				c.enqueueLocked(*e.cancel)
				e.lastSent = now
			case now.Sub(e.lastSent) >= c.recheck:
				// Acknowledged and still working (e.g. a resting limit). Re-sending the idempotent NEW
				// makes the exchange replay the order's full state, so a fill lost without a
				// disconnect is still recovered within one recheck interval.
				c.enqueueLocked(e.newMsg)
				e.lastSent = now
			}
		}
		c.mu.Unlock()
	}
}
