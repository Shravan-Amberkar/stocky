// Package oms owns the TCP order session with the exchange.
//
// It lives under rms/internal/, so Go's internal-package rule lets ONLY the rms package import
// it: there is no code path to the exchange that does not go through the RMS, and the compiler
// enforces that.
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
	resendAt time.Duration

	mu       sync.Mutex
	inflight map[string]*entry
	nextSeq  int64
	w        *wire.Writer // nil while disconnected
	conn     net.Conn
	stop     chan struct{}
	stopped  bool
}

func New(addr string, onMsg func(wire.Msg), log *slog.Logger) *Client {
	return &Client{addr: addr, onMsg: onMsg, log: log, resendAt: time.Second,
		inflight: map[string]*entry{}, stop: make(chan struct{})}
}

// Start connects and keeps reconnecting until Close.
func (c *Client) Start() {
	go c.connectLoop()
	go c.resendLoop()
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

// Send records a NEW as in flight and writes it if connected. If not connected, the reconnect
// resend delivers it.
func (c *Client) Send(m wire.Msg) {
	c.mu.Lock()
	c.nextSeq++
	c.inflight[m.ClientID] = &entry{newMsg: m, lastSent: time.Now(), seq: c.nextSeq}
	w := c.w
	c.mu.Unlock()
	if w != nil {
		c.write(w, m)
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
	w := c.w
	c.mu.Unlock()
	if ok && w != nil {
		c.write(w, m)
	}
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

func (c *Client) write(w *wire.Writer, m wire.Msg) {
	if err := w.Write(m); err != nil {
		c.dropConn(w)
	}
}

func (c *Client) dropConn(w *wire.Writer) {
	c.mu.Lock()
	if c.w == w && c.conn != nil {
		c.conn.Close()
		c.w, c.conn = nil, nil
	}
	c.mu.Unlock()
}

// resendAll replays every in-flight order (and pending cancel) in submission order.
func (c *Client) resendAll(w *wire.Writer) {
	type item struct {
		seq int64
		msg []wire.Msg
	}
	c.mu.Lock()
	items := make([]item, 0, len(c.inflight))
	for _, e := range c.inflight {
		it := item{seq: e.seq, msg: []wire.Msg{e.newMsg}}
		if e.cancel != nil {
			it.msg = append(it.msg, *e.cancel)
		}
		items = append(items, it)
		e.lastSent = time.Now()
	}
	c.mu.Unlock()
	sort.Slice(items, func(i, j int) bool { return items[i].seq < items[j].seq })
	for _, it := range items {
		for _, m := range it.msg {
			c.write(w, m)
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
		c.conn, c.w = conn, w
		c.mu.Unlock()
		c.log.Info("oms: connected", "addr", c.addr)
		c.resendAll(w)

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
		var due []wire.Msg
		c.mu.Lock()
		w := c.w
		for _, e := range c.inflight {
			if now.Sub(e.lastSent) < c.resendAt {
				continue
			}
			if !e.acked {
				due = append(due, e.newMsg)
				e.lastSent = now
			} else if e.cancel != nil {
				due = append(due, *e.cancel)
				e.lastSent = now
			}
		}
		c.mu.Unlock()
		if w == nil {
			continue
		}
		for _, m := range due {
			c.write(w, m)
		}
	}
}
