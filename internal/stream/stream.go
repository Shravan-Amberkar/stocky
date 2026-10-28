// Package stream is the per-user WebSocket push: every strategy and order status change for a
// user is sent to each of that user's open connections.
//
// It sits behind engine.Publisher, so it can be replaced by a dedicated realtime server (for
// example Centrifugo with user-limited channels, history and multi-node fan-out) without touching
// the engine. A client that cannot keep up is disconnected rather than allowed to slow anything.
package stream

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/Shravan-Amberkar/stocky/internal/engine"
)

type Hub struct {
	log  *slog.Logger
	mu   sync.Mutex
	subs map[string]map[*sub]struct{}
}

type sub struct{ q chan []byte }

func NewHub(log *slog.Logger) *Hub { return &Hub{log: log, subs: map[string]map[*sub]struct{}{}} }

func (h *Hub) Publish(user string, ev engine.Event) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs[user] {
		select {
		case s.q <- b:
		default: // too slow: drop the client, never the trading path
			delete(h.subs[user], s)
			close(s.q)
		}
	}
}

// ServeHTTP upgrades GET /ws?user=<id>. Authentication is out of scope for this exercise.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Query().Get("user")
	if user == "" {
		http.Error(w, "user query parameter required", http.StatusBadRequest)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	s := &sub{q: make(chan []byte, 256)}
	h.mu.Lock()
	if h.subs[user] == nil {
		h.subs[user] = map[*sub]struct{}{}
	}
	h.subs[user][s] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		if _, ok := h.subs[user][s]; ok {
			delete(h.subs[user], s)
			close(s.q)
		}
		h.mu.Unlock()
		c.CloseNow()
	}()

	ctx := c.CloseRead(r.Context()) // we only write; this also notices the client going away
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-s.q:
			if !ok {
				c.Close(websocket.StatusPolicyViolation, "client too slow")
				return
			}
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.Write(wctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
