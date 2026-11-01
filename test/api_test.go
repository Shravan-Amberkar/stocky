package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/Shravan-Amberkar/stocky/internal/api"
	"github.com/Shravan-Amberkar/stocky/internal/engine"
	"github.com/Shravan-Amberkar/stocky/internal/stream"
)

// apiServer runs the real HTTP API and WebSocket hub over the in-process system. The hub is fed
// from the engine's events through the recorder, so the test can use both.
func apiServer(t *testing.T) (*system, *httptest.Server) {
	t.Helper()
	s := start(t, map[string]int64{"AAA": 10_000}, map[string]int64{"u1": 10_000_000, "u2": 10_000_000})
	hub := stream.NewHub(logger())
	s.pub.mu.Lock()
	s.pub.fanout = hub
	s.pub.mu.Unlock()
	srv := httptest.NewServer((&api.API{Engine: s.eng, Risk: s.gw, WS: hub}).Handler())
	t.Cleanup(srv.Close)
	s.waitFor("first price", 3*time.Second, func() bool { return s.eng.Prices()["AAA"] > 0 })
	return s, srv
}

func call(t *testing.T, method, url, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var out map[string]any
	json.NewDecoder(r.Body).Decode(&out) //nolint:errcheck
	return r.StatusCode, out
}

func TestAPIStatusCodes(t *testing.T) {
	_, srv := apiServer(t)
	u := srv.URL + "/strategies"
	ok := `{"user":"u1","symbol":"AAA","side":"LONG","entry":"MARKET","trigger":20000,"qty":1,"client_ref":"r1"}`
	cases := []struct {
		name, body string
		want       int
	}{
		{"valid", ok, 201},
		{"retry with same client_ref", ok, 200},
		{"bad json", `{"user":`, 400},
		{"unknown field", `{"user":"u1","sym":"AAA"}`, 400},
		{"unknown user", `{"user":"nobody","symbol":"AAA","side":"LONG","entry":"MARKET","trigger":1,"qty":1}`, 400},
		{"unknown symbol", `{"user":"u1","symbol":"ZZZ","side":"LONG","entry":"MARKET","trigger":1,"qty":1}`, 400},
		{"target on loss side", `{"user":"u1","symbol":"AAA","side":"LONG","entry":"MARKET","trigger":20000,"qty":1,"target":100}`, 400},
	}
	var id string
	for _, c := range cases {
		code, body := call(t, "POST", u, c.body)
		if code != c.want {
			t.Errorf("%s: got %d want %d (%v)", c.name, code, c.want, body)
		}
		if c.name == "valid" {
			id, _ = body["id"].(string)
		}
	}
	if code, _ := call(t, "GET", u+"/"+id, ""); code != 200 {
		t.Errorf("get: %d", code)
	}
	if code, _ := call(t, "GET", u+"/nope", ""); code != 404 {
		t.Errorf("get unknown: %d", code)
	}
	if code, body := call(t, "DELETE", u+"/"+id, ""); code != 202 || body["state"] != "CANCELLED" {
		t.Errorf("cancel: %d %v", code, body)
	}
	if code, _ := call(t, "DELETE", u+"/"+id, ""); code != 409 {
		t.Errorf("cancel twice: %d", code)
	}
	if code, body := call(t, "GET", srv.URL+"/accounts/u1", ""); code != 200 || body["limit"] == nil {
		t.Errorf("account: %d %v", code, body)
	}
}

// Each user's WebSocket stream carries their own strategy and order events, and nobody else's.
func TestWebSocketStreamIsPerUser(t *testing.T) {
	s, srv := apiServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?user="
	c1, _, err := websocket.Dial(ctx, wsURL+"u1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.CloseNow()
	c2, _, err := websocket.Dial(ctx, wsURL+"u2", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.CloseNow()
	time.Sleep(100 * time.Millisecond)

	st, err := s.create(strategySpec("u1"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for !seen["OPEN"] || !seen["FILLED"] {
		_, b, err := c1.Read(ctx)
		if err != nil {
			t.Fatalf("u1 stream: %v (saw %v)", err, seen)
		}
		var ev engine.Event
		json.Unmarshal(b, &ev) //nolint:errcheck
		switch {
		case ev.Strategy != nil && ev.Strategy.ID == st.ID:
			seen[string(ev.Strategy.State)] = true
		case ev.Order != nil && ev.Order.StrategyID == st.ID:
			seen[string(ev.Order.Kind)] = true
		}
	}
	short, done := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer done()
	if _, b, err := c2.Read(short); err == nil {
		t.Fatalf("u2 received u1's event: %s", b)
	}
}
