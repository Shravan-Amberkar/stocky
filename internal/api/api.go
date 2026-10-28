// Package api is the strategy HTTP API: create, view and cancel strategies, and view an account.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Shravan-Amberkar/stocky/internal/engine"
	"github.com/Shravan-Amberkar/stocky/internal/rms"
	"github.com/Shravan-Amberkar/stocky/internal/strategy"
)

type API struct {
	Engine *engine.Engine
	Risk   *rms.Gateway
	WS     http.Handler
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /strategies", a.create)
	mux.HandleFunc("GET /strategies", a.list)
	mux.HandleFunc("GET /strategies/{id}", a.get)
	mux.HandleFunc("DELETE /strategies/{id}", a.cancel)
	mux.HandleFunc("GET /accounts/{user}", a.account)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }) //nolint:errcheck
	if a.WS != nil {
		mux.Handle("GET /ws", a.WS)
	}
	return mux
}

func write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func fail(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, strategy.ErrInvalid), errors.Is(err, strategy.ErrNoPrice), errors.Is(err, engine.ErrUnknownSymbol):
		code = http.StatusBadRequest
	case errors.Is(err, engine.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, strategy.ErrNotNow):
		code = http.StatusConflict
	}
	write(w, code, map[string]string{"error": err.Error()})
}

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	var sp strategy.Spec
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&sp); err != nil {
		write(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if _, ok := a.Risk.Account(sp.User); !ok {
		write(w, http.StatusBadRequest, map[string]string{"error": "unknown user"})
		return
	}
	s, err := a.Engine.Create(sp)
	if err != nil {
		fail(w, err)
		return
	}
	write(w, http.StatusCreated, s)
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	write(w, http.StatusOK, a.Engine.List(r.URL.Query().Get("user")))
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	s, ok := a.Engine.Get(r.PathValue("id"))
	if !ok {
		fail(w, engine.ErrNotFound)
		return
	}
	write(w, http.StatusOK, s)
}

func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	s, err := a.Engine.Cancel(r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	write(w, http.StatusAccepted, s)
}

func (a *API) account(w http.ResponseWriter, r *http.Request) {
	v, ok := a.Risk.Account(r.PathValue("user"))
	if !ok {
		fail(w, engine.ErrNotFound)
		return
	}
	write(w, http.StatusOK, v)
}
