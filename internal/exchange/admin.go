package exchange

import (
	"encoding/json"
	"net/http"
)

// AdminHandler exposes runtime toggles:
//
//	GET  /chaos            current toggles
//	PUT  /chaos            replace toggles, e.g. {"drop":0.02,"delay":50000000,"duplicate":0.1,"partial":0.5}
//	POST /price            {"symbol":"INFY","price":160000}  move a price (any distance)
//	POST /drop             close the current order session now
//	GET  /fills, /orders   the exchange's own records
func (e *Exchange) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	j := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(v) //nolint:errcheck
	}
	mux.HandleFunc("GET /chaos", func(w http.ResponseWriter, _ *http.Request) { j(w, e.Chaos()) })
	mux.HandleFunc("PUT /chaos", func(w http.ResponseWriter, r *http.Request) {
		var c Chaos
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		e.SetChaos(c)
		j(w, c)
	})
	mux.HandleFunc("POST /price", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Symbol string `json:"symbol"`
			Price  int64  `json:"price"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		e.SetPrice(in.Symbol, in.Price)
		j(w, map[string]int64{in.Symbol: e.Price(in.Symbol)})
	})
	mux.HandleFunc("POST /drop", func(w http.ResponseWriter, _ *http.Request) { e.DropActive(); j(w, "dropped") })
	mux.HandleFunc("GET /fills", func(w http.ResponseWriter, _ *http.Request) { j(w, e.Fills()) })
	mux.HandleFunc("GET /orders", func(w http.ResponseWriter, _ *http.Request) { j(w, e.Orders()) })
	// Local dev tool: allow the Stocky demo console (another origin) to drive it.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
