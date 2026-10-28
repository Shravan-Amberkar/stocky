// Command stocky runs the broker: strategy API, symbol actors, RMS, OMS and the WebSocket stream.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/Shravan-Amberkar/stocky/internal/api"
	"github.com/Shravan-Amberkar/stocky/internal/engine"
	"github.com/Shravan-Amberkar/stocky/internal/rms"
	"github.com/Shravan-Amberkar/stocky/internal/stream"
)

func main() {
	httpAddr := flag.String("http", "127.0.0.1:8080", "API + WebSocket listen address")
	orders := flag.String("exchange", "127.0.0.1:9001", "exchange order port")
	md := flag.String("md", "127.0.0.1:9002", "exchange market data port")
	symbols := flag.String("symbols", "RELIANCE,INFY,TCS,HDFCBANK,SBIN", "tradable symbols")
	users := flag.String("users", "u1=100000000,u2=50000000,u3=20000000", "user=margin in paise")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	limits := map[string]int64{}
	for _, kv := range strings.Split(*users, ",") {
		u, v, ok := strings.Cut(kv, "=")
		n, err := strconv.ParseInt(v, 10, 64)
		if !ok || err != nil || n < 0 {
			log.Error("bad -users entry", "entry", kv)
			os.Exit(2)
		}
		limits[u] = n
	}

	hub := stream.NewHub(log)
	eng := engine.New(strings.Split(*symbols, ","), hub, log)
	gw := rms.New(limits, *orders, eng.Deliver, log)
	eng.Start(gw)
	go engine.Feed(*md, eng, log, nil)

	a := &api.API{Engine: eng, Risk: gw, WS: hub}
	log.Info("stocky up", "http", *httpAddr, "exchange", *orders, "md", *md)
	if err := http.ListenAndServe(*httpAddr, a.Handler()); err != nil {
		log.Error("http", "err", err)
		os.Exit(1)
	}
}
