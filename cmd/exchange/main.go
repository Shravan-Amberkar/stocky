// Command exchange runs the mock exchange: an order port, a market-data port and an admin port
// for switching misbehaviours on and off at runtime.
package main

import (
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Shravan-Amberkar/stocky/internal/exchange"
)

func main() {
	orders := flag.String("orders", "127.0.0.1:9001", "order session listen address")
	md := flag.String("md", "127.0.0.1:9002", "market data listen address")
	admin := flag.String("admin", "127.0.0.1:9003", "admin HTTP listen address")
	tick := flag.Duration("tick", 250*time.Millisecond, "price simulation interval (0 = off; move prices via admin)")
	gapProb := flag.Float64("gap-prob", 0.08, "probability a tick jumps several price levels")
	gapMax := flag.Int("gap-max", 15, "max price levels in one jump")
	drop := flag.Float64("drop", 0, "probability per message of dropping the order connection")
	delay := flag.Duration("delay", 0, "max random delay per outbound message")
	dup := flag.Float64("duplicate", 0, "probability an outbound message is sent twice")
	reorder := flag.Float64("reorder", 0, "probability an outbound message is overtaken by later ones")
	reject := flag.Float64("reject", 0, "probability a new order is rejected")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ex := exchange.New(map[string]int64{ // paise
		"RELIANCE": 290000, "INFY": 160000, "TCS": 410000, "HDFCBANK": 170000, "SBIN": 80000,
	}, log)
	ex.SetChaos(exchange.Chaos{Drop: *drop, Delay: *delay, Duplicate: *dup, Reorder: *reorder, Reject: *reject})

	lo, err := net.Listen("tcp", *orders)
	must(err)
	lm, err := net.Listen("tcp", *md)
	must(err)
	go ex.ServeOrders(lo)     //nolint:errcheck
	go ex.ServeMarketData(lm) //nolint:errcheck
	if *tick > 0 {
		go ex.Simulate(*tick, *gapProb, *gapMax, nil)
	}
	log.Info("mock exchange up", "orders", *orders, "md", *md, "admin", *admin, "chaos", ex.Chaos())
	must(http.ListenAndServe(*admin, ex.AdminHandler()))
}

func must(err error) {
	if err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
