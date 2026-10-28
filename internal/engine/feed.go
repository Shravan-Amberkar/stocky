package engine

import (
	"log/slog"
	"net"
	"time"

	"github.com/Shravan-Amberkar/stocky/internal/wire"
)

// Feed subscribes to the exchange market-data stream and hands each tick to the engine,
// reconnecting forever. It never blocks on a symbol: OnTick only enqueues.
func Feed(addr string, e *Engine, log *slog.Logger, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		go func() { <-stop; c.Close() }()
		r := wire.NewReader(c)
		for {
			m, err := r.Read()
			if err != nil {
				break
			}
			if m.Type == wire.Tick {
				e.OnTick(m.Symbol, m.Price)
			}
		}
		c.Close()
		log.Warn("feed: market data disconnected; reconnecting")
	}
}
