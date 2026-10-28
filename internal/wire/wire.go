// Package wire is the line protocol between Stocky and the exchange: one JSON object per line
// over TCP. Prices and values are integer paise, never floats.
package wire

import (
	"bufio"
	"encoding/json"
	"io"
	"sync"
)

type Type string

const (
	// Stocky -> exchange
	New    Type = "NEW"    // idempotent by ClientID: a repeat returns the order's current state
	Cancel Type = "CANCEL" // idempotent by ClientID

	// exchange -> Stocky
	Ack          Type = "ACK"
	Fill         Type = "FILL"
	Reject       Type = "REJECT"
	Cancelled    Type = "CANCELLED"
	CancelReject Type = "CANCEL_REJECT"
	Tick         Type = "TICK" // market-data connection only
)

type Side string

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"
)

type OrdType string

const (
	Market OrdType = "MARKET"
	Limit  OrdType = "LIMIT"
)

type Msg struct {
	Type     Type    `json:"type"`
	ClientID string  `json:"client_id,omitempty"`
	User     string  `json:"user,omitempty"`
	Symbol   string  `json:"symbol,omitempty"`
	Side     Side    `json:"side,omitempty"`
	OrdType  OrdType `json:"ord_type,omitempty"`
	Qty      int64   `json:"qty,omitempty"`
	Price    int64   `json:"price,omitempty"` // limit price on NEW, fill price on FILL, last price on TICK
	ExecID   string  `json:"exec_id,omitempty"`
	Reason   string  `json:"reason,omitempty"`
}

// Writer serialises concurrent writers onto one connection.
type Writer struct {
	mu sync.Mutex
	w  io.Writer
}

func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

func (w *Writer) Write(m Msg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err = w.w.Write(append(b, '\n'))
	return err
}

// Reader decodes one message per line.
type Reader struct{ s *bufio.Scanner }

func NewReader(r io.Reader) *Reader {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64<<10), 64<<10)
	return &Reader{s}
}

func (r *Reader) Read() (Msg, error) {
	for r.s.Scan() {
		var m Msg
		if err := json.Unmarshal(r.s.Bytes(), &m); err != nil {
			continue // a garbled line is skipped, not fatal
		}
		return m, nil
	}
	if err := r.s.Err(); err != nil {
		return Msg{}, err
	}
	return Msg{}, io.EOF
}
