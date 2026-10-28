// Command watch prints a user's WebSocket event stream: go run ./cmd/watch -user u1
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/coder/websocket"
)

func main() {
	url := flag.String("url", "ws://127.0.0.1:8080/ws", "stream URL")
	user := flag.String("user", "u1", "user id")
	flag.Parse()
	c, _, err := websocket.Dial(context.Background(), *url+"?user="+*user, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer c.CloseNow()
	for {
		_, b, err := c.Read(context.Background())
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		fmt.Println(string(b))
	}
}
