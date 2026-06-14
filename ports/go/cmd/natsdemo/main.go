// Command natsdemo runs the banking graph over a live NATS JetStream server: publish
// turns to a persistent stream, a consumer runs the graph against KV-backed state and
// replies. Needs a JetStream server (e.g. `podman run -p 4222:4222 nats:latest -js`);
// override the URL with AGENTIC_NATS_URL.
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/jagentic/goagentic/engines/natsjs"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := natsjs.New(nil, nil, nil)
	if err := rt.Connect(ctx, os.Getenv("AGENTIC_NATS_URL")); err != nil {
		fmt.Printf("no JetStream server reachable: %v\n", err)
		fmt.Println("start one with: podman run -p 4222:4222 nats:latest -js")
		os.Exit(1)
	}
	defer rt.Close()

	var mu sync.Mutex
	replies := []natsjs.Reply{}
	done := make(chan struct{})
	unsub, err := rt.SubscribeReplies(func(rep natsjs.Reply) {
		mu.Lock()
		replies = append(replies, rep)
		n := len(replies)
		mu.Unlock()
		if n >= 4 {
			select {
			case <-done:
			default:
				close(done)
			}
		}
	})
	if err != nil {
		fmt.Printf("subscribe failed: %v\n", err)
		os.Exit(1)
	}
	defer unsub()

	go func() { _ = rt.Consume(ctx) }()

	turns := [][2]string{
		{"c1", "what card types do you offer?"},
		{"c2", "what is my balance?"},
		{"c1", "tell me about crypto cash-back"},
		{"c3", "where is the nearest branch?"},
	}
	for _, t := range turns {
		if err := rt.PublishTurn(ctx, t[0], t[1], "demo"); err != nil {
			fmt.Printf("publish failed: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("=== Agentic-Flink :: Banking RoutedGraph on NATS JetStream (Go) ===")
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		fmt.Println("timed out waiting for replies")
	}

	mu.Lock()
	sort.Slice(replies, func(i, j int) bool { return replies[i].ConversationID < replies[j].ConversationID })
	out := replies
	mu.Unlock()
	for _, r := range out {
		fmt.Printf("[%s] path=%s ok=%v reply=%q tools=%v\n", r.ConversationID, r.Path, r.OK, r.Reply, r.ToolCalls)
	}
	if n, err := rt.MessageCount(ctx, "c1"); err == nil {
		fmt.Printf("\nc1 persisted message count = %d (state durable in JetStream KV)\n", n)
	}
}
