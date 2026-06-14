// Command demo runs the banking router->path->verifier graph on the Go LocalRuntime —
// the Go peer of the other ports' LocalDemo. No engine, no model, no network.
package main

import (
	"fmt"

	"github.com/jagentic/goagentic/core"
)

func main() {
	rt := core.NewBankingRuntime()

	turns := [][2]string{
		{"c1", "what card types do you offer?"},
		{"c2", "what is my balance?"},
		{"c1", "tell me about crypto cash-back"},
		{"c3", "where is the nearest branch?"},
	}

	fmt.Println("=== Agentic-Flink :: Banking RoutedGraph on the Go core (LocalRuntime) ===")
	for _, t := range turns {
		res := rt.Submit(core.NewEvent(t[0], "demo", t[1]))
		fmt.Printf("[%s] path=%s ok=%v reply=%q tools=%v\n", res.ConversationID, res.Path, res.OK, res.Reply, res.ToolCalls)
	}
	fmt.Printf("\nc1 message count = %d (state kept across turns)\n", rt.Store().MessageCount("c1"))
}
