// Command pipeline builds a pipeline.yaml on the chosen backend and runs a turn.
//
//	go run ./cmd/pipeline ../../examples/pipelines/banking.yaml --text "what is my balance?"
//	go run ./cmd/pipeline ../../examples/pipelines/banking.yaml --backend nats --text "card types?"
package main

import (
	"fmt"
	"os"

	"github.com/jagentic/goagentic/core"
	"github.com/jagentic/goagentic/pipeline"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: pipeline <pipeline.yaml> [--backend x] [--text t] [--conv c] [--user u]")
		os.Exit(2)
	}
	path := os.Args[1]
	backend, text, conv, user := "", "what is my balance?", "c1", "demo"
	for i := 2; i+1 < len(os.Args); i += 2 {
		switch os.Args[i] {
		case "--backend":
			backend = os.Args[i+1]
		case "--text":
			text = os.Args[i+1]
		case "--conv":
			conv = os.Args[i+1]
		case "--user":
			user = os.Args[i+1]
		}
	}
	system, err := pipeline.Load(path, backend)
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	res := system.Submit(core.NewEvent(conv, user, text))
	fmt.Printf("backend=%s path=%s ok=%v\n", system.BackendName, res.Path, res.OK)
	fmt.Printf("reply: %s\n", res.Reply)
	if len(res.ToolCalls) > 0 {
		fmt.Printf("tools: %v\n", res.ToolCalls)
	}
}
