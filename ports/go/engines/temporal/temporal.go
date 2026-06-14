// Package temporal hosts the Agentic-Flink essence on Temporal in Go — the Go peer of
// ports/temporal (Java). One entity workflow per conversation (workflowID ==
// conversationID): exactly one running execution (single-writer — C2), event-sourced
// durable state (C1+C3), turns delivered as signals and processed serially on the
// workflow's single coroutine. The deterministic banking graph runs in-workflow; a real
// LLM/tool call would move into an Activity.
package temporal

import (
	"github.com/jagentic/goagentic/core"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is the worker task queue.
const TaskQueue = "agentic-banking-go"

// Signal/query names.
const (
	TurnSignal        = "turn"
	CloseSignal       = "close"
	MessageCountQuery = "messageCount"
	RepliesQuery      = "replies"
)

// TurnRequest is one inbound turn delivered via the "turn" signal.
type TurnRequest struct {
	Text   string `json:"text"`
	UserID string `json:"user_id"`
}

// TurnReply is the recorded outcome of a turn, readable via the "replies" query.
type TurnReply struct {
	Reply        string   `json:"reply"`
	Path         string   `json:"path"`
	OK           bool     `json:"ok"`
	ToolCalls    []string `json:"tool_calls"`
	MessageCount int      `json:"message_count"`
}

// MakeConversationWorkflow returns an entity workflow bound to the given graph/tools/
// retriever — the injectable seam so an extended core graph runs on Temporal unchanged.
func MakeConversationWorkflow(graph *core.RoutedGraph, tools *core.ToolRegistry,
	retriever *core.TwoTierRetriever) func(workflow.Context, string) error {

	return func(ctx workflow.Context, conversationID string) error {
		store := core.NewInMemoryConversationStore(0) // durable via Temporal event history
		state := core.NewInMemoryKeyedStateStore()
		replies := []TurnReply{}
		closed := false

		if err := workflow.SetQueryHandler(ctx, MessageCountQuery, func() (int, error) {
			return store.MessageCount(conversationID), nil
		}); err != nil {
			return err
		}
		if err := workflow.SetQueryHandler(ctx, RepliesQuery, func() ([]TurnReply, error) {
			return replies, nil
		}); err != nil {
			return err
		}

		turnCh := workflow.GetSignalChannel(ctx, TurnSignal)
		closeCh := workflow.GetSignalChannel(ctx, CloseSignal)
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(turnCh, func(c workflow.ReceiveChannel, _ bool) {
			var req TurnRequest
			c.Receive(ctx, &req)
			actx := &core.AgentContext{
				ConversationID: conversationID,
				UserID:         req.UserID,
				Store:          store,
				State:          state,
				Tools:          tools,
				Retriever:      retriever,
			}
			// === The engine seam: the portable router->path->verifier graph ===
			res := graph.Handle(core.NewEvent(conversationID, req.UserID, req.Text), actx)
			calls := res.ToolCalls
			if calls == nil {
				calls = []string{}
			}
			replies = append(replies, TurnReply{
				Reply: res.Reply, Path: res.Path, OK: res.OK, ToolCalls: calls,
				MessageCount: store.MessageCount(conversationID),
			})
		})
		sel.AddReceive(closeCh, func(c workflow.ReceiveChannel, _ bool) {
			var s string
			c.Receive(ctx, &s)
			closed = true
		})

		for !closed {
			sel.Select(ctx)
		}
		return nil
	}
}

// BankingConversationWorkflow is the default entity workflow over the shared banking
// essence — registered by name on the worker.
func BankingConversationWorkflow(ctx workflow.Context, conversationID string) error {
	return MakeConversationWorkflow(
		core.BuildBankingGraph(), core.DefaultBankingTools(), core.BankingRetriever())(ctx, conversationID)
}
