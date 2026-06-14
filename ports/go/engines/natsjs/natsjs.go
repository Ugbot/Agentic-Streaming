// Package natsjs hosts the Agentic-Flink essence on NATS JetStream in Go — the Go peer
// of ports/nats (Python). JetStream's KV store is native durable keyed state (C1); a
// persistent stream + consumer is the ordered, redelivering transport (C3). The turn
// runs the portable Go core in a load -> handle -> save bracket around the KV.
package natsjs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jagentic/goagentic/core"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// Stream and subjects for the turn transport.
	StreamName    = "AGENTIC_TURNS_GO"
	TurnSubject   = "agentic.go.turn."
	ReplySubject  = "agentic.go.reply."
	kvBucket      = "agentic_conversations_go"
	maxTranscript = 200
)

// envelope is the per-conversation state persisted as one KV value.
type envelope struct {
	Messages [][4]string       `json:"messages"` // [role, content, toolName, toolCallID]
	Attrs    map[string]string `json:"attrs"`
	Owner    string            `json:"owner"`
}

// Runtime is a NATS JetStream-backed runtime for the banking essence.
type Runtime struct {
	graph     *core.RoutedGraph
	tools     *core.ToolRegistry
	retriever *core.TwoTierRetriever
	state     core.KeyedStateStore

	nc *nats.Conn
	js jetstream.JetStream
	kv jetstream.KeyValue
}

// New builds a Runtime. Pass nil graph/tools/retriever to use the shared banking
// essence; pass custom ones (e.g. an extended graph) to run them on the NATS seam.
func New(graph *core.RoutedGraph, tools *core.ToolRegistry, retriever *core.TwoTierRetriever) *Runtime {
	if graph == nil {
		graph = core.BuildBankingGraph()
	}
	if tools == nil {
		tools = core.DefaultBankingTools()
	}
	if retriever == nil {
		retriever = core.BankingRetriever()
	}
	return &Runtime{graph: graph, tools: tools, retriever: retriever, state: core.NewInMemoryKeyedStateStore()}
}

// Connect dials NATS, ensures the stream + KV bucket exist, and is idempotent.
func (r *Runtime) Connect(ctx context.Context, url string) error {
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	if err != nil {
		return err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return err
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{TurnSubject + "*"},
	}); err != nil {
		nc.Close()
		return err
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: kvBucket})
	if err != nil {
		nc.Close()
		return err
	}
	r.nc, r.js, r.kv = nc, js, kv
	return nil
}

// Close drains the connection.
func (r *Runtime) Close() {
	if r.nc != nil {
		_ = r.nc.Drain()
	}
}

func kvKey(cid string) string {
	return "conv_" + strings.ReplaceAll(cid, ".", "_")
}

func (r *Runtime) load(ctx context.Context, cid string) (*core.InMemoryConversationStore, uint64, error) {
	store := core.NewInMemoryConversationStore(maxTranscript)
	entry, err := r.kv.Get(ctx, kvKey(cid))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return store, 0, nil
	}
	if err != nil {
		return store, 0, err
	}
	var env envelope
	if err := json.Unmarshal(entry.Value(), &env); err != nil {
		return store, 0, err
	}
	for _, m := range env.Messages {
		store.Append(cid, core.ChatMessage{Role: m[0], Content: m[1], ToolName: m[2], ToolCallID: m[3]})
	}
	for k, v := range env.Attrs {
		store.PutAttribute(cid, k, v)
	}
	if env.Owner != "" {
		store.AssociateUser(cid, env.Owner)
	}
	return store, entry.Revision(), nil
}

func (r *Runtime) save(ctx context.Context, cid string, store core.ConversationStore, owner string, rev uint64) error {
	hist := store.History(cid)
	env := envelope{Attrs: store.Attributes(cid), Owner: owner}
	for _, m := range hist {
		env.Messages = append(env.Messages, [4]string{m.Role, m.Content, m.ToolName, m.ToolCallID})
	}
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if rev == 0 {
		_, err = r.kv.Put(ctx, kvKey(cid), data)
	} else {
		// Compare-and-set on the last revision = optimistic single-writer (C2 backstop).
		_, err = r.kv.Update(ctx, kvKey(cid), data, rev)
	}
	return err
}

// Submit runs one turn: load the KV envelope, run the portable graph, save it back.
func (r *Runtime) Submit(ctx context.Context, event core.Event) (core.TurnResult, error) {
	store, rev, err := r.load(ctx, event.ConversationID)
	if err != nil {
		return core.TurnResult{}, err
	}
	actx := &core.AgentContext{
		ConversationID: event.ConversationID,
		UserID:         event.UserID,
		Store:          store,
		State:          r.state,
		Tools:          r.tools,
		Retriever:      r.retriever,
	}
	res := r.graph.Handle(event, actx)
	if err := r.save(ctx, event.ConversationID, store, event.UserID, rev); err != nil {
		return res, err
	}
	return res, nil
}

// MessageCount reads the durable transcript size for a conversation from the KV store.
func (r *Runtime) MessageCount(ctx context.Context, cid string) (int, error) {
	store, _, err := r.load(ctx, cid)
	if err != nil {
		return 0, err
	}
	return store.MessageCount(cid), nil
}

// turnMsg is the JSON wire form of a turn on the stream.
type turnMsg struct {
	ConversationID string `json:"conversation_id"`
	Text           string `json:"text"`
	UserID         string `json:"user_id"`
}

// PublishTurn publishes a turn onto the persistent stream.
func (r *Runtime) PublishTurn(ctx context.Context, cid, text, userID string) error {
	data, _ := json.Marshal(turnMsg{ConversationID: cid, Text: text, UserID: userID})
	_, err := r.js.Publish(ctx, TurnSubject+kvKey(cid), data)
	return err
}

// Reply is what the worker publishes back per turn.
type Reply struct {
	ConversationID string   `json:"conversation_id"`
	Reply          string   `json:"reply"`
	Path           string   `json:"path"`
	OK             bool     `json:"ok"`
	ToolCalls      []string `json:"tool_calls"`
}

// SubscribeReplies subscribes to the reply subject and invokes handler per reply.
// Returns an unsubscribe func.
func (r *Runtime) SubscribeReplies(handler func(Reply)) (func(), error) {
	sub, err := r.nc.Subscribe(ReplySubject+"*", func(m *nats.Msg) {
		var rep Reply
		if json.Unmarshal(m.Data, &rep) == nil {
			handler(rep)
		}
	})
	if err != nil {
		return func() {}, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}

// Consume runs a durable JetStream consumer that processes turns in publish order, runs
// the graph (load -> handle -> save KV), publishes the reply on ReplySubject+key, and
// acks. It blocks until ctx is cancelled.
func (r *Runtime) Consume(ctx context.Context) error {
	cons, err := r.js.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Durable:   "agentic-go-worker",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return err
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		var t turnMsg
		if err := json.Unmarshal(msg.Data(), &t); err != nil {
			_ = msg.Term()
			return
		}
		res, err := r.Submit(ctx, core.NewEvent(t.ConversationID, t.UserID, t.Text))
		if err != nil {
			_ = msg.Nak()
			return
		}
		calls := res.ToolCalls
		if calls == nil {
			calls = []string{}
		}
		reply, _ := json.Marshal(Reply{
			ConversationID: res.ConversationID, Reply: res.Reply, Path: res.Path, OK: res.OK, ToolCalls: calls,
		})
		_ = r.nc.Publish(ReplySubject+kvKey(t.ConversationID), reply)
		_ = msg.Ack()
	})
	if err != nil {
		return err
	}
	defer cc.Stop()
	<-ctx.Done()
	return nil
}
