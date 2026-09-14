package core

import "sync"

// LocalRuntime binds the agent core to the in-process substrate: shared stores + a
// per-conversation lock so each conversation is processed by a single writer at a time
// (the local stand-in for Flink's keyBy / a Kafka partition / a NATS subject). It is the
// zero-dependency Runtime; the NATS/Temporal adapters provide their own.
type LocalRuntime struct {
	graph     *RoutedGraph
	store     ConversationStore
	state     KeyedStateStore
	tools     *ToolRegistry
	retriever *TwoTierRetriever

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// NewLocalRuntime builds a runtime. Pass nil for store/state to use in-memory defaults.
func NewLocalRuntime(graph *RoutedGraph, store ConversationStore, state KeyedStateStore,
	tools *ToolRegistry, retriever *TwoTierRetriever) *LocalRuntime {
	if store == nil {
		store = NewInMemoryConversationStore(0)
	}
	if state == nil {
		state = NewInMemoryKeyedStateStore()
	}
	if tools == nil {
		tools = NewToolRegistry()
	}
	return &LocalRuntime{
		graph: graph, store: store, state: state, tools: tools, retriever: retriever,
		locks: map[string]*sync.Mutex{},
	}
}

// NewBankingRuntime is a convenience constructor wiring the shared banking essence.
func NewBankingRuntime() *LocalRuntime {
	return NewLocalRuntime(BuildBankingGraph(), nil, nil, DefaultBankingTools(), BankingRetriever())
}

// Store exposes the conversation store (for transcript inspection by gateways).
func (r *LocalRuntime) Store() ConversationStore { return r.store }

func (r *LocalRuntime) lockFor(cid string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	lk := r.locks[cid]
	if lk == nil {
		lk = &sync.Mutex{}
		r.locks[cid] = lk
	}
	return lk
}

// Submit runs one turn for an event, single-writer per conversation.
func (r *LocalRuntime) Submit(event Event) TurnResult {
	lk := r.lockFor(event.ConversationID)
	lk.Lock()
	defer lk.Unlock()
	ctx := &AgentContext{
		ConversationID: event.ConversationID,
		UserID:         event.UserID,
		Store:          r.store,
		State:          r.state,
		Tools:          r.tools,
		Retriever:      r.retriever,
	}
	return r.graph.Handle(event, ctx)
}
