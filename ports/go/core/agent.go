package core

// AgentContext is the per-conversation handle an agent's brain uses for one turn — it
// decouples agent logic from the engine. Give it a context, it runs a turn.
type AgentContext struct {
	ConversationID string
	UserID         string
	Store          ConversationStore
	State          KeyedStateStore
	Tools          *ToolRegistry
	Retriever      *TwoTierRetriever // may be nil
	ToolCalls      []string
}

// CallTool records and executes a tool call.
func (c *AgentContext) CallTool(name string, params map[string]any) any {
	c.ToolCalls = append(c.ToolCalls, name)
	return c.Tools.Execute(name, params)
}

// Brain produces a reply for a turn. A real brain runs an LLM ReAct loop; tests use a
// deterministic rule brain so the port runs with no model.
type Brain interface {
	Turn(userText string, ctx *AgentContext) string
}

// BrainFunc adapts a function to the Brain interface.
type BrainFunc func(userText string, ctx *AgentContext) string

// Turn implements Brain.
func (f BrainFunc) Turn(userText string, ctx *AgentContext) string { return f(userText, ctx) }

// Agent is a named agent = id + system prompt + a brain. Stateless itself; all state is
// in the ConversationStore/KeyedStateStore reached through the context.
type Agent struct {
	ID           string
	SystemPrompt string
	Brain        Brain
}

// NewAgent builds an Agent.
func NewAgent(id, systemPrompt string, brain Brain) *Agent {
	return &Agent{ID: id, SystemPrompt: systemPrompt, Brain: brain}
}

// Turn runs one turn: record the user message, run the brain, record the reply.
func (a *Agent) Turn(event Event, ctx *AgentContext) TurnResult {
	ctx.Store.AssociateUser(event.ConversationID, event.UserID)
	ctx.Store.Append(event.ConversationID, UserMessage(event.Text))
	reply := a.Brain.Turn(event.Text, ctx)
	ctx.Store.Append(event.ConversationID, AssistantMessage(reply))
	calls := make([]string, len(ctx.ToolCalls))
	copy(calls, ctx.ToolCalls)
	return TurnResult{ConversationID: event.ConversationID, Reply: reply, ToolCalls: calls}
}
