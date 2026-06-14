// Package core is the engine-agnostic, dependency-free essence of Agentic-Flink in Go
// — the Go peer of the Python `pyagentic` and Java `jagentic-core` packages.
//
// Nothing here knows about any engine. An adapter (NATS JetStream, Temporal, the HTTP
// gateway, …) supplies how a durable thing per conversation is processed in order, then
// calls RoutedGraph.Handle (or Agent.Turn) per inbound Event. See
// ../../../docs/portability/00-essence-and-core-abstractions.md.
package core

// Event is one inbound message. ConversationID is the partition/state key.
type Event struct {
	ConversationID string
	UserID         string
	Text           string
	Metadata       map[string]string
}

// NewEvent builds an Event with no metadata.
func NewEvent(conversationID, userID, text string) Event {
	return Event{ConversationID: conversationID, UserID: userID, Text: text}
}

// ChatMessage is a transcript entry. Role is one of system|user|assistant|tool.
type ChatMessage struct {
	Role       string
	Content    string
	ToolName   string
	ToolCallID string
}

// UserMessage / AssistantMessage / SystemMessage are convenience constructors.
func UserMessage(text string) ChatMessage      { return ChatMessage{Role: "user", Content: text} }
func AssistantMessage(text string) ChatMessage { return ChatMessage{Role: "assistant", Content: text} }
func SystemMessage(text string) ChatMessage    { return ChatMessage{Role: "system", Content: text} }

// ToolMessage records a tool result in the transcript.
func ToolMessage(callID, name, content string) ChatMessage {
	return ChatMessage{Role: "tool", Content: content, ToolName: name, ToolCallID: callID}
}

// TurnResult is the outcome of one turn through an Agent or RoutedGraph.
type TurnResult struct {
	ConversationID string
	Reply          string
	Path           string
	OK             bool
	ToolCalls      []string
}
