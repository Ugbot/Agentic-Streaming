package core

import "sync"

// Suspension is a turn held awaiting external input (human approval, an async result).
// PendingText is the held turn's text, replayed on resume; Since is when it suspended
// (used for timeout sweeps). The Go peer of jagentic-core's Suspension record.
type Suspension struct {
	ConversationID string
	Reason         string
	PendingText    string
	Since          int64
}

// SuspensionService tracks which conversations are suspended awaiting input. The
// InMemory default is process-local; a durable impl can persist (as the timer service
// does) so a suspended turn survives restart.
type SuspensionService interface {
	// Suspend records a conversation as awaiting approval.
	Suspend(conversationID, reason, pendingText string, now int64)
	// IsSuspended reports whether a conversation is currently suspended.
	IsSuspended(conversationID string) bool
	// Peek returns the pending suspension without clearing it.
	Peek(conversationID string) (Suspension, bool)
	// Clear removes and returns the pending suspension (the resume command).
	Clear(conversationID string) (Suspension, bool)
	// AllPending returns every currently-suspended conversation (for timeout sweeps).
	AllPending() []Suspension
}

// InMemorySuspensionService is the process-local, mutex-guarded default SuspensionService.
type InMemorySuspensionService struct {
	mu    sync.Mutex
	byKey map[string]Suspension
}

// NewInMemorySuspensionService builds an empty InMemorySuspensionService.
func NewInMemorySuspensionService() *InMemorySuspensionService {
	return &InMemorySuspensionService{byKey: map[string]Suspension{}}
}

// Suspend records a conversation as awaiting approval.
func (s *InMemorySuspensionService) Suspend(conversationID, reason, pendingText string, now int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byKey[conversationID] = Suspension{
		ConversationID: conversationID,
		Reason:         reason,
		PendingText:    pendingText,
		Since:          now,
	}
}

// IsSuspended reports whether a conversation is currently suspended.
func (s *InMemorySuspensionService) IsSuspended(conversationID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.byKey[conversationID]
	return ok
}

// Peek returns the pending suspension without clearing it.
func (s *InMemorySuspensionService) Peek(conversationID string) (Suspension, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sus, ok := s.byKey[conversationID]
	return sus, ok
}

// Clear removes and returns the pending suspension.
func (s *InMemorySuspensionService) Clear(conversationID string) (Suspension, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sus, ok := s.byKey[conversationID]
	if ok {
		delete(s.byKey, conversationID)
	}
	return sus, ok
}

// AllPending returns every currently-suspended conversation.
func (s *InMemorySuspensionService) AllPending() []Suspension {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Suspension, 0, len(s.byKey))
	for _, sus := range s.byKey {
		out = append(out, sus)
	}
	return out
}

// NeedsApproval decides whether an event must be held for human approval.
type NeedsApproval func(Event) bool

// HumanGate is a human-in-the-loop gate around a Runtime. A turn that needsApproval
// suspends instead of completing; a later Resume (CQRS: resume is just another command)
// replays the held turn (approved) or denies it. CheckTimeouts escalates suspensions that
// age past the timeout — the portable "approve within N minutes or escalate" pattern,
// composing with the timer service. The Go peer of jagentic-core's HumanGate.
type HumanGate struct {
	runtime       Runtime
	suspensions   SuspensionService
	needsApproval NeedsApproval
	timeoutMillis int64 // 0 = no timeout
}

// NewHumanGate builds a HumanGate with no timeout.
func NewHumanGate(runtime Runtime, s SuspensionService, needsApproval NeedsApproval) *HumanGate {
	return NewHumanGateWithTimeout(runtime, s, needsApproval, 0)
}

// NewHumanGateWithTimeout builds a HumanGate that escalates suspensions older than
// timeoutMillis on CheckTimeouts.
func NewHumanGateWithTimeout(runtime Runtime, s SuspensionService, needsApproval NeedsApproval, timeoutMillis int64) *HumanGate {
	return &HumanGate{
		runtime:       runtime,
		suspensions:   s,
		needsApproval: needsApproval,
		timeoutMillis: timeoutMillis,
	}
}

// Submit runs an event through the gate. Suspends (awaiting approval) when required or
// when a turn is already pending; otherwise it is a normal turn through the Runtime.
func (g *HumanGate) Submit(event Event, now int64) TurnResult {
	cid := event.ConversationID
	if g.suspensions.IsSuspended(cid) {
		return TurnResult{
			ConversationID: cid,
			Reply:          "[awaiting-approval] a turn is already pending approval",
			Path:           "awaiting-approval",
			OK:             false,
		}
	}
	if g.needsApproval(event) {
		g.suspensions.Suspend(cid, "approval required: "+event.Text, event.Text, now)
		return TurnResult{
			ConversationID: cid,
			Reply:          "[awaiting-approval] " + event.Text,
			Path:           "awaiting-approval",
			OK:             false,
		}
	}
	return g.runtime.Submit(event)
}

// Resume a suspended conversation: approved → replay the held turn as a fresh,
// metadata-free system event (so it won't re-trigger the gate); denied → report.
func (g *HumanGate) Resume(conversationID string, approved bool, now int64) TurnResult {
	pending, ok := g.suspensions.Clear(conversationID)
	if !ok {
		return TurnResult{
			ConversationID: conversationID,
			Reply:          "[resume] nothing pending",
			Path:           "resume",
			OK:             false,
		}
	}
	if !approved {
		return TurnResult{
			ConversationID: conversationID,
			Reply:          "[denied] " + pending.Reason,
			Path:           "denied",
			OK:             false,
		}
	}
	return g.runtime.Submit(Event{
		ConversationID: conversationID,
		UserID:         "system",
		Text:           pending.PendingText,
	})
}

// CheckTimeouts escalates (and clears) suspensions older than the timeout; one result per
// escalated conversation. Returns nil when no timeout is configured.
func (g *HumanGate) CheckTimeouts(now int64) []TurnResult {
	if g.timeoutMillis <= 0 {
		return nil
	}
	var out []TurnResult
	for _, s := range g.suspensions.AllPending() {
		if now-s.Since > g.timeoutMillis {
			g.suspensions.Clear(s.ConversationID)
			out = append(out, TurnResult{
				ConversationID: s.ConversationID,
				Reply:          "[escalated] approval timed out: " + s.Reason,
				Path:           "escalated",
				OK:             false,
			})
		}
	}
	return out
}
