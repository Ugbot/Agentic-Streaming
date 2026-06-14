package core

// CompensationAction is a recorded undo for a completed saga step.
type CompensationAction struct {
	Name string
	Undo func()
}

// Saga records completed steps and rolls them back in reverse order on failure — the
// portable analogue of the Flink CompensationHandler.
type Saga struct {
	done []CompensationAction
}

// NewSaga builds an empty saga.
func NewSaga() *Saga { return &Saga{} }

// Step runs do(); on success records undo for later rollback. If do() returns an error,
// the saga compensates everything recorded so far and returns that error.
func (s *Saga) Step(name string, do func() error, undo func()) error {
	if err := do(); err != nil {
		s.Compensate()
		return err
	}
	s.done = append(s.done, CompensationAction{Name: name, Undo: undo})
	return nil
}

// Compensate runs all recorded undos in reverse order; returns the names compensated.
func (s *Saga) Compensate() []string {
	var names []string
	for i := len(s.done) - 1; i >= 0; i-- {
		a := s.done[i]
		if a.Undo != nil {
			a.Undo()
		}
		names = append(names, a.Name)
	}
	s.done = nil
	return names
}
