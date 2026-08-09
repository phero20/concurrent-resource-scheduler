package scheduler

import "github.com/feroz/concurrent-resource-scheduler/events"

// EmitForTesting allows the external tests/ package to trigger the internal dispatcher
// before the core scheduler operations are fully instrumented in Phase 6.3.
// This will be removed in Phase 6.3.
func (s *Scheduler[T, ID]) EmitForTesting(eventType events.EventType, id ID) {
	s.emit(eventType, id)
}
