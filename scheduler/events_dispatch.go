package scheduler

import (
	"github.com/phero20/concurrent-resource-scheduler/events"
)

// emit sends a non-blocking lifecycle event to the internal dispatcher.
// If the buffer is full, the event is immediately and silently dropped.
func (s *Scheduler[T, ID]) emit(eventType events.EventType, id ID) {
	if len(s.cfg.Observers) == 0 {
		return
	}

	e := events.Event[ID]{
		Type: eventType,
		ID:   id,
	}

	select {
	case s.eventStream <- e:
		// successfully buffered
	default:
		// Drop Policy: Event buffer is full. Drop event immediately
		// rather than blocking the scheduler's hot path.
	}
}

// dispatchLoop runs in a background goroutine and pushes events to all registered observers.
// Shutdown behavior: When the scheduler shuts down, this loop terminates immediately.
// Any events remaining in the eventStream buffer are intentionally discarded to ensure
// that a slow observer cannot block the scheduler's shutdown process.
func (s *Scheduler[T, ID]) dispatchLoop() {
	for {
		select {
		case e := <-s.eventStream:
			for _, obs := range s.cfg.Observers {
				if obs == nil {
					continue
				}
				obs.OnEvent(e)
			}
		case <-s.stopDispatcher:
			return
		}
	}
}
