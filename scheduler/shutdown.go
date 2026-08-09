package scheduler

// Shutdown permanently closes the scheduler and halts all background processing.
// It stops the event dispatcher goroutine to prevent leaks and marks the scheduler
// as closed, causing all subsequent operations to return ErrSchedulerClosed.
//
// Concurrency Guarantees:
// Thread-safe. It uses atomic flags to ensure idempotent execution.
//
// Lifecycle:
// This method MUST be called when the scheduler is no longer needed.
func (s *Scheduler[T, ID]) Shutdown() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.stopDispatcher)
	}
}
