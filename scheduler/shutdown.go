package scheduler

// Shutdown permanently closes the scheduler and stops all background
// processing.
//
// After Shutdown returns:
//
//   - All subsequent calls to exported methods (except [Scheduler.Stats])
//     return [errors.ErrSchedulerClosed].
//   - The background event-dispatcher goroutine (if started) is stopped.
//     Events remaining in the internal buffer are discarded.
//   - In-flight operations that began before Shutdown was called complete
//     normally; they are not interrupted.
//
// Shutdown is idempotent: calling it more than once has no additional effect.
//
// # Concurrency
//
// Shutdown is safe for concurrent use by multiple goroutines.
func (s *Scheduler[T, ID]) Shutdown() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.stopDispatcher)
	}
}
