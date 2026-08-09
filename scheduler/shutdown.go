package scheduler

// Shutdown permanently closes the scheduler to new operations.
func (s *Scheduler[T, ID]) Shutdown() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.stopDispatcher)
	}
}
