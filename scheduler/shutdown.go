package scheduler

// Shutdown permanently closes the scheduler to new operations.
func (s *Scheduler[T, ID]) Shutdown() {
	s.closed.Store(true)
}
