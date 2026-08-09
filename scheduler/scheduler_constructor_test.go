package scheduler_test

import (
	"testing"

	"github.com/phero20/concurrent-resource-scheduler/scheduler"
)

func TestNewScheduler(t *testing.T) {
	s, err := scheduler.New(validConfig(2))
	if err != nil {
		t.Fatalf("Expected nil error on New, got %v", err)
	}
	if s == nil {
		t.Fatalf("Expected scheduler instance, got nil")
	}
}
