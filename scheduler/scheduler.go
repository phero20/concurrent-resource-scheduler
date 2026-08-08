package scheduler

import (
	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/placement"
)

// Scheduler is the opaque concurrent scheduler.
type Scheduler[T any, ID comparable] struct {
	// Fields will be populated in subsequent phases.
	// For Phase 1, we just define the struct.
}

// New validates the configuration and creates an empty ready scheduler.
func New[T any, ID comparable](cfg config.Config[T, ID]) (*Scheduler[T, ID], error) {
	// 1. Validate applies primitive defaults and validates the normalized configuration.
	validatedCfg, err := config.Validate(cfg)
	if err != nil {
		return nil, err
	}

	// 2. Resolve implementation-specific defaults.
	if validatedCfg.AcquireStrategy == nil {
		validatedCfg.AcquireStrategy = placement.NewRoundRobin()
	}

	// 3. Prepare construction for future runtime initialization.
	// (Not implemented in Phase 1)

	// 4. Return an empty ready scheduler.
	_ = validatedCfg // Prevent unused variable error for Phase 1
	return &Scheduler[T, ID]{}, nil
}
