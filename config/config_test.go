package config_test

import (
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/errors"
)

type Resource struct {
	ID string
}

func defaultKeyFunc(r Resource) string { return r.ID }
func defaultComparator(a, b Resource) int { return 0 }

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         config.Config[Resource, string]
		wantErr     error
		verifyAfter func(t *testing.T, c config.Config[Resource, string])
	}{
		{
			name: "valid config with explicit values",
			cfg: config.Config[Resource, string]{
				HeapCount:     5,
				Comparator:    defaultComparator,
				KeyFunc:       defaultKeyFunc,
				AcquirePolicy: config.Exclusive,
			},
			wantErr: nil,
			verifyAfter: func(t *testing.T, c config.Config[Resource, string]) {
				if c.HeapCount != 5 {
					t.Errorf("Expected HeapCount 5, got %d", c.HeapCount)
				}
				if c.AcquirePolicy != config.Exclusive {
					t.Errorf("Expected policy Exclusive, got %v", c.AcquirePolicy)
				}
			},
		},
		{
			name: "default heap count application",
			cfg: config.Config[Resource, string]{
				HeapCount:     0,
				Comparator:    defaultComparator,
				KeyFunc:       defaultKeyFunc,
			},
			wantErr: nil,
			verifyAfter: func(t *testing.T, c config.Config[Resource, string]) {
				if c.HeapCount != config.DefaultHeapCount {
					t.Errorf("Expected HeapCount to default to %d, got %d", config.DefaultHeapCount, c.HeapCount)
				}
				if c.AcquirePolicy != config.Shared {
					t.Errorf("Expected AcquirePolicy to default to Shared, got %v", c.AcquirePolicy)
				}
			},
		},
		{
			name: "negative heap count",
			cfg: config.Config[Resource, string]{
				HeapCount:     -1,
				Comparator:    defaultComparator,
				KeyFunc:       defaultKeyFunc,
			},
			wantErr: errors.ErrInvalidHeapCount,
		},
		{
			name: "exceeds max heap count",
			cfg: config.Config[Resource, string]{
				HeapCount:     1025,
				Comparator:    defaultComparator,
				KeyFunc:       defaultKeyFunc,
			},
			wantErr: errors.ErrInvalidHeapCount,
		},
		{
			name: "nil comparator",
			cfg: config.Config[Resource, string]{
				HeapCount:     1,
				Comparator:    nil,
				KeyFunc:       defaultKeyFunc,
			},
			wantErr: errors.ErrNilComparator,
		},
		{
			name: "nil key func",
			cfg: config.Config[Resource, string]{
				HeapCount:     1,
				Comparator:    defaultComparator,
				KeyFunc:       nil,
			},
			wantErr: errors.ErrNilKeyFunc,
		},
		{
			name: "invalid acquire policy",
			cfg: config.Config[Resource, string]{
				HeapCount:     1,
				Comparator:    defaultComparator,
				KeyFunc:       defaultKeyFunc,
				AcquirePolicy: 255, // Invalid enum value
			},
			wantErr: errors.ErrInvalidAcquirePolicy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalized, err := config.Validate(tt.cfg)
			if err != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && tt.verifyAfter != nil {
				tt.verifyAfter(t, normalized)
			}
		})
	}
}
