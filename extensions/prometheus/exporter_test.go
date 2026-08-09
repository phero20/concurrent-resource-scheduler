package prometheus_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/phero20/concurrent-resource-scheduler/extensions/metrics"
	prom "github.com/phero20/concurrent-resource-scheduler/extensions/prometheus"
	"github.com/phero20/concurrent-resource-scheduler/stats"
)

type mockMetricsProvider struct{}

func (m *mockMetricsProvider) Stats() stats.Stats {
	return stats.Stats{
		HeapCount:         4,
		TotalResources:    10,
		ActiveResources:   8,
		InactiveResources: 2,
		EmptyHeaps:        1,
		NonEmptyHeaps:     3,
	}
}

type mockTelemetryProvider struct{}

func (m *mockTelemetryProvider) Snapshot() metrics.TelemetryStats {
	return metrics.TelemetryStats{
		AddCount:     100,
		AcquireCount: 50,
		ReleaseCount: 45,
		ExcludeCount: 5,
		IncludeCount: 2,
		RemoveCount:  1,
		UpdateCount:  10,
	}
}

func TestCollector_WithTelemetry(t *testing.T) {
	provider := &mockMetricsProvider{}
	telemetry := &mockTelemetryProvider{}

	collector := prom.NewCollector(provider, telemetry)
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)

	// Collect and format metrics
	expected := `
		# HELP crs_events_acquire_total Total number of successful acquires
		# TYPE crs_events_acquire_total counter
		crs_events_acquire_total 50
		# HELP crs_events_add_total Total number of resources added to the scheduler
		# TYPE crs_events_add_total counter
		crs_events_add_total 100
		# HELP crs_events_exclude_total Total number of resources manually excluded
		# TYPE crs_events_exclude_total counter
		crs_events_exclude_total 5
		# HELP crs_events_include_total Total number of resources manually included
		# TYPE crs_events_include_total counter
		crs_events_include_total 2
		# HELP crs_events_release_total Total number of resources released back to the scheduler
		# TYPE crs_events_release_total counter
		crs_events_release_total 45
		# HELP crs_events_remove_total Total number of resources permanently removed
		# TYPE crs_events_remove_total counter
		crs_events_remove_total 1
		# HELP crs_events_update_total Total number of resource updates
		# TYPE crs_events_update_total counter
		crs_events_update_total 10
		# HELP crs_heap_count Total number of heap shards in the scheduler
		# TYPE crs_heap_count gauge
		crs_heap_count 4
		# HELP crs_heaps_empty Number of heap shards that are currently empty
		# TYPE crs_heaps_empty gauge
		crs_heaps_empty 1
		# HELP crs_heaps_non_empty Number of heap shards that currently contain at least one active resource
		# TYPE crs_heaps_non_empty gauge
		crs_heaps_non_empty 3
		# HELP crs_resources_active Total number of resources currently active and available
		# TYPE crs_resources_active gauge
		crs_resources_active 8
		# HELP crs_resources_inactive Total number of resources currently inactive (acquired or cooling down)
		# TYPE crs_resources_inactive gauge
		crs_resources_inactive 2
		# HELP crs_resources_total Total number of resources registered in the scheduler
		# TYPE crs_resources_total gauge
		crs_resources_total 10
	`

	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected)); err != nil {
		t.Errorf("Unexpected metrics format:\n%v", err)
	}
}

func TestCollector_WithoutTelemetry(t *testing.T) {
	provider := &mockMetricsProvider{}
	collector := prom.NewCollector(provider, nil)
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)

	// Collect and format metrics (should only have the gauge metrics)
	expected := `
		# HELP crs_heap_count Total number of heap shards in the scheduler
		# TYPE crs_heap_count gauge
		crs_heap_count 4
		# HELP crs_heaps_empty Number of heap shards that are currently empty
		# TYPE crs_heaps_empty gauge
		crs_heaps_empty 1
		# HELP crs_heaps_non_empty Number of heap shards that currently contain at least one active resource
		# TYPE crs_heaps_non_empty gauge
		crs_heaps_non_empty 3
		# HELP crs_resources_active Total number of resources currently active and available
		# TYPE crs_resources_active gauge
		crs_resources_active 8
		# HELP crs_resources_inactive Total number of resources currently inactive (acquired or cooling down)
		# TYPE crs_resources_inactive gauge
		crs_resources_inactive 2
		# HELP crs_resources_total Total number of resources registered in the scheduler
		# TYPE crs_resources_total gauge
		crs_resources_total 10
	`

	// Provide metric names to filter since other default prometheus metrics might exist
	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected)); err != nil {
		t.Errorf("Unexpected metrics format:\n%v", err)
	}
}
