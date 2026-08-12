// Package prometheus provides a Prometheus [prometheus.Collector] that exports
// Concurrent Resource Scheduler metrics to the Prometheus ecosystem.
//
// # Overview
//
// The [Collector] bridges two CRS data sources into Prometheus metrics:
//
//   - Structural metrics ([MetricsProvider]): derived from
//     [scheduler.Scheduler.Stats]. These are O(H) per scrape.
//   - Throughput metrics ([TelemetryProvider]): derived from
//     [metrics.TelemetryObserver.Snapshot]. These are O(1) per scrape.
//
// The telemetry provider is optional; omitting it disables all counter metrics.
//
// # Exported Metrics
//
// Gauge metrics (current values):
//
//	crs_heap_count           — number of Heap Shards
//	crs_resources_total      — total resources (active + inactive)
//	crs_resources_active     — resources currently in active Heap Shards
//	crs_resources_inactive   — resources currently in the Inactive Store
//	crs_heaps_empty          — shards with zero active resources
//	crs_heaps_non_empty      — shards with at least one active resource
//
// Counter metrics (monotonically increasing, requires TelemetryProvider):
//
//	crs_events_add_total     — total Add/BatchAdd operations
//	crs_events_acquire_total — total Acquire/AcquireByAffinity operations
//	crs_events_release_total — total Release operations
//	crs_events_exclude_total — total Exclude operations
//	crs_events_include_total — total Include operations
//	crs_events_remove_total  — total Remove operations
//	crs_events_update_total  — total Update operations
//
// # Usage
//
//	telemetry := metrics.NewTelemetryObserver[string]()
//	cfg := config.Config[*MyResource, string]{
//	    Observers: []events.Observer[string]{telemetry},
//	}
//	sched, _ := scheduler.New(cfg)
//
//	collector := prometheus.NewCollector(sched, telemetry)
//	prometheus.MustRegister(collector)
package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/phero20/concurrent-resource-scheduler/extensions/metrics"
	"github.com/phero20/concurrent-resource-scheduler/stats"
)

// MetricsProvider defines the interface for retrieving core scheduler stats.
//
// Behavior:
// Allows the Collector to snapshot O(H) structural metrics like heap counts.
type MetricsProvider interface {
	Stats() stats.Stats
}

// TelemetryProvider defines the interface for retrieving O(1) throughput metrics.
//
// Behavior:
// Allows the Collector to retrieve lock-free event counters from the TelemetryObserver.
type TelemetryProvider interface {
	Snapshot() metrics.TelemetryStats
}

// Collector is a prometheus.Collector integration for the scheduler.
//
// Behavior:
// It bridges CRS metrics into the Prometheus ecosystem safely.
//
// Concurrency Guarantees:
// Thread-safe. Prometheus scrape calls operate concurrently against the provider's Snapshot.
type Collector struct {
	provider  MetricsProvider
	telemetry TelemetryProvider

	heapCount         *prometheus.Desc
	totalResources    *prometheus.Desc
	activeResources   *prometheus.Desc
	inactiveResources *prometheus.Desc
	emptyHeaps        *prometheus.Desc
	nonEmptyHeaps     *prometheus.Desc

	adds     *prometheus.Desc
	acquires *prometheus.Desc
	releases *prometheus.Desc
	excludes *prometheus.Desc
	includes *prometheus.Desc
	removes  *prometheus.Desc
	updates  *prometheus.Desc
}

// NewCollector constructs a Prometheus Collector that exports CRS metrics.
//
// provider must be non-nil; pass the *[scheduler.Scheduler] directly. telemetry
// is optional: if nil, the seven throughput counter metrics are not exported
// (Describe still sends their descriptors; Collect omits them).
//
// The returned Collector must be registered with a prometheus.Registry or the
// default registry before scraping begins:
//
//	prometheus.MustRegister(collector)
//
// The returned Collector is safe for concurrent use by multiple goroutines.
func NewCollector(provider MetricsProvider, telemetry TelemetryProvider) *Collector {
	return &Collector{
		provider:  provider,
		telemetry: telemetry,
		heapCount: prometheus.NewDesc(
			"crs_heap_count",
			"Total number of heap shards in the scheduler",
			nil, nil,
		),
		totalResources: prometheus.NewDesc(
			"crs_resources_total",
			"Total number of resources registered in the scheduler",
			nil, nil,
		),
		activeResources: prometheus.NewDesc(
			"crs_resources_active",
			"Total number of resources currently active and available",
			nil, nil,
		),
		inactiveResources: prometheus.NewDesc(
			"crs_resources_inactive",
			"Total number of resources currently inactive (acquired or cooling down)",
			nil, nil,
		),
		emptyHeaps: prometheus.NewDesc(
			"crs_heaps_empty",
			"Number of heap shards that are currently empty",
			nil, nil,
		),
		nonEmptyHeaps: prometheus.NewDesc(
			"crs_heaps_non_empty",
			"Number of heap shards that currently contain at least one active resource",
			nil, nil,
		),
		adds: prometheus.NewDesc(
			"crs_events_add_total",
			"Total number of resources added to the scheduler",
			nil, nil,
		),
		acquires: prometheus.NewDesc(
			"crs_events_acquire_total",
			"Total number of successful acquires",
			nil, nil,
		),
		releases: prometheus.NewDesc(
			"crs_events_release_total",
			"Total number of resources released back to the scheduler",
			nil, nil,
		),
		excludes: prometheus.NewDesc(
			"crs_events_exclude_total",
			"Total number of resources manually excluded",
			nil, nil,
		),
		includes: prometheus.NewDesc(
			"crs_events_include_total",
			"Total number of resources manually included",
			nil, nil,
		),
		removes: prometheus.NewDesc(
			"crs_events_remove_total",
			"Total number of resources permanently removed",
			nil, nil,
		),
		updates: prometheus.NewDesc(
			"crs_events_update_total",
			"Total number of resource updates",
			nil, nil,
		),
	}
}

// Describe sends all metric descriptors to the Prometheus channel.
// It satisfies the [prometheus.Collector] interface.
//
// Describe always sends all 13 descriptors regardless of whether a
// TelemetryProvider was supplied. Prometheus requires consistent descriptor
// registration even when the corresponding Collect may not emit values.
//
// # Concurrency
//
// Describe is safe for concurrent use by multiple goroutines, as required
// by the prometheus.Collector contract.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.heapCount
	ch <- c.totalResources
	ch <- c.activeResources
	ch <- c.inactiveResources
	ch <- c.emptyHeaps
	ch <- c.nonEmptyHeaps

	ch <- c.adds
	ch <- c.acquires
	ch <- c.releases
	ch <- c.excludes
	ch <- c.includes
	ch <- c.removes
	ch <- c.updates
}

// Collect gathers current metric values and sends them to the Prometheus channel.
// It satisfies the [prometheus.Collector] interface.
//
// Collect performs an O(H) Stats snapshot for the gauge metrics and, if a
// TelemetryProvider was supplied, an O(1) atomic Snapshot for the counters.
//
// If TelemetryProvider is nil, the seven throughput counter metrics are not
// emitted during this scrape.
//
// # Concurrency
//
// Collect is safe for concurrent use by multiple goroutines, as required
// by the prometheus.Collector contract.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	s := c.provider.Stats()

	ch <- prometheus.MustNewConstMetric(c.heapCount, prometheus.GaugeValue, float64(s.HeapCount))
	ch <- prometheus.MustNewConstMetric(c.totalResources, prometheus.GaugeValue, float64(s.TotalResources))
	ch <- prometheus.MustNewConstMetric(c.activeResources, prometheus.GaugeValue, float64(s.ActiveResources))
	ch <- prometheus.MustNewConstMetric(c.inactiveResources, prometheus.GaugeValue, float64(s.InactiveResources))
	ch <- prometheus.MustNewConstMetric(c.emptyHeaps, prometheus.GaugeValue, float64(s.EmptyHeaps))
	ch <- prometheus.MustNewConstMetric(c.nonEmptyHeaps, prometheus.GaugeValue, float64(s.NonEmptyHeaps))

	if c.telemetry != nil {
		t := c.telemetry.Snapshot()
		ch <- prometheus.MustNewConstMetric(c.adds, prometheus.CounterValue, float64(t.AddCount))
		ch <- prometheus.MustNewConstMetric(c.acquires, prometheus.CounterValue, float64(t.AcquireCount))
		ch <- prometheus.MustNewConstMetric(c.releases, prometheus.CounterValue, float64(t.ReleaseCount))
		ch <- prometheus.MustNewConstMetric(c.excludes, prometheus.CounterValue, float64(t.ExcludeCount))
		ch <- prometheus.MustNewConstMetric(c.includes, prometheus.CounterValue, float64(t.IncludeCount))
		ch <- prometheus.MustNewConstMetric(c.removes, prometheus.CounterValue, float64(t.RemoveCount))
		ch <- prometheus.MustNewConstMetric(c.updates, prometheus.CounterValue, float64(t.UpdateCount))
	}
}
