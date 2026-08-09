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

// NewCollector constructs a Prometheus Collector.
//
// Behavior:
// The telemetry provider is optional. If nil, throughput metrics (adds, acquires)
// will not be exported.
//
// Lifecycle:
// The returned Collector must be registered with a prometheus.Registry.
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

// Describe sends all possible metric descriptions to the Prometheus channel.
//
// Concurrency Guarantees:
// Thread-safe as per the prometheus.Collector contract.
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

// Collect executes the metric gathering and writes to the Prometheus channel.
//
// Concurrency Guarantees:
// Thread-safe. It performs a rapid O(H) stats snapshot and an O(1) atomic telemetry read.
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
