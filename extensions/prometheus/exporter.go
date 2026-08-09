package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/feroz/concurrent-resource-scheduler/extensions/metrics"
	"github.com/feroz/concurrent-resource-scheduler/stats"
)

// MetricsProvider defines the minimal interface required by the Collector
// to snapshot the core scheduler stats.
type MetricsProvider interface {
	Stats() stats.Stats
}

// TelemetryProvider defines the interface to get TelemetryStats
// without coupling the exporter to the generic resource ID type.
type TelemetryProvider interface {
	Snapshot() metrics.TelemetryStats
}

// Collector implements prometheus.Collector for the concurrent resource scheduler.
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

// NewCollector creates a new Prometheus Collector that bridges scheduler stats
// and telemetry throughput counters. The telemetry provider is optional and can be nil.
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

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector.
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

// Collect is called by the Prometheus registry when collecting metrics.
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
