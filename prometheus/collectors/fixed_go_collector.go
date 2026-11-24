// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collectors

import (
	"math"
	"runtime"
	"runtime/metrics"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// fixedRuntimeMetricName is the runtime/metrics name backing one entry of the
// stable metric set exported by FixedGoCollector.
type fixedRuntimeMetricName string

const (
	// runtimeGoroutines backs go_goroutines. It is the authoritative live
	// goroutine count maintained by the scheduler and replaces the
	// runtime.NumGoroutine snapshot used by the legacy collector.
	runtimeGoroutines fixedRuntimeMetricName = "/sched/goroutines:goroutines"
	// runtimeThreads backs go_threads.
	runtimeThreads fixedRuntimeMetricName = "/sched/threads/total:threads"
	// runtimeHeapObjectsBytes backs go_goruntime_heap_objects_bytes.
	runtimeHeapObjectsBytes fixedRuntimeMetricName = "/memory/classes/heap/objects:bytes"
	// runtimeHeapGoalBytes backs go_goruntime_heap_goal_bytes.
	runtimeHeapGoalBytes fixedRuntimeMetricName = "/gc/heap/goal:bytes"
	// runtimeHeapAllocsBytes backs go_goruntime_heap_allocs_bytes_total.
	runtimeHeapAllocsBytes fixedRuntimeMetricName = "/gc/heap/allocs:bytes"
	// runtimeHeapAllocsObjects backs go_goruntime_heap_allocs_objects_total.
	runtimeHeapAllocsObjects fixedRuntimeMetricName = "/gc/heap/allocs:objects"
	// runtimeHeapFreesObjects backs go_goruntime_heap_frees_objects_total.
	runtimeHeapFreesObjects fixedRuntimeMetricName = "/gc/heap/frees:objects"
	// runtimeGCCyclesTotal backs go_goruntime_gc_cycles_total.
	runtimeGCCyclesTotal fixedRuntimeMetricName = "/gc/cycles/total:gc-cycles"
	// runtimeGCCPUSeconds backs go_goruntime_gc_cpu_seconds_total.
	runtimeGCCPUSeconds fixedRuntimeMetricName = "/cpu/classes/gc/total:cpu-seconds"
)

// fixedRuntimeMetric binds a runtime/metrics sample to its stable Prometheus
// descriptor. The Prometheus name and help string are hard-coded, so they can
// never drift across Go versions even if the runtime renames its underlying
// metric.
type fixedRuntimeMetric struct {
	sampleName fixedRuntimeMetricName
	desc       *prometheus.Desc
	valueType  prometheus.ValueType
}

// fixedRuntimeMetricSet is the complete and immutable set exported by
// NewFixedGoCollector. Every entry has a stable name, help text and value
// type. Metrics missing from the running Go version are detected at
// construction time and skipped during collection rather than emitted with
// bogus values.
var fixedRuntimeMetricSet = []fixedRuntimeMetric{
	{
		sampleName: runtimeGoroutines,
		desc: prometheus.NewDesc(
			"go_goroutines",
			"Number of goroutines that currently exist.",
			nil, nil,
		),
		valueType: prometheus.GaugeValue,
	},
	{
		sampleName: runtimeThreads,
		desc: prometheus.NewDesc(
			"go_threads",
			"Number of OS threads created.",
			nil, nil,
		),
		valueType: prometheus.GaugeValue,
	},
	{
		sampleName: runtimeHeapObjectsBytes,
		desc: prometheus.NewDesc(
			"go_goruntime_heap_objects_bytes",
			"Memory occupied by live heap objects in bytes.",
			nil, nil,
		),
		valueType: prometheus.GaugeValue,
	},
	{
		sampleName: runtimeHeapGoalBytes,
		desc: prometheus.NewDesc(
			"go_goruntime_heap_goal_bytes",
			"Heap size target for the end of the next garbage collection cycle in bytes.",
			nil, nil,
		),
		valueType: prometheus.GaugeValue,
	},
	{
		sampleName: runtimeHeapAllocsBytes,
		desc: prometheus.NewDesc(
			"go_goruntime_heap_allocs_bytes_total",
			"Total number of bytes allocated to the heap by the application.",
			nil, nil,
		),
		valueType: prometheus.CounterValue,
	},
	{
		sampleName: runtimeHeapAllocsObjects,
		desc: prometheus.NewDesc(
			"go_goruntime_heap_allocs_objects_total",
			"Total number of heap object allocations triggered by the application.",
			nil, nil,
		),
		valueType: prometheus.CounterValue,
	},
	{
		sampleName: runtimeHeapFreesObjects,
		desc: prometheus.NewDesc(
			"go_goruntime_heap_frees_objects_total",
			"Total number of heap allocations whose storage was freed by the garbage collector.",
			nil, nil,
		),
		valueType: prometheus.CounterValue,
	},
	{
		sampleName: runtimeGCCyclesTotal,
		desc: prometheus.NewDesc(
			"go_goruntime_gc_cycles_total",
			"Total number of completed garbage collection cycles.",
			nil, nil,
		),
		valueType: prometheus.CounterValue,
	},
	{
		sampleName: runtimeGCCPUSeconds,
		desc: prometheus.NewDesc(
			"go_goruntime_gc_cpu_seconds_total",
			"Total estimated CPU time spent on garbage collection tasks in seconds.",
			nil, nil,
		),
		valueType: prometheus.CounterValue,
	},
}

// fixedGoCollector reads an immutable, explicitly enumerated set of
// runtime/metrics samples and exposes them under stable Prometheus names.
// Unlike NewGoCollector, the exported metric set never depends on regexp
// rules, Go-version-specific discovery or mutable runtime counters, which
// keeps names, help texts and value types predictable for dashboards and
// alerts.
type fixedGoCollector struct {
	// mu serializes collections because the sample buffer is shared state.
	mu sync.Mutex

	// samples holds one runtime/metrics sample per supported entry of
	// fixedRuntimeMetricSet. Unsupported entries are absent, so collection
	// never reads a zero-valued KindBad sample.
	samples []metrics.Sample
	// metricBySample maps a runtime/metrics sample name back to its stable
	// descriptor and value type.
	metricBySample map[fixedRuntimeMetricName]fixedRuntimeMetric

	// goInfoDesc exposes the runtime version under a stable name and help
	// text.
	goInfoDesc *prometheus.Desc
}

// NewFixedGoCollector returns a collector that exports a fixed set of Go
// runtime metrics with stable names, help texts and value types. The set is
// resolved once against the runtime/metrics catalog of the running Go
// version; metrics that a future Go version removes are silently omitted
// instead of producing invalid samples. The collector is safe for concurrent
// use and can be registered alongside NewGoCollector because the
// go_goruntime_* metric names do not collide with the legacy metric names.
func NewFixedGoCollector() prometheus.Collector {
	available := make(map[fixedRuntimeMetricName]struct{}, len(fixedRuntimeMetricSet))
	for _, d := range metrics.All() {
		available[fixedRuntimeMetricName(d.Name)] = struct{}{}
	}

	c := &fixedGoCollector{
		metricBySample: make(map[fixedRuntimeMetricName]fixedRuntimeMetric, len(fixedRuntimeMetricSet)),
		goInfoDesc: prometheus.NewDesc(
			"go_goruntime_info",
			"Information about the Go runtime environment.",
			nil, prometheus.Labels{"version": runtime.Version()},
		),
	}

	for _, m := range fixedRuntimeMetricSet {
		if _, ok := available[m.sampleName]; !ok {
			// The running Go version does not provide this metric. Skip it
			// permanently rather than emitting a fabricated zero value.
			continue
		}
		c.samples = append(c.samples, metrics.Sample{Name: string(m.sampleName)})
		c.metricBySample[m.sampleName] = m
	}

	return c
}

// Describe emits the descriptor of every metric that the running Go version
// can provide, plus the runtime info metric. The list is fixed at
// construction time and therefore stable for the collector's lifetime.
func (c *fixedGoCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, sample := range c.samples {
		ch <- c.metricBySample[fixedRuntimeMetricName(sample.Name)].desc
	}
	ch <- c.goInfoDesc
}

// Collect reads all supported runtime/metrics samples in a single
// runtime/metrics.Read call and emits them under their stable names. A
// sample with an unsupported kind or a non-finite value is skipped, so a
// single bad sample can never invalidate the whole scrape.
func (c *fixedGoCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.samples) > 0 {
		// One batch read keeps the collection cheap, a single runtime call,
		// and yields a mutually consistent snapshot.
		metrics.Read(c.samples)
	}

	for i := range c.samples {
		sample := &c.samples[i]
		m := c.metricBySample[fixedRuntimeMetricName(sample.Name)]

		value, ok := scalarRuntimeMetricValue(sample.Value)
		if !ok {
			// Histograms and KindBad values are not part of the fixed scalar
			// set; skip them instead of panicking or emitting garbage.
			continue
		}
		if m.valueType == prometheus.CounterValue && value < 0 {
			// A cumulative metric can never go backwards.
			continue
		}
		ch <- prometheus.MustNewConstMetric(m.desc, m.valueType, value)
	}

	ch <- prometheus.MustNewConstMetric(c.goInfoDesc, prometheus.GaugeValue, 1)
}

// scalarRuntimeMetricValue extracts a finite float64 from a scalar
// runtime/metrics value. It returns false for histogram values, KindBad
// samples and non-finite numbers, which must never be exported.
func scalarRuntimeMetricValue(value metrics.Value) (float64, bool) {
	var result float64
	switch value.Kind() {
	case metrics.KindUint64:
		result = float64(value.Uint64())
	case metrics.KindFloat64:
		result = value.Float64()
	default:
		return 0, false
	}
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return 0, false
	}
	return result, true
}
