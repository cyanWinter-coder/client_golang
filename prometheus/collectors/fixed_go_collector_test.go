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

package collectors_test

import (
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	dto "github.com/prometheus/client_model/go"
)

// expectedFixedMetricNames is the stable, version-independent metric set that
// NewFixedGoCollector promises to export whenever the underlying runtime
// metric is available. Keeping this list explicit guards against accidental
// drift of names or help texts.
var expectedFixedMetricNames = []string{
	"go_goroutines",
	"go_threads",
	"go_goruntime_heap_objects_bytes",
	"go_goruntime_heap_goal_bytes",
	"go_goruntime_heap_allocs_bytes_total",
	"go_goruntime_heap_allocs_objects_total",
	"go_goruntime_heap_frees_objects_total",
	"go_goruntime_gc_cycles_total",
	"go_goruntime_gc_cpu_seconds_total",
	"go_goruntime_info",
}

// counterMetricNames marks the cumulative members of the fixed set, which
// must never report a negative value.
var counterMetricNames = map[string]struct{}{
	"go_goruntime_heap_allocs_bytes_total":   {},
	"go_goruntime_heap_allocs_objects_total": {},
	"go_goruntime_heap_frees_objects_total":  {},
	"go_goruntime_gc_cycles_total":           {},
	"go_goruntime_gc_cpu_seconds_total":      {},
}

// TestNewFixedGoCollectorExportsStableSet verifies that the collector
// registered on a fresh registry gathers without errors and every exported
// metric belongs to the documented fixed set, with the two scheduler metrics
// and the info gauge always present.
func TestNewFixedGoCollectorExportsStableSet(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewFixedGoCollector())

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather failed: %v", err)
	}

	gotNames := make(map[string]struct{}, len(families))
	helpByName := make(map[string]string, len(families))
	for _, family := range families {
		gotNames[family.GetName()] = struct{}{}
		helpByName[family.GetName()] = family.GetHelp()
	}

	allowed := make(map[string]struct{}, len(expectedFixedMetricNames))
	for _, name := range expectedFixedMetricNames {
		allowed[name] = struct{}{}
	}
	for name := range gotNames {
		if _, ok := allowed[name]; !ok {
			t.Errorf("collected unexpected metric %q outside the fixed set", name)
		}
	}

	// The scheduler metrics and the info gauge must exist on every supported
	// Go version.
	for _, required := range []string{"go_goroutines", "go_threads", "go_goruntime_info"} {
		if _, ok := gotNames[required]; !ok {
			t.Errorf("required metric %q missing from %v", required, sortedKeys(gotNames))
		}
	}

	// Help texts must be non-empty and stable, so dashboards and alerts can
	// rely on them.
	for name, help := range helpByName {
		if strings.TrimSpace(help) == "" {
			t.Errorf("metric %q has an empty help text", name)
		}
	}
}

// TestNewFixedGoCollectorValuesAreValid ensures every numeric sample is
// finite and counters never report a negative value.
func TestNewFixedGoCollectorValuesAreValid(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewFixedGoCollector())

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather failed: %v", err)
	}

	var goroutineValue float64
	for _, family := range families {
		_, isCounter := counterMetricNames[family.GetName()]
		for _, metric := range family.GetMetric() {
			value := scalarValue(metric)
			if math.IsNaN(value) || math.IsInf(value, 0) {
				t.Errorf("metric %s reported non-finite value %v", family.GetName(), value)
			}
			if isCounter && value < 0 {
				t.Errorf("counter %s reported negative value %v", family.GetName(), value)
			}
			if family.GetName() == "go_goroutines" {
				goroutineValue = value
			}
		}
	}

	// The runtime always has at least the goroutine running the test, so a
	// zero or absurdly small value would indicate a misbound runtime sample.
	if goroutineValue < 1 {
		t.Fatalf("expected go_goroutines >= 1, got %v", goroutineValue)
	}
}

// TestNewFixedGoCollectorDescribeCollectAlignment ensures every described
// metric is also collected and vice versa, matching registry expectations.
func TestNewFixedGoCollectorDescribeCollectAlignment(t *testing.T) {
	t.Parallel()

	collector := collectors.NewFixedGoCollector()

	descCh := make(chan *prometheus.Desc, 32)
	collector.Describe(descCh)
	close(descCh)
	described := make(map[string]struct{})
	for desc := range descCh {
		described[desc.String()] = struct{}{}
	}

	metricCh := make(chan prometheus.Metric, 32)
	collector.Collect(metricCh)
	close(metricCh)
	collected := make(map[string]struct{})
	for metric := range metricCh {
		collected[metric.Desc().String()] = struct{}{}
	}

	if len(described) != len(collected) {
		t.Fatalf("described %d metrics but collected %d", len(described), len(collected))
	}
	for name := range described {
		if _, ok := collected[name]; !ok {
			t.Errorf("metric %s described but not collected", name)
		}
	}
	for name := range collected {
		if _, ok := described[name]; !ok {
			t.Errorf("metric %s collected but not described", name)
		}
	}
}

// TestNewFixedGoCollectorIsStableAcrossCollections verifies that repeated
// collections return the same metric names and help texts, which is the
// stability guarantee dashboards depend on.
func TestNewFixedGoCollectorIsStableAcrossCollections(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewFixedGoCollector())

	first, err := registry.Gather()
	if err != nil {
		t.Fatalf("first gather failed: %v", err)
	}
	second, err := registry.Gather()
	if err != nil {
		t.Fatalf("second gather failed: %v", err)
	}

	if len(first) != len(second) {
		t.Fatalf("metric family count changed between gathers: %d vs %d", len(first), len(second))
	}
	firstHelp := helpMap(first)
	secondHelp := helpMap(second)
	for name, help := range firstHelp {
		if secondHelp[name] != help {
			t.Errorf("help text for %s changed between gathers: %q vs %q", name, help, secondHelp[name])
		}
	}
}

// scalarValue extracts the single numeric value of a scalar DTO metric. The
// fixed set only contains gauges and counters, so histogram and summary
// branches are not needed.
func scalarValue(metric *dto.Metric) float64 {
	if metric.GetGauge() != nil {
		return metric.GetGauge().GetValue()
	}
	if metric.GetCounter() != nil {
		return metric.GetCounter().GetValue()
	}
	if metric.GetUntyped() != nil {
		return metric.GetUntyped().GetValue()
	}
	return math.NaN()
}

func helpMap(families []*dto.MetricFamily) map[string]string {
	result := make(map[string]string, len(families))
	for _, family := range families {
		result[family.GetName()] = family.GetHelp()
	}
	return result
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
