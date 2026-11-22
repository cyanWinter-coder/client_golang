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

package prometheus_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// staticCollector is a checked Collector that emits a fixed metric set. It is
// used to build deterministic registrations for snapshot tests.
type staticCollector struct {
	desc  *prometheus.Desc
	value float64
	mtype prometheus.ValueType
}

func (c *staticCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *staticCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.desc, c.mtype, c.value)
}

// blockingCollector blocks its Collect method on a release channel and
// signals when Collect has been entered, so a test can pin a Gather generation.
type blockingCollector struct {
	desc    *prometheus.Desc
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *blockingCollector) Collect(ch chan<- prometheus.Metric) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1)
}

func newBlockingCollector(name string) *blockingCollector {
	return &blockingCollector{
		desc:    prometheus.NewDesc(name, "blocking help", nil, nil),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

// TestGatherSnapshotIsolatesConcurrentRegistration verifies requirements 1
// and 2: a Gather generation freezes the descriptor-to-collector mapping, and
// concurrent Register/Unregister calls only affect later Gather calls.
func TestGatherSnapshotIsolatesConcurrentRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	first := &staticCollector{
		desc:  prometheus.NewDesc("snap_metric", "help", nil, nil),
		value: 1,
		mtype: prometheus.GaugeValue,
	}
	if err := reg.Register(first); err != nil {
		t.Fatal("registering first collector failed:", err)
	}

	blocking := newBlockingCollector("blocked_metric")
	if err := reg.Register(blocking); err != nil {
		t.Fatal("registering blocking collector failed:", err)
	}

	type gatherResult struct {
		mfs []*dto.MetricFamily
		err error
	}
	resultCh := make(chan gatherResult, 1)
	go func() {
		mfs, err := reg.Gather()
		resultCh <- gatherResult{mfs: mfs, err: err}
	}()

	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Gather did not enter the blocking collector")
	}

	// While the first Gather is in flight, mutate the registry. The extra
	// Collector has a distinct name so registration succeeds.
	extra := &staticCollector{
		desc:  prometheus.NewDesc("later_metric", "help", nil, nil),
		value: 2,
		mtype: prometheus.GaugeValue,
	}
	if err := reg.Register(extra); err != nil {
		t.Fatal("registering extra collector failed:", err)
	}
	if !reg.Unregister(first) {
		t.Fatal("unregistering the first collector failed")
	}

	close(blocking.release)
	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatal("in-flight Gather failed:", res.err)
		}
		// The frozen generation still sees the unregistered collector and
		// does not see the collector registered after the snapshot.
		names := familyNames(res.mfs)
		if !contains(names, "snap_metric") {
			t.Errorf("in-flight Gather lost the unregistered metric, got %v", names)
		}
		if contains(names, "later_metric") {
			t.Errorf("in-flight Gather observed a collector registered later, got %v", names)
		}
		if !contains(names, "blocked_metric") {
			t.Errorf("in-flight Gather is missing the blocking metric, got %v", names)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight Gather did not finish")
	}

	// A subsequent Gather reflects the new registry generation.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal("follow-up Gather failed:", err)
	}
	names := familyNames(mfs)
	if contains(names, "snap_metric") {
		t.Errorf("follow-up Gather still saw the unregistered metric, got %v", names)
	}
	if !contains(names, "later_metric") {
		t.Errorf("follow-up Gather did not see the new metric, got %v", names)
	}
}

// TestGatherMetricFamilyOrderIsStable verifies requirement 3's ordering
// guarantee: repeated Gathers of the same generation return metric families in
// a stable, lexicographic order, and metrics within a family stay ordered.
func TestGatherMetricFamilyOrderIsStable(t *testing.T) {
	reg := prometheus.NewRegistry()
	for _, name := range []string{"zeta_metric", "alpha_metric", "mid_metric"} {
		desc := prometheus.NewDesc(name, "help", []string{"id"}, nil)
		// Give each collector a labeled metric so the family is non-empty.
		labeled := labeledCollector{
			desc:   desc,
			values: []string{"b", "a", "c"},
		}
		if err := reg.Register(&labeled); err != nil {
			t.Fatal("registering collector failed:", err)
		}
	}

	wantFamilies := []string{"alpha_metric", "mid_metric", "zeta_metric"}
	for range 5 {
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal("Gather failed:", err)
		}
		gotFamilies := familyNames(mfs)
		if fmt.Sprint(gotFamilies) != fmt.Sprint(wantFamilies) {
			t.Fatalf("unstable family order: got %v, want %v", gotFamilies, wantFamilies)
		}
		for _, mf := range mfs {
			gotLabels := make([]string, 0, len(mf.GetMetric()))
			for _, metric := range mf.GetMetric() {
				gotLabels = append(gotLabels, metric.GetLabel()[0].GetValue())
			}
			wantLabels := []string{"a", "b", "c"}
			if fmt.Sprint(gotLabels) != fmt.Sprint(wantLabels) {
				t.Errorf("unstable metric order in %s: got %v, want %v", mf.GetName(), gotLabels, wantLabels)
			}
		}
	}
}

// labeledCollector emits one gauge per provided label value.
type labeledCollector struct {
	desc   *prometheus.Desc
	values []string
}

func (c *labeledCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *labeledCollector) Collect(ch chan<- prometheus.Metric) {
	for _, value := range c.values {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1, value)
	}
}

// TestGatherConflictErrorNamesCollectors verifies requirement 3's conflict
// attribution: when two Collectors disagree about the type of the same metric
// family, the returned error names both Collectors. Unchecked Collectors are
// used so the disagreement surfaces at Gather time rather than registration.
func TestGatherConflictErrorNamesCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	desc := prometheus.NewDesc("shared_metric", "shared help", nil, nil)

	counterCollector := &customCollector{
		collectFunc: func(ch chan<- prometheus.Metric) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, 1)
		},
	}
	gaugeCollector := &customCollector{
		collectFunc: func(ch chan<- prometheus.Metric) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, 1)
		},
	}
	if err := reg.Register(counterCollector); err != nil {
		t.Fatal("registering counter collector failed:", err)
	}
	if err := reg.Register(gaugeCollector); err != nil {
		t.Fatal("registering gauge collector failed:", err)
	}

	_, err := reg.Gather()
	if err == nil {
		t.Fatal("expected a conflict error, got nil")
	}
	conflict, ok := prometheus.AsCollectorConflictError(err)
	if !ok {
		t.Fatalf("expected a CollectorConflictError, got %T: %v", err, err)
	}
	if conflict.Family != "shared_metric" {
		t.Errorf("unexpected family name %q", conflict.Family)
	}
	if conflict.ExistingCollector == nil || conflict.ConflictingCollector == nil {
		t.Fatalf("conflict error did not name both collectors: %+v", conflict)
	}
	if !strings.Contains(err.Error(), "customCollector") {
		t.Errorf("error message does not name the conflicting collector types: %v", err)
	}
	if !errors.Is(err, conflict.Unwrap()) {
		t.Error("errors.Is does not unwrap to the underlying consistency error")
	}
}

// TestGatherConflictWithinSameCollectorKeepsLegacyError verifies backward
// compatibility: a conflict produced by a single Collector keeps the original
// error text and is not wrapped in a CollectorConflictError, since there is no
// second Collector to attribute the conflict to.
func TestGatherConflictWithinSameCollectorKeepsLegacyError(t *testing.T) {
	reg := prometheus.NewRegistry()
	desc := prometheus.NewDesc("self_conflict", "help", nil, nil)
	collector := &customCollector{
		collectFunc: func(ch chan<- prometheus.Metric) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, 1)
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, 1)
		},
	}
	if err := reg.Register(collector); err != nil {
		t.Fatal("registering collector failed:", err)
	}

	_, err := reg.Gather()
	if err == nil {
		t.Fatal("expected a conflict error, got nil")
	}
	if _, ok := prometheus.AsCollectorConflictError(err); ok {
		t.Fatalf("same-collector conflict must not be wrapped, got %v", err)
	}
	if !strings.Contains(err.Error(), "should be a Counter") {
		t.Fatalf("legacy error message changed, got %v", err)
	}
}

// processedSignalMetric signals when the gather loop processes it by closing
// the processed channel inside Write. This gives a test a deterministic way
// to know that a metric has been fully handled before cancellation.
type processedSignalMetric struct {
	desc      *prometheus.Desc
	processed chan struct{}
	once      sync.Once
}

func (m *processedSignalMetric) Desc() *prometheus.Desc { return m.desc }

func (m *processedSignalMetric) Write(out *dto.Metric) error {
	m.once.Do(func() { close(m.processed) })
	out.Gauge = &dto.Gauge{Value: proto.Float64(7)}
	return nil
}

// signalMetricCollector emits a single processedSignalMetric.
type signalMetricCollector struct {
	metric *processedSignalMetric
}

func (c *signalMetricCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.metric.desc
}

func (c *signalMetricCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- c.metric
}

// TestGatherWithContextCancellationKeepsCompletedResults verifies requirement
// 4: canceling the context stops further Collector invocations while retaining
// metrics that were already processed, and returns the context error.
func TestGatherWithContextCancellationKeepsCompletedResults(t *testing.T) {
	reg := prometheus.NewRegistry()

	// The fast collector's metric closes a channel when the gather loop
	// processes it, giving a deterministic "already completed" checkpoint.
	doneMetric := &processedSignalMetric{
		desc:      prometheus.NewDesc("done_metric", "help", nil, nil),
		processed: make(chan struct{}),
	}
	if err := reg.Register(&signalMetricCollector{metric: doneMetric}); err != nil {
		t.Fatal("registering done collector failed:", err)
	}

	// A slow collector that blocks inside Collect until released. The context
	// is canceled while it is blocked, keeping the generation in flight.
	slow := newBlockingCollector("slow_metric")
	if err := reg.Register(slow); err != nil {
		t.Fatal("registering slow collector failed:", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(slow.release) }) }
	t.Cleanup(release)

	type gatherResult struct {
		mfs []*dto.MetricFamily
		err error
	}
	resultCh := make(chan gatherResult, 1)
	go func() {
		mfs, err := reg.GatherWithContext(ctx)
		resultCh <- gatherResult{mfs: mfs, err: err}
	}()

	// Wait until the fast metric has actually been processed, which guarantees
	// it is part of the retained result.
	select {
	case <-doneMetric.processed:
	case <-time.After(5 * time.Second):
		t.Fatal("gather loop never processed the done metric")
	}
	// And confirm the slow collector is holding the generation open.
	select {
	case <-slow.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("gather never entered the slow collector")
	}

	cancel()
	release()

	select {
	case res := <-resultCh:
		if res.err == nil {
			t.Fatal("expected the context error after cancellation, got nil")
		}
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("expected context.Canceled in the error chain, got %v", res.err)
		}
		// The metric processed before cancellation must be retained.
		if !contains(familyNames(res.mfs), "done_metric") {
			t.Errorf("canceled Gather dropped already completed metrics, got %v", familyNames(res.mfs))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled GatherWithContext did not return")
	}
}

// TestGatherWithContextBackgroundEqualsGather verifies requirement 5: with a
// background context, the context-aware path is equivalent to Gather, so the
// stable Registerer and Gatherer behavior remains unchanged.
func TestGatherWithContextBackgroundEqualsGather(t *testing.T) {
	reg := prometheus.NewRegistry()
	collector := &staticCollector{
		desc:  prometheus.NewDesc("equiv_metric", "help", nil, nil),
		value: 3,
		mtype: prometheus.CounterValue,
	}
	if err := reg.Register(collector); err != nil {
		t.Fatal("registering collector failed:", err)
	}

	plain, plainErr := reg.Gather()
	withCtx, ctxErr := reg.GatherWithContext(context.Background())
	if (plainErr == nil) != (ctxErr == nil) {
		t.Fatalf("error mismatch: Gather=%v GatherWithContext=%v", plainErr, ctxErr)
	}
	if len(plain) != len(withCtx) {
		t.Fatalf("family count mismatch: Gather=%d GatherWithContext=%d", len(plain), len(withCtx))
	}
	if plain[0].GetName() != withCtx[0].GetName() {
		t.Fatalf("family name mismatch: Gather=%q GatherWithContext=%q", plain[0].GetName(), withCtx[0].GetName())
	}

	// The registry still satisfies the stable interfaces.
	var _ prometheus.Registerer = reg
	var _ prometheus.Gatherer = reg
}

// TestDefaultRegistryStillSatisfiesStableInterfaces verifies requirement 5 for
// the package-level default registry, which existing users depend on.
func TestDefaultRegistryStillSatisfiesStableInterfaces(t *testing.T) {
	var _ prometheus.Registerer = prometheus.DefaultRegisterer
	var _ prometheus.Gatherer = prometheus.DefaultGatherer
	regFromRegisterer, ok := prometheus.DefaultRegisterer.(*prometheus.Registry)
	if !ok {
		t.Fatal("default registerer is not a *Registry")
	}
	regFromGatherer, ok := prometheus.DefaultGatherer.(*prometheus.Registry)
	if !ok {
		t.Fatal("default gatherer is not a *Registry")
	}
	if regFromRegisterer != regFromGatherer {
		t.Fatal("default registerer and gatherer no longer share a registry")
	}
	if _, err := prometheus.DefaultGatherer.Gather(); err != nil {
		t.Fatal("default registry Gather failed:", err)
	}
}

func familyNames(mfs []*dto.MetricFamily) []string {
	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	return names
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
