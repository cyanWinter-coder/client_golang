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
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// metricCollectFunc builds a simple collector that emits one gauge with the
// provided fully-qualified name and value every time Collect is called.
func metricCollectFunc(name string, value float64) func(chan<- prometheus.Metric) {
	desc := prometheus.NewDesc(name, "help for "+name, nil, nil)
	return func(ch chan<- prometheus.Metric) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value)
	}
}

// funcCollector adapts a plain collect function to the Collector interface.
type funcCollector struct {
	describe func(chan<- *prometheus.Desc)
	collect  func(chan<- prometheus.Metric)
}

func (c *funcCollector) Describe(ch chan<- *prometheus.Desc) { c.describe(ch) }
func (c *funcCollector) Collect(ch chan<- prometheus.Metric) { c.collect(ch) }

func newFuncCollector(name string, collect func(chan<- prometheus.Metric)) *funcCollector {
	desc := prometheus.NewDesc(name, "help for "+name, nil, nil)
	return &funcCollector{
		describe: func(ch chan<- *prometheus.Desc) { ch <- desc },
		collect:  collect,
	}
}

// blockingCollector blocks on a release channel while optionally emitting a
// metric first. It records how many Collect calls are in flight so tests can
// wait until collection has started.
type blockingCollector struct {
	desc      *prometheus.Desc
	release   <-chan struct{}
	started   chan<- struct{}
	inFlight  atomic.Int32
	emitFirst bool
}

func (c *blockingCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *blockingCollector) Collect(ch chan<- prometheus.Metric) {
	c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	if c.emitFirst {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1)
	}
	c.started <- struct{}{}
	<-c.release
}

// contextBlockingCollector is a ContextCollector that respects cancellation,
// records whether it observed the canceled context, and signals when Collect
// has returned.
type contextBlockingCollector struct {
	desc       *prometheus.Desc
	started    chan<- struct{}
	returned   chan<- struct{}
	observered atomic.Bool
}

func (c *contextBlockingCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *contextBlockingCollector) Collect(ch chan<- prometheus.Metric) {
	panic("Collect must not be called on a ContextCollector")
}

func (c *contextBlockingCollector) CollectWithContext(ctx context.Context, ch chan<- prometheus.Metric) {
	c.started <- struct{}{}
	<-ctx.Done()
	c.observered.Store(true)
	c.returned <- struct{}{}
}

// panicCollector panics during collection, optionally after sending a metric.
type panicCollector struct {
	desc       *prometheus.Desc
	emitFirst  bool
	panicValue any
}

func (c *panicCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *panicCollector) Collect(ch chan<- prometheus.Metric) {
	if c.emitFirst {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1)
	}
	panic(c.panicValue)
}

// contextPanicCollector panics inside CollectWithContext.
type contextPanicCollector struct {
	desc       *prometheus.Desc
	panicValue any
}

func (c *contextPanicCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *contextPanicCollector) Collect(ch chan<- prometheus.Metric) {
	panic("Collect must not be called on a ContextCollector")
}

func (c *contextPanicCollector) CollectWithContext(_ context.Context, ch chan<- prometheus.Metric) {
	panic(c.panicValue)
}

// transactionalCollector counts how often its done callback runs and records
// whether collection had finished when done was invoked.
type transactionalCollector struct {
	desc      *prometheus.Desc
	collect   func(chan<- prometheus.Metric)
	doneCount atomic.Int32
}

func (c *transactionalCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *transactionalCollector) Collect(ch chan<- prometheus.Metric) {
	c.collect(ch)
}

func (c *transactionalCollector) CollectTransactional(ch chan<- prometheus.Metric) func() {
	c.collect(ch)
	return func() {
		c.doneCount.Add(1)
	}
}

// metricFamilyNames returns the ordered family names of a gather result.
func metricFamilyNames(mfs []*dto.MetricFamily) []string {
	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	return names
}

func TestGatherWithTimeoutReturnsAllMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	fast1 := newFuncCollector("zeta_metric", metricCollectFunc("zeta_metric", 1))
	fast2 := newFuncCollector("alpha_metric", metricCollectFunc("alpha_metric", 2))
	if err := reg.Register(fast1); err != nil {
		t.Fatalf("register fast1: %v", err)
	}
	if err := reg.Register(fast2); err != nil {
		t.Fatalf("register fast2: %v", err)
	}

	mfs, err := reg.GatherWithTimeout(context.Background(), time.Second, prometheus.GatherConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := metricFamilyNames(mfs), []string{"alpha_metric", "zeta_metric"}; len(got) != len(want) {
		t.Fatalf("got %d families, want %d", len(got), len(want))
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("family order = %v, want %v", got, want)
			}
		}
	}
}

func TestGatherWithTimeoutStableOrderingAcrossRuns(t *testing.T) {
	reg := prometheus.NewRegistry()
	names := []string{"delta_metric", "alpha_metric", "charlie_metric", "bravo_metric"}
	for _, name := range names {
		if err := reg.Register(newFuncCollector(name, metricCollectFunc(name, 1))); err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
	}

	want := []string{"alpha_metric", "bravo_metric", "charlie_metric", "delta_metric"}
	for run := 0; run < 20; run++ {
		mfs, err := reg.GatherWithTimeout(context.Background(), time.Second, prometheus.GatherConfig{})
		if err != nil {
			t.Fatalf("run %d: unexpected error: %v", run, err)
		}
		got := metricFamilyNames(mfs)
		if len(got) != len(want) {
			t.Fatalf("run %d: order = %v, want %v", run, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("run %d: order = %v, want %v", run, got, want)
			}
		}
	}
}

func TestGatherWithTimeoutSlowCollectorDoesNotBlockOthers(t *testing.T) {
	reg := prometheus.NewRegistry()

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	slow := &blockingCollector{
		desc:    prometheus.NewDesc("slow_metric", "slow help", nil, nil),
		release: release,
		started: started,
	}
	fast := newFuncCollector("fast_metric", metricCollectFunc("fast_metric", 7))
	if err := reg.Register(slow); err != nil {
		t.Fatalf("register slow: %v", err)
	}
	if err := reg.Register(fast); err != nil {
		t.Fatalf("register fast: %v", err)
	}

	start := time.Now()
	mfs, err := reg.GatherWithTimeout(context.Background(), 100*time.Millisecond, prometheus.GatherConfig{})
	elapsed := time.Since(start)

	// The gather must return around the timeout, not wait for the slow
	// collector to be released.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("gather took %v, expected around 100ms", elapsed)
	}
	if err == nil {
		t.Fatal("expected a cancellation error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded in error chain, got %v", err)
	}

	// The fast collector's metrics must survive the timeout.
	var foundFast bool
	for _, mf := range mfs {
		if mf.GetName() == "fast_metric" {
			foundFast = true
			if got := mf.Metric[0].GetGauge().GetValue(); got != 7 {
				t.Fatalf("fast metric value = %v, want 7", got)
			}
		}
	}
	if !foundFast {
		t.Fatalf("fast collector metrics were dropped: %v", metricFamilyNames(mfs))
	}

	// Release the slow collector so its goroutine can exit and goleak stays
	// happy.
	close(release)
	<-started
}

func TestGatherWithTimeoutContextCollectorStopsOnCancel(t *testing.T) {
	reg := prometheus.NewRegistry()

	started := make(chan struct{}, 1)
	returned := make(chan struct{}, 1)
	slow := &contextBlockingCollector{
		desc:     prometheus.NewDesc("slow_ctx_metric", "slow help", nil, nil),
		started:  started,
		returned: returned,
	}
	fast := newFuncCollector("fast_ctx_metric", metricCollectFunc("fast_ctx_metric", 3))
	if err := reg.Register(slow); err != nil {
		t.Fatalf("register slow: %v", err)
	}
	if err := reg.Register(fast); err != nil {
		t.Fatalf("register fast: %v", err)
	}

	mfs, err := reg.GatherWithTimeout(context.Background(), 50*time.Millisecond, prometheus.GatherConfig{})
	if err == nil {
		t.Fatal("expected a cancellation error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	// The ContextCollector must observe the shared cancellation and return.
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("context-aware collector did not return after cancellation")
	}
	if !slow.observered.Load() {
		t.Fatal("collector did not observe the canceled context")
	}

	// Fast results are still returned.
	if got := metricFamilyNames(mfs); len(got) != 1 || got[0] != "fast_ctx_metric" {
		t.Fatalf("unexpected families: %v", got)
	}
}

func TestGatherWithTimeoutAllCollectorsBlockWithoutMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	const n = 3
	releases := make([]chan struct{}, n)
	started := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		release := make(chan struct{})
		releases[i] = release
		blocking := &blockingCollector{
			desc:    prometheus.NewDesc(fmt.Sprintf("all_blocked_metric_%d", i), "blocked help", nil, nil),
			release: release,
			started: started,
		}
		if err := reg.Register(blocking); err != nil {
			t.Fatalf("register blocking %d: %v", i, err)
		}
	}

	// Every collector blocks before emitting anything. The gather must still
	// return around the deadline through the cancellation path with no metrics.
	start := time.Now()
	mfs, err := reg.GatherWithTimeout(context.Background(), 50*time.Millisecond, prometheus.GatherConfig{})
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("gather took %v, expected around 50ms", elapsed)
	}
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if len(mfs) != 0 {
		t.Fatalf("expected no metric families, got %v", metricFamilyNames(mfs))
	}

	for _, release := range releases {
		close(release)
	}
	for i := 0; i < n; i++ {
		<-started
	}
}

func TestGatherWithTimeoutParentContextCancelation(t *testing.T) {
	reg := prometheus.NewRegistry()

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	slow := &blockingCollector{
		desc:    prometheus.NewDesc("parent_slow_metric", "slow help", nil, nil),
		release: release,
		started: started,
	}
	if err := reg.Register(slow); err != nil {
		t.Fatalf("register slow: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	_, err := reg.GatherWithTimeout(ctx, time.Minute, prometheus.GatherConfig{})
	if err == nil {
		t.Fatal("expected a cancellation error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled in error chain, got %v", err)
	}
	close(release)
}

func TestGatherWithTimeoutPanicIsRecovered(t *testing.T) {
	t.Run("regular collector", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		panicking := &panicCollector{
			desc:       prometheus.NewDesc("panic_metric", "panic help", nil, nil),
			panicValue: "boom",
		}
		fast := newFuncCollector("surviving_metric", metricCollectFunc("surviving_metric", 9))
		if err := reg.Register(panicking); err != nil {
			t.Fatalf("register panicking: %v", err)
		}
		if err := reg.Register(fast); err != nil {
			t.Fatalf("register fast: %v", err)
		}

		mfs, err := reg.GatherWithTimeout(context.Background(), time.Second, prometheus.GatherConfig{})
		if err == nil {
			t.Fatal("expected an error for the panicking collector, got nil")
		}
		if !strings.Contains(err.Error(), "panic recovered") {
			t.Fatalf("error does not mention recovered panic: %v", err)
		}
		if !strings.Contains(err.Error(), "boom") {
			t.Fatalf("error does not contain panic value: %v", err)
		}
		// The healthy collector's metrics must not be dropped.
		var found bool
		for _, mf := range mfs {
			if mf.GetName() == "surviving_metric" {
				found = true
			}
		}
		if !found {
			t.Fatalf("surviving metric was dropped: %v", metricFamilyNames(mfs))
		}
	})

	t.Run("context collector", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		panicking := &contextPanicCollector{
			desc:       prometheus.NewDesc("panic_ctx_metric", "panic help", nil, nil),
			panicValue: "ctx boom",
		}
		if err := reg.Register(panicking); err != nil {
			t.Fatalf("register panicking: %v", err)
		}

		mfs, err := reg.GatherWithTimeout(context.Background(), time.Second, prometheus.GatherConfig{})
		if err == nil {
			t.Fatal("expected an error for the panicking collector, got nil")
		}
		if !strings.Contains(err.Error(), "ctx boom") {
			t.Fatalf("error does not contain panic value: %v", err)
		}
		_ = mfs
	})
}

func TestGatherWithTimeoutErrorsCarrySourceIdentity(t *testing.T) {
	reg := prometheus.NewRegistry()

	panicking := &panicCollector{
		desc:       prometheus.NewDesc("source_panic_metric", "panic help", nil, nil),
		panicValue: "labeled boom",
	}
	if err := reg.Register(panicking); err != nil {
		t.Fatalf("register panicking: %v", err)
	}

	cfg := prometheus.GatherConfig{
		CollectorNames: map[prometheus.Collector]string{
			panicking: "custom-panic-source",
		},
	}
	_, err := reg.GatherWithTimeout(context.Background(), time.Second, cfg)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "custom-panic-source") {
		t.Fatalf("error does not carry configured source name: %v", err)
	}

	// Without a configured name, the Go type identifies the collector.
	_, err = reg.GatherWithTimeout(context.Background(), time.Second, prometheus.GatherConfig{})
	if err == nil || !strings.Contains(err.Error(), "*prometheus_test.panicCollector") {
		t.Fatalf("error does not carry default type-based source: %v", err)
	}
}

func TestGatherWithTimeoutPartialFailureKeepsSuccessMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	panicking := &panicCollector{
		desc:       prometheus.NewDesc("broken_metric", "broken help", nil, nil),
		emitFirst:  true,
		panicValue: "partial boom",
	}
	healthy1 := newFuncCollector("healthy_one", metricCollectFunc("healthy_one", 1))
	healthy2 := newFuncCollector("healthy_two", metricCollectFunc("healthy_two", 2))
	for _, c := range []prometheus.Collector{panicking, healthy1, healthy2} {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	mfs, err := reg.GatherWithTimeout(context.Background(), time.Second, prometheus.GatherConfig{})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	// The error must identify the failing collector source.
	if !strings.Contains(err.Error(), "panicCollector") || !strings.Contains(err.Error(), "partial boom") {
		t.Fatalf("error does not identify the failing source: %v", err)
	}
	// Every metric sent before the panic, and every healthy collector's
	// metrics, must be retained: a partial failure never discards successes.
	gotNames := metricFamilyNames(mfs)
	wantNames := []string{"broken_metric", "healthy_one", "healthy_two"}
	if len(gotNames) != len(wantNames) {
		t.Fatalf("families = %v, want %v (error was: %v)", gotNames, wantNames, err)
	}
	for i, want := range wantNames {
		if gotNames[i] != want {
			t.Fatalf("families = %v, want %v", gotNames, wantNames)
		}
	}
}

func TestGatherTransactionalDoneExactlyOnce(t *testing.T) {
	t.Run("successful gather", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		tc := &transactionalCollector{
			desc:    prometheus.NewDesc("tx_metric", "tx help", nil, nil),
			collect: metricCollectFunc("tx_metric", 4),
		}
		plain := newFuncCollector("plain_metric", metricCollectFunc("plain_metric", 5))
		if err := reg.Register(tc); err != nil {
			t.Fatalf("register tx: %v", err)
		}
		if err := reg.Register(plain); err != nil {
			t.Fatalf("register plain: %v", err)
		}

		mfs, done, err := reg.GatherTransactionalWithContext(context.Background(), time.Second, prometheus.GatherConfig{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if done == nil {
			t.Fatal("done callback must never be nil")
		}
		if len(mfs) != 2 {
			t.Fatalf("got %d families, want 2", len(mfs))
		}
		// The callback must not run before the caller invokes done.
		if got := tc.doneCount.Load(); got != 0 {
			t.Fatalf("done invoked %d times before callback, want 0", got)
		}
		done()
		done() // Extra calls must not run the underlying callbacks again.
		if !waitForCount(&tc.doneCount, 1, time.Second) {
			t.Fatalf("done callback ran %d times, want exactly 1", tc.doneCount.Load())
		}
	})

	t.Run("timeout before collector finishes", func(t *testing.T) {
		reg := prometheus.NewRegistry()

		release := make(chan struct{})
		started := make(chan struct{}, 1)
		blocking := &blockingCollector{
			desc:    prometheus.NewDesc("blocking_tx_metric", "blocking help", nil, nil),
			release: release,
			started: started,
		}
		tc := &transactionalCollector{
			desc:    prometheus.NewDesc("fast_tx_metric", "tx help", nil, nil),
			collect: metricCollectFunc("fast_tx_metric", 6),
		}
		if err := reg.Register(blocking); err != nil {
			t.Fatalf("register blocking: %v", err)
		}
		if err := reg.Register(tc); err != nil {
			t.Fatalf("register tx: %v", err)
		}

		_, done, err := reg.GatherTransactionalWithContext(context.Background(), 50*time.Millisecond, prometheus.GatherConfig{})
		if err == nil {
			t.Fatal("expected a timeout error, got nil")
		}
		if done == nil {
			t.Fatal("done callback must never be nil, even on timeout")
		}

		// Before the caller invokes done, no completion callback has run.
		if got := tc.doneCount.Load(); got != 0 {
			t.Fatalf("fast tx done count = %d, want 0 before done is invoked", got)
		}

		done()
		done() // Extra calls must not run the underlying callbacks again.

		// The finished transactional collector's callback runs after the
		// completion goroutine observes that collection has ended.
		if !waitForCount(&tc.doneCount, 1, time.Second) {
			t.Fatalf("tx done callback ran %d times, want exactly 1", tc.doneCount.Load())
		}

		// Releasing the blocking collector lets its detached goroutine exit
		// and must not make any transactional callback run twice.
		close(release)
		<-started

		// Give deferred goroutines a chance to run.
		time.Sleep(50 * time.Millisecond)
		if got := tc.doneCount.Load(); got != 1 {
			t.Fatalf("tx done callback ran %d times, want exactly 1", got)
		}
	})
}

// waitForCount polls an atomic counter until it reaches want or the deadline
// elapses. It reports whether the target value was observed.
func waitForCount(count *atomic.Int32, want int32, deadline time.Duration) bool {
	deadlineAt := time.Now().Add(deadline)
	for time.Now().Before(deadlineAt) {
		if count.Load() == want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return count.Load() == want
}

func TestGatherWithTimeoutConcurrentScrapes(t *testing.T) {
	reg := prometheus.NewRegistry()
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("concurrent_metric_%d", i)
		if err := reg.Register(newFuncCollector(name, metricCollectFunc(name, float64(i)))); err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
	}

	const scrapers = 32
	var wg sync.WaitGroup
	errCh := make(chan error, scrapers)
	wg.Add(scrapers)
	for i := 0; i < scrapers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				mfs, err := reg.GatherWithTimeout(context.Background(), time.Second, prometheus.GatherConfig{})
				if err != nil {
					errCh <- err
					return
				}
				if len(mfs) != 10 {
					errCh <- fmt.Errorf("got %d families, want 10", len(mfs))
					return
				}
				// Every scrape must see the same stable ordering.
				for k, mf := range mfs {
					want := fmt.Sprintf("concurrent_metric_%d", k)
					if mf.GetName() != want {
						errCh <- fmt.Errorf("position %d = %s, want %s", k, mf.GetName(), want)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestGatherWithTimeoutEmptyRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	mfs, err := reg.GatherWithTimeout(context.Background(), time.Second, prometheus.GatherConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mfs != nil {
		t.Fatalf("expected nil families, got %v", mfs)
	}

	_, done, err := reg.GatherTransactionalWithContext(context.Background(), time.Second, prometheus.GatherConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if done == nil {
		t.Fatal("done callback must never be nil")
	}
	done()
}

func TestGatherWithTimeoutCustomChannelCapacity(t *testing.T) {
	reg := prometheus.NewRegistry()

	// A collector that emits more metrics than the tiny per-collector buffer
	// used in this test, then returns. Forwarding must still deliver every
	// metric even though the collector blocks on the small channel.
	desc := prometheus.NewDesc("burst_metric", "burst help", []string{"i"}, nil)
	burst := &funcCollector{
		describe: func(ch chan<- *prometheus.Desc) { ch <- desc },
		collect: func(ch chan<- prometheus.Metric) {
			for i := 0; i < 50; i++ {
				ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(i), fmt.Sprint(i))
			}
		},
	}
	if err := reg.Register(burst); err != nil {
		t.Fatalf("register burst: %v", err)
	}

	mfs, err := reg.GatherWithTimeout(context.Background(), time.Second, prometheus.GatherConfig{
		MetricChannelCapacity: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mfs) != 1 {
		t.Fatalf("got %d families, want 1", len(mfs))
	}
	if got := len(mfs[0].Metric); got != 50 {
		t.Fatalf("got %d metrics, want 50", got)
	}
}

func TestGatherWithTimeoutDoesNotChangeRegularGather(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := newFuncCollector("unchanged_metric", metricCollectFunc("unchanged_metric", 1))
	if err := reg.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Regular Gather keeps its signature and no-timeout behavior.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather error: %v", err)
	}
	if got := metricFamilyNames(mfs); len(got) != 1 || got[0] != "unchanged_metric" {
		t.Fatalf("unexpected Gather result: %v", got)
	}
}
