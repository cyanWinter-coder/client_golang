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
	"fmt"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/prometheus/client_golang/prometheus"
)

// recordingLogger captures every formatted log line for assertions.
type recordingLogger struct {
	lines []string
}

func (l *recordingLogger) Printf(format string, v ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

// testCollector is a controllable Collector used to exercise the timeout
// wrapper deterministically.
type testCollector struct {
	desc       *prometheus.Desc
	beforeWait func(chan<- prometheus.Metric)
	wait       time.Duration
	afterWait  func(chan<- prometheus.Metric)
}

func (c *testCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *testCollector) Collect(ch chan<- prometheus.Metric) {
	if c.beforeWait != nil {
		c.beforeWait(ch)
	}
	if c.wait > 0 {
		time.Sleep(c.wait)
	}
	if c.afterWait != nil {
		c.afterWait(ch)
	}
}

func newMetric(name string, value float64) prometheus.Metric {
	return prometheus.MustNewConstMetric(
		prometheus.NewDesc(name, "help text", nil, nil),
		prometheus.GaugeValue,
		value,
	)
}

// TestTimedCollectorFastPath verifies that a collector finishing within the
// timeout has all of its metrics forwarded and the wrapper adds no latency of
// its own beyond normal channel handling.
func TestTimedCollectorFastPath(t *testing.T) {
	t.Parallel()

	inner := &testCollector{
		desc: prometheus.NewDesc("fast_metric", "help text", nil, nil),
		beforeWait: func(ch chan<- prometheus.Metric) {
			ch <- newMetric("fast_metric", 1)
			ch <- newMetric("fast_metric", 2)
		},
	}
	logger := &recordingLogger{}
	wrapped := prometheus.NewTimedCollector(inner, prometheus.TimedCollectorOpts{
		Timeout:     time.Second,
		ErrorLogger: logger,
	})

	ch := make(chan prometheus.Metric, 4)
	wrapped.Collect(ch)
	close(ch)

	if len(ch) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(ch))
	}
	if len(logger.lines) != 0 {
		t.Fatalf("expected no timeout logs, got %v", logger.lines)
	}
}

// TestTimedCollectorTimeoutReturnsPartialResults ensures that when the inner
// collector overruns the timeout, Collect returns on time with the metrics
// emitted before the deadline and records the reason through the logger.
func TestTimedCollectorTimeoutReturnsPartialResults(t *testing.T) {
	t.Parallel()

	inner := &testCollector{
		desc: prometheus.NewDesc("slow_metric", "help text", nil, nil),
		beforeWait: func(ch chan<- prometheus.Metric) {
			ch <- newMetric("slow_metric", 1)
		},
		wait: 200 * time.Millisecond,
		afterWait: func(ch chan<- prometheus.Metric) {
			// This metric is produced after the deadline and must be dropped.
			ch <- newMetric("slow_metric", 2)
		},
	}
	logger := &recordingLogger{}
	wrapped := prometheus.NewTimedCollector(inner, prometheus.TimedCollectorOpts{
		Timeout:     50 * time.Millisecond,
		ErrorLogger: logger,
	})

	ch := make(chan prometheus.Metric, 4)
	start := time.Now()
	wrapped.Collect(ch)
	elapsed := time.Since(start)
	close(ch)

	if elapsed >= 150*time.Millisecond {
		t.Fatalf("Collect took %s, expected to return near the 50ms timeout", elapsed)
	}
	if len(ch) != 1 {
		t.Fatalf("expected only the 1 pre-timeout metric, got %d", len(ch))
	}
	if len(logger.lines) != 1 || !strings.Contains(logger.lines[0], "exceeded timeout") {
		t.Fatalf("expected one timeout log line, got %v", logger.lines)
	}

	// Give the background collector time to finish and be drained so the
	// test does not leave work running past its assertions.
	time.Sleep(250 * time.Millisecond)
}

// TestTimedCollectorZeroTimeoutPassesThrough checks that a non-positive
// timeout disables the wrapper machinery and delegates Collect directly.
func TestTimedCollectorZeroTimeoutPassesThrough(t *testing.T) {
	t.Parallel()

	inner := &testCollector{
		desc:      prometheus.NewDesc("direct_metric", "help text", nil, nil),
		afterWait: func(ch chan<- prometheus.Metric) { ch <- newMetric("direct_metric", 1) },
	}
	wrapped := prometheus.NewTimedCollector(inner, prometheus.TimedCollectorOpts{})

	ch := make(chan prometheus.Metric, 2)
	wrapped.Collect(ch)
	close(ch)

	if len(ch) != 1 {
		t.Fatalf("expected 1 metric from direct delegation, got %d", len(ch))
	}
}

// TestTimedCollectorPanicIsRecovered ensures a panicking inner collector does
// not crash the scrape and that the panic reason is logged.
func TestTimedCollectorPanicIsRecovered(t *testing.T) {
	t.Parallel()

	panicking := panicCollector{}
	logger := &recordingLogger{}
	wrapped := prometheus.NewTimedCollector(panicking, prometheus.TimedCollectorOpts{
		Timeout:     time.Second,
		ErrorLogger: logger,
	})

	ch := make(chan prometheus.Metric, 2)
	wrapped.Collect(ch)
	close(ch)

	if len(logger.lines) != 1 || !strings.Contains(logger.lines[0], "panicked") {
		t.Fatalf("expected one panic log line, got %v", logger.lines)
	}
}

// TestTimedCollectorDescribeDelegated verifies descriptors are forwarded
// unchanged so registration semantics stay compatible.
func TestTimedCollectorDescribeDelegated(t *testing.T) {
	t.Parallel()

	desc := prometheus.NewDesc("described_metric", "help text", nil, nil)
	inner := &testCollector{desc: desc}
	wrapped := prometheus.NewTimedCollector(inner, prometheus.TimedCollectorOpts{Timeout: time.Second})

	ch := make(chan *prometheus.Desc, 2)
	wrapped.Describe(ch)
	close(ch)

	if len(ch) != 1 {
		t.Fatalf("expected 1 descriptor, got %d", len(ch))
	}
}

// TestTimedCollectorDoesNotLeakGoroutines ensures the worker and drain
// goroutines spawned for an over-running collector all terminate once it
// finishes, instead of living on after the timed Collect call returned.
func TestTimedCollectorDoesNotLeakGoroutines(t *testing.T) {
	defer goleak.VerifyNone(t)

	inner := &testCollector{
		desc: prometheus.NewDesc("leak_check_metric", "help text", nil, nil),
		beforeWait: func(ch chan<- prometheus.Metric) {
			ch <- newMetric("leak_check_metric", 1)
		},
		// Longer than the timeout, but short enough to finish and be drained
		// well before the test ends.
		wait: 100 * time.Millisecond,
	}
	wrapped := prometheus.NewTimedCollector(inner, prometheus.TimedCollectorOpts{
		Timeout: 25 * time.Millisecond,
	})

	ch := make(chan prometheus.Metric, 4)
	wrapped.Collect(ch)
	close(ch)

	// Wait for the background collector and its drain goroutine to observe
	// completion. goleak then asserts nothing remains.
	time.Sleep(200 * time.Millisecond)
}

// panicCollector always panics during Collect.
type panicCollector struct{}

func (panicCollector) Describe(chan<- *prometheus.Desc) {}

func (panicCollector) Collect(chan<- prometheus.Metric) {
	panic("boom")
}

// TestTimedCollectorWithRegistry verifies the wrapper behaves like an ordinary
// Collector when registered: Gather succeeds, descriptors match and a slow
// collector only costs the configured timeout instead of the full scrape.
func TestTimedCollectorWithRegistry(t *testing.T) {
	t.Parallel()

	t.Run("fast collector integrates unchanged", func(t *testing.T) {
		t.Parallel()

		inner := &testCollector{
			desc:      prometheus.NewDesc("registered_metric", "help text", nil, nil),
			afterWait: func(ch chan<- prometheus.Metric) { ch <- newMetric("registered_metric", 3) },
		}
		registry := prometheus.NewPedanticRegistry()
		if err := registry.Register(prometheus.NewTimedCollector(inner, prometheus.TimedCollectorOpts{
			Timeout: time.Second,
		})); err != nil {
			t.Fatalf("registration failed: %v", err)
		}

		families, err := registry.Gather()
		if err != nil {
			t.Fatalf("gather failed: %v", err)
		}
		if len(families) != 1 || families[0].GetName() != "registered_metric" {
			t.Fatalf("unexpected gathered families: %v", families)
		}
	})

	t.Run("slow collector bounds gather duration", func(t *testing.T) {
		t.Parallel()

		inner := &testCollector{
			desc: prometheus.NewDesc("bounded_metric", "help text", nil, nil),
			beforeWait: func(ch chan<- prometheus.Metric) {
				ch <- newMetric("bounded_metric", 1)
			},
			wait: 300 * time.Millisecond,
		}
		registry := prometheus.NewRegistry()
		registry.MustRegister(prometheus.NewTimedCollector(inner, prometheus.TimedCollectorOpts{
			Timeout: 50 * time.Millisecond,
		}))

		start := time.Now()
		families, err := registry.Gather()
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("gather failed: %v", err)
		}
		if elapsed >= 200*time.Millisecond {
			t.Fatalf("gather took %s, expected to be bounded by the timeout", elapsed)
		}
		if len(families) != 1 || len(families[0].GetMetric()) != 1 {
			t.Fatalf("expected the single pre-timeout metric, got %v", families)
		}

		// Let the background collector finish so it does not outlive the test.
		time.Sleep(300 * time.Millisecond)
	})
}
