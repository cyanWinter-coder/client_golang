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

package prometheus

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/prometheus/procfs"

	dto "github.com/prometheus/client_model/go"
)

// gatherScalar extracts the numeric value from a scalar DTO metric. The
// process collector only emits gauges and counters.
func gatherScalar(metric *dto.Metric) float64 {
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

// isFiniteFloat reports whether value is neither NaN nor infinite.
func isFiniteFloat(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

// testErrorLogger records collection failures for assertions in the
// platform-specific process collector tests.
type testErrorLogger struct {
	lines []string
}

func (l *testErrorLogger) Printf(format string, v ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

// TestProcessCollectorLogsFatalFailure verifies that a failing PID lookup
// aborts collection without emitting bogus metrics and records the reason
// through the error logger, while still honoring ReportErrors independently.
func TestProcessCollectorLogsFatalFailure(t *testing.T) {
	if _, err := procfs.Self(); err != nil {
		t.Skipf("skipping %s, procfs not available: %s", t.Name(), err)
	}

	t.Run("failure logged and no metric emitted", func(t *testing.T) {
		logger := &testErrorLogger{}
		collector := NewProcessCollector(ProcessCollectorOpts{
			PidFn:       func() (int, error) { return 0, fmt.Errorf("pid unavailable") },
			ErrorLogger: logger,
		})

		ch := make(chan Metric, 4)
		collector.Collect(ch)
		close(ch)

		if len(ch) != 0 {
			t.Fatalf("expected no metrics on fatal failure, got %d", len(ch))
		}
		if len(logger.lines) != 1 || !strings.Contains(logger.lines[0], "pid unavailable") {
			t.Fatalf("expected one fatal log line, got %v", logger.lines)
		}
	})

	t.Run("failure logged and invalid metric reported", func(t *testing.T) {
		logger := &testErrorLogger{}
		collector := NewProcessCollector(ProcessCollectorOpts{
			PidFn:        func() (int, error) { return 0, fmt.Errorf("pid unavailable") },
			ErrorLogger:  logger,
			ReportErrors: true,
		})

		ch := make(chan Metric, 4)
		collector.Collect(ch)
		close(ch)

		if len(ch) != 1 {
			t.Fatalf("expected one invalid metric, got %d", len(ch))
		}
		if len(logger.lines) != 1 {
			t.Fatalf("expected exactly one log line, got %v", logger.lines)
		}
	})
}

// TestProcessCollectorPartialFailureIsIsolated confirms that an unreachable
// target process produces only logged/invalid error metrics and never
// fabricated zero or negative samples for the well-known process metrics.
func TestProcessCollectorPartialFailureIsIsolated(t *testing.T) {
	if _, err := procfs.Self(); err != nil {
		t.Skipf("skipping %s, procfs not available: %s", t.Name(), err)
	}

	logger := &testErrorLogger{}
	// PID 2^22 is extremely unlikely to exist inside a container, forcing
	// every /proc lookup to fail without relying on a race.
	collector := NewProcessCollector(ProcessCollectorOpts{
		PidFn:        func() (int, error) { return 1 << 22, nil },
		ErrorLogger:  logger,
		ReportErrors: false,
	})

	ch := make(chan Metric, 16)
	collector.Collect(ch)
	close(ch)

	if len(ch) != 0 {
		t.Fatalf("expected no fabricated metrics for a missing process, got %d", len(ch))
	}
	if len(logger.lines) == 0 {
		t.Fatal("expected the skipped metrics to be logged with reasons")
	}
	for _, line := range logger.lines {
		if !strings.Contains(line, "process collector") {
			t.Fatalf("log line %q does not identify the process collector", line)
		}
	}
}

// TestProcessCollectorEmitsOnlyValidValues runs a real collection against the
// current process and asserts every emitted scalar is finite, with counters
// non-negative. This is the regression guard for negative CPU time and
// non-finite samples after a container restart.
func TestProcessCollectorEmitsOnlyValidValues(t *testing.T) {
	if _, err := procfs.Self(); err != nil {
		t.Skipf("skipping %s, procfs not available: %s", t.Name(), err)
	}

	registry := NewPedanticRegistry()
	registry.MustRegister(NewProcessCollector(ProcessCollectorOpts{
		ErrorLogger: &testErrorLogger{},
	}))

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather failed: %v", err)
	}

	counters := map[string]struct{}{
		"process_cpu_seconds_total":            {},
		"process_network_receive_bytes_total":  {},
		"process_network_transmit_bytes_total": {},
	}

	for _, family := range families {
		for _, metric := range family.GetMetric() {
			value := gatherScalar(metric)
			if !isFiniteFloat(value) {
				t.Errorf("metric %s emitted non-finite value %v", family.GetName(), value)
			}
			if _, isCounter := counters[family.GetName()]; isCounter && value < 0 {
				t.Errorf("counter %s emitted negative value %v", family.GetName(), value)
			}
		}
	}
}
