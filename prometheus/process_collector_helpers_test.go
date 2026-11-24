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
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// errSentinelForTest is a stable error used to verify reason propagation.
var errSentinelForTest = errors.New("sentinel failure")

// recordingLogger captures every formatted log line for assertions.
type recordingLogger struct {
	lines []string
}

func (l *recordingLogger) Printf(format string, v ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

// newTestProcessCollector builds a processCollector wired with the given
// logger and error reporting mode, enough to exercise the shared value and
// error handling helpers without touching platform-specific data sources.
func newTestProcessCollector(logger ProcessCollectorErrorLogger, reportErrors bool) *processCollector {
	return &processCollector{
		errorLogger:  logger,
		reportErrors: reportErrors,
	}
}

// TestEmitMetricValidation exercises the platform-independent value guards:
// non-finite values and negative counters are skipped with a logged reason,
// while valid samples are emitted unchanged.
func TestEmitMetricValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		valueType   ValueType
		value       float64
		wantEmitted bool
		wantLog     string
	}{
		{
			name:        "valid gauge",
			valueType:   GaugeValue,
			value:       42,
			wantEmitted: true,
		},
		{
			name:        "valid counter",
			valueType:   CounterValue,
			value:       7,
			wantEmitted: true,
		},
		{
			name:        "gauge may be negative",
			valueType:   GaugeValue,
			value:       -1,
			wantEmitted: true,
		},
		{
			name:        "negative counter is rejected",
			valueType:   CounterValue,
			value:       -0.5,
			wantEmitted: false,
			wantLog:     "must not be negative",
		},
		{
			name:        "NaN is rejected",
			valueType:   GaugeValue,
			value:       math.NaN(),
			wantEmitted: false,
			wantLog:     "not finite",
		},
		{
			name:        "positive Inf is rejected",
			valueType:   GaugeValue,
			value:       math.Inf(1),
			wantEmitted: false,
			wantLog:     "not finite",
		},
		{
			name:        "negative Inf is rejected",
			valueType:   GaugeValue,
			value:       math.Inf(-1),
			wantEmitted: false,
			wantLog:     "not finite",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := &recordingLogger{}
			// ReportErrors is disabled so the channel only receives accepted
			// samples; rejection reasons are still observable via the logger.
			collector := newTestProcessCollector(logger, false)
			desc := NewDesc("test_metric", "help text", nil, nil)

			ch := make(chan Metric, 2)
			collector.emitMetric(ch, desc, tc.valueType, tc.value)
			close(ch)

			var emitted int
			for metric := range ch {
				pb := &dto.Metric{}
				if err := metric.Write(pb); err != nil {
					t.Fatalf("unexpected invalid metric for an emitted sample: %v", err)
				}
				emitted++
			}

			if tc.wantEmitted && emitted != 1 {
				t.Fatalf("expected 1 emitted metric, got %d", emitted)
			}
			if !tc.wantEmitted && emitted != 0 {
				t.Fatalf("expected no emitted metric, got %d", emitted)
			}
			if tc.wantLog == "" {
				if len(logger.lines) != 0 {
					t.Fatalf("expected no log lines, got %v", logger.lines)
				}
				return
			}
			if len(logger.lines) != 1 || !strings.Contains(logger.lines[0], tc.wantLog) {
				t.Fatalf("expected one log line containing %q, got %v", tc.wantLog, logger.lines)
			}
		})
	}
}

// TestEmitMetricInvalidMetricReporting verifies that when ReportErrors is
// disabled, skipped metrics neither emit a sample nor require a logger, while
// with ReportErrors enabled an invalid metric carrying the reason is emitted.
func TestEmitMetricInvalidMetricReporting(t *testing.T) {
	t.Parallel()

	desc := NewDesc("test_counter", "help text", nil, nil)

	t.Run("errors ignored without logger", func(t *testing.T) {
		t.Parallel()

		collector := newTestProcessCollector(nil, false)
		ch := make(chan Metric, 2)
		collector.emitMetric(ch, desc, CounterValue, -1)
		close(ch)

		if len(ch) != 0 {
			t.Fatalf("expected no metric when errors are ignored, got %d", len(ch))
		}
	})

	t.Run("invalid metric carries the reason", func(t *testing.T) {
		t.Parallel()

		collector := newTestProcessCollector(nil, true)
		ch := make(chan Metric, 2)
		collector.emitMetric(ch, desc, CounterValue, -1)
		close(ch)

		metric := <-ch
		pb := &dto.Metric{}
		if err := metric.Write(pb); err == nil {
			t.Fatal("expected the invalid metric Write to fail, but it succeeded")
		}
	})
}

// TestReportAndLogError ensures a data source failure is both logged with the
// metric name and surfaced according to the ReportErrors setting.
func TestReportAndLogError(t *testing.T) {
	t.Parallel()

	desc := NewDesc("test_metric", "help text", nil, nil)
	logger := &recordingLogger{}
	collector := newTestProcessCollector(logger, true)

	ch := make(chan Metric, 2)
	collector.reportAndLogError(ch, desc, errSentinelForTest)
	close(ch)

	if len(logger.lines) != 1 {
		t.Fatalf("expected exactly one log line, got %v", logger.lines)
	}
	if !strings.Contains(logger.lines[0], "test_metric") || !strings.Contains(logger.lines[0], "sentinel") {
		t.Fatalf("log line %q should mention the metric and the reason", logger.lines[0])
	}

	metric := <-ch
	pb := &dto.Metric{}
	if err := metric.Write(pb); err == nil {
		t.Fatal("expected the reported error to produce an invalid metric")
	}
}
