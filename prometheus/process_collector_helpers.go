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
	"math"
)

// validProcessMetricValue reports whether value is a finite number. NaN and
// Inf values cannot be represented in the Prometheus text exposition format
// and would break the whole scrape, so metrics with such values are skipped.
func validProcessMetricValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

// emitMetric sends the metric described by desc with the given value to ch,
// unless value is invalid (NaN or Inf) or violates the metric type's
// monotonicity contract. On failure, the metric is skipped, the reason is
// logged when an error logger is configured, and an invalid metric is emitted
// when error reporting is enabled. This guarantees that no anomalous value
// ever reaches the scrape output.
func (c *processCollector) emitMetric(ch chan<- Metric, desc *Desc, valueType ValueType, value float64) {
	if !validProcessMetricValue(value) {
		c.reportAndLogError(ch, desc, errors.New("metric value is not finite"))
		return
	}

	if valueType == CounterValue && value < 0 {
		// Counters must never decrease below zero. A negative value indicates
		// a broken data source, for example a PID reuse race or a clock tick
		// conversion issue, so the sample is dropped instead of poisoning
		// dashboards and triggering bogus counter resets.
		c.reportAndLogError(ch, desc, errors.New("counter value must not be negative"))
		return
	}

	ch <- MustNewConstMetric(desc, valueType, value)
}

// reportAndLogError records err for the metric described by desc. The failure
// is always logged when an error logger is configured, independently of the
// ReportErrors setting, so skipped metrics remain observable. When error
// reporting is enabled, the error is additionally surfaced as an invalid
// metric (see NewInvalidMetric).
func (c *processCollector) reportAndLogError(ch chan<- Metric, desc *Desc, err error) {
	if c.errorLogger != nil {
		c.errorLogger.Printf("process collector: skipping metric %s: %s", desc.fqName, err)
	}
	c.reportError(ch, desc, err)
}

// reportFatalAndLogError records a failure that prevents collection of every
// metric, for example an unreadable PID or a vanished /proc entry. It logs the
// reason when a logger is configured and additionally emits an invalid metric
// only when error reporting is enabled.
func (c *processCollector) reportFatalAndLogError(ch chan<- Metric, err error) {
	if c.errorLogger != nil {
		c.errorLogger.Printf("process collector: collection failed: %s", err)
	}
	c.reportError(ch, nil, err)
}
