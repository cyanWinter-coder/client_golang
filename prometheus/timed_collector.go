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
	"time"
)

// TimedCollectorOpts configures the behavior of a collector created with
// NewTimedCollector.
type TimedCollectorOpts struct {
	// Timeout is the maximum wall-clock duration that a single Collect call
	// of the wrapped collector may take. A zero or negative value disables
	// the timeout, in which case the wrapped collector is called directly
	// without spawning a goroutine.
	Timeout time.Duration
	// ErrorLogger, when non-nil, receives a message when the wrapped
	// collector does not finish within Timeout or panics. The zero value
	// disables logging.
	ErrorLogger ProcessCollectorErrorLogger
}

// timedCollector wraps a Collector and bounds the duration of every Collect
// call. When the wrapped collector exceeds the configured timeout, Collect
// returns anyway with the metrics produced so far, while the wrapped
// collector keeps running in the background until it finishes on its own.
// Describe is always delegated directly, because descriptor retrieval must
// stay complete and is not expected to block.
type timedCollector struct {
	collector   Collector
	timeout     time.Duration
	errorLogger ProcessCollectorErrorLogger
}

// NewTimedCollector wraps collector so that each Collect call finishes within
// the timeout configured in opts. On timeout, the already collected metrics
// are returned and the missed remainder is logged when an error logger is
// configured. The wrapped collector and all of its metric names remain
// unchanged, so the wrapper is fully backward compatible.
func NewTimedCollector(collector Collector, opts TimedCollectorOpts) Collector {
	return &timedCollector{
		collector:   collector,
		timeout:     opts.Timeout,
		errorLogger: opts.ErrorLogger,
	}
}

// Describe delegates directly to the wrapped collector. Descriptor retrieval
// must not be truncated, otherwise registration against a pedantic registry
// would silently lose metrics.
func (c *timedCollector) Describe(ch chan<- *Desc) {
	c.collector.Describe(ch)
}

// Collect invokes the wrapped collector and guarantees that it returns within
// the configured timeout. Metrics emitted before the deadline are forwarded
// to ch; metrics emitted afterwards are discarded because their scrape has
// already ended.
func (c *timedCollector) Collect(ch chan<- Metric) {
	if c.timeout <= 0 {
		c.collector.Collect(ch)
		return
	}

	// The buffered channel decouples the wrapped collector from the timeout:
	// it never blocks on a send, so a collector that overruns the deadline
	// can still finish and release its resources instead of leaking a
	// blocked goroutine. If the buffer fills up before completion, the
	// background drain keeps consuming the excess instead of stalling the
	// wrapped collector.
	metricCh := make(chan Metric, capMetricChan)
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				if c.errorLogger != nil {
					c.errorLogger.Printf("timed collector: wrapped collector %T panicked: %v", c.collector, r)
				}
			}
		}()
		c.collector.Collect(metricCh)
	}()

	timer := time.NewTimer(c.timeout)
	defer timer.Stop()

	for {
		select {
		case metric := <-metricCh:
			// Forward every metric produced before the deadline. Sends to ch
			// may legitimately block while the registry is processing other
			// collectors; the timeout only bounds how long this Collect call
			// keeps waiting for the wrapped collector itself.
			select {
			case ch <- metric:
			case <-timer.C:
				c.logTimeout()
				go c.drainAfterTimeout(metricCh, done)
				return
			}
		case <-done:
			// The wrapped collector finished within the deadline; forward the
			// metrics still buffered in the channel before returning.
			for {
				select {
				case metric := <-metricCh:
					ch <- metric
				default:
					return
				}
			}
		case <-timer.C:
			c.logTimeout()
			go c.drainAfterTimeout(metricCh, done)
			return
		}
	}
}

// logTimeout records that the wrapped collector overran its deadline.
func (c *timedCollector) logTimeout() {
	if c.errorLogger != nil {
		c.errorLogger.Printf("timed collector: collector %T exceeded timeout %s, returning partial results", c.collector, c.timeout)
	}
}

// drainAfterTimeout consumes the remaining output of a collector that
// overran the deadline until it signals completion. It runs in its own
// goroutine so that the timed Collect call can return immediately and the
// wrapped collector never blocks on sending into the abandoned channel.
func (c *timedCollector) drainAfterTimeout(metricCh <-chan Metric, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-metricCh:
			// Discard late metrics; the scrape that requested them has
			// already moved on.
		}
	}
}
