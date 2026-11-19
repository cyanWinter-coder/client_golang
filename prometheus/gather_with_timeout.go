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
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus/internal"
	dto "github.com/prometheus/client_model/go"
)

// ContextCollector is an optional Collector interface for collectors that are
// able to stop collection early when the provided context is canceled. The
// context-aware gather methods pass the shared, deadline-bound context to
// CollectWithContext so that a single slow collector cannot delay the whole
// scrape beyond its deadline.
//
// Implementations must observe ctx.Done while sending metrics or waiting on
// slow resources and return as soon as possible after cancellation. Metrics
// sent before the cancellation are still processed and returned. Collectors
// that do not implement this interface are collected through their regular
// Collect method and are abandoned in place when the deadline expires; the
// goroutines running them exit once the collector eventually returns on its
// own.
type ContextCollector interface {
	Collector

	// CollectWithContext behaves like Collect but must stop sending metrics
	// and return promptly when ctx is canceled.
	CollectWithContext(ctx context.Context, ch chan<- Metric)
}

// TransactionalCollector is a Collector that requires a completion callback
// once the caller no longer needs the metrics collected during a gather, for
// example because it reuses buffers between scrapes. The context-aware gather
// methods guarantee that the returned done function invokes the callback
// exactly once, including when the gather fails, times out, or is canceled.
// The done function returned by CollectTransactional itself must never be
// nil.
//
// This is the collector-level counterpart of TransactionalGatherer, which
// serves the same purpose at the Gatherer level.
type TransactionalCollector interface {
	Collector

	// CollectTransactional collects metrics like Collect and returns a done
	// function that releases the resources associated with the collection.
	// The gather invokes done exactly once.
	CollectTransactional(ch chan<- Metric) (done func())
}

// GatherConfig configures the context-aware gather methods. The zero value is
// valid and yields sensible defaults: collectors are identified by their Go
// type, and the per-collector metric channel is buffered with capMetricChan.
type GatherConfig struct {
	// CollectorNames maps a Collector instance to a stable source identifier
	// used in error messages. Collectors absent from the map are identified
	// by their Go type. The map is only read, never retained or mutated.
	CollectorNames map[Collector]string

	// MetricChannelCapacity overrides the capacity of the per-collector
	// metric channel. A zero or negative value selects the default
	// capMetricChan. A larger buffer lets a collector keep producing metrics
	// for longer after the context is canceled before it has to observe the
	// cancellation itself.
	MetricChannelCapacity int
}

// sourceName returns the error source identifier for the provided collector.
func (cfg GatherConfig) sourceName(c Collector) string {
	if name, ok := cfg.CollectorNames[c]; ok {
		return name
	}
	return fmt.Sprintf("%T", c)
}

// channelCapacity returns the configured per-collector channel capacity or the
// default capMetricChan when the configuration is unset.
func (cfg GatherConfig) channelCapacity() int {
	if cfg.MetricChannelCapacity > 0 {
		return cfg.MetricChannelCapacity
	}
	return capMetricChan
}

// GatherWithTimeout works like Gather but bounds the whole collection with the
// provided timeout. All collectors share the same deadline: when the timeout
// expires (or ctx is canceled, whichever happens first), collectors
// implementing ContextCollector stop sending as soon as they observe the
// cancellation, and the gather returns promptly with the metrics collected so
// far. A single collector that blocks past the deadline does not prevent the
// results of all other collectors from being returned.
//
// The existing Gather method is unchanged and has no timeout behavior.
//
// Even on timeout, cancellation, or collector errors, all successfully
// collected metrics are returned together with an error that identifies the
// offending collector source. Panics in collectors are recovered and reported
// as errors, matching the behavior of Gather.
func (r *Registry) GatherWithTimeout(ctx context.Context, timeout time.Duration, cfg GatherConfig) ([]*dto.MetricFamily, error) {
	mfs, _, err := r.gatherContext(ctx, timeout, cfg)
	return mfs, err
}

// GatherTransactionalWithContext works like GatherWithTimeout but additionally
// guarantees that the completion callback of every TransactionalCollector is
// invoked exactly once through the returned done function. The returned done
// function is never nil and must be invoked by the caller, even when the
// returned error is non-nil or the gather was canceled before all collectors
// finished.
//
// When the deadline expires before a transactional collector finishes, its
// completion callback is deferred until the collector eventually returns, so
// resource release is never skipped and never runs concurrently with the
// collector's own collection logic.
func (r *Registry) GatherTransactionalWithContext(
	ctx context.Context, timeout time.Duration, cfg GatherConfig,
) (mfs []*dto.MetricFamily, done func(), err error) {
	return r.gatherContext(ctx, timeout, cfg)
}

// collectorOutcome carries the collection error of a collector that finished
// while the context was still alive. It is only published after the
// collector's metric channel has been fully drained.
type collectorOutcome struct {
	// source identifies the collector in error messages.
	source string
	// err holds the collection error, including recovered panics.
	err error
}

// gatherContext implements the shared collection logic of the context-aware
// gather methods. It snapshots the registered collectors under the registry
// read lock and then collects each of them in its own goroutine.
func (r *Registry) gatherContext(
	ctx context.Context, timeout time.Duration, cfg GatherConfig,
) ([]*dto.MetricFamily, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	r.mtx.RLock()
	if len(r.collectorsByID) == 0 && len(r.uncheckedCollectors) == 0 {
		// Fast path, mirroring Gather.
		r.mtx.RUnlock()
		return nil, func() {}, nil
	}

	collectors := make([]Collector, 0, len(r.collectorsByID)+len(r.uncheckedCollectors))
	for _, collector := range r.collectorsByID {
		collectors = append(collectors, collector)
	}
	collectors = append(collectors, r.uncheckedCollectors...)

	var registeredDescIDs map[uint64]struct{} // Only used for pedantic checks.
	if r.pedanticChecksEnabled {
		registeredDescIDs = make(map[uint64]struct{}, len(r.descIDs))
		for id := range r.descIDs {
			registeredDescIDs[id] = struct{}{}
		}
	}
	r.mtx.RUnlock()

	xxh := xxhashPool.Get().(*xxhash.Digest)
	defer xxhashPool.Put(xxh)

	// metricOut is the shared channel on which reader goroutines forward
	// collected metrics while the context is alive.
	metricOut := make(chan Metric, capMetricChan)
	// outcomeCh is buffered for every collector so that publishing an outcome
	// never blocks. A janitor goroutine closes it once all readers are done.
	outcomeCh := make(chan collectorOutcome, len(collectors))

	var readerWg sync.WaitGroup
	readerWg.Add(len(collectors))
	completionFns := make([]func(), 0, len(collectors))
	for _, collector := range collectors {
		completionFns = append(completionFns, r.startCollector(ctx, collector, cfg, metricOut, outcomeCh, &readerWg))
	}
	go func() {
		readerWg.Wait()
		close(outcomeCh)
	}()

	metricFamiliesByName := make(map[string]*dto.MetricFamily, len(r.dimHashesByName))
	metricHashes := make(map[uint64]struct{}, r.lastMetricCount.Load())
	safeErrs := &SafeMultiError{}

	canceled := false
	finished := 0
	for !canceled && finished < len(collectors) {
		// Give metricOut strict priority: a reader may have forwarded a
		// metric into the buffered metricOut channel and immediately published
		// its outcome. If the outcome were consumed first, the loop could exit
		// with forwarded metrics still buffered in metricOut, silently
		// dropping them. Draining ready metrics before selecting on outcomeCh
		// prevents that race.
		select {
		case metric := <-metricOut:
			safeErrs.Append(processMetric(
				metric, metricFamiliesByName,
				metricHashes,
				registeredDescIDs,
				xxh,
			))
			continue
		default:
		}

		select {
		case <-ctx.Done():
			// The deadline expired or the caller canceled the gather. Return
			// promptly without waiting for collectors that ignore
			// cancellation. A janitor goroutine below reaps their channels
			// and reader goroutines.
			safeErrs.Append(fmt.Errorf("gather stopped before all collectors finished: %w", ctx.Err()))
			canceled = true
		case metric := <-metricOut:
			safeErrs.Append(processMetric(
				metric, metricFamiliesByName,
				metricHashes,
				registeredDescIDs,
				xxh,
			))
		case outcome := <-outcomeCh:
			finished++
			safeErrs.Append(wrapSourceError(outcome.source, outcome.err))
		}
	}

	// All collectors have finished. Drain any metrics still buffered in
	// metricOut so that nothing forwarded before the last outcome is lost.
	if !canceled {
	drainLoop:
		for {
			select {
			case metric := <-metricOut:
				safeErrs.Append(processMetric(
					metric, metricFamiliesByName,
					metricHashes,
					registeredDescIDs,
					xxh,
				))
			default:
				break drainLoop
			}
		}
	}

	if canceled {
		// Reap everything the gather no longer reads: wait for the reader
		// goroutines to observe the cancellation and return, then drain the
		// buffered channels so no sender blocks. Detached per-collector
		// drainers keep un-interruptible collectors unblocked until they
		// return on their own.
		go func() {
			readerWg.Wait()
			for outcome := range outcomeCh {
				safeErrs.Append(wrapSourceError(outcome.source, outcome.err))
			}
			drainChannel(metricOut)
		}()
	}

	r.lastMetricCount.Store(int64(len(metricHashes)))
	return internal.NormalizeMetricFamilies(metricFamiliesByName), combineDoneFuncs(completionFns), safeErrs.errs.MaybeUnwrap()
}

// startCollector launches a collector and returns its completion callback,
// which is always non-nil. For a TransactionalCollector, the callback defers
// the actual release until collection has ended, so invoking it after a
// timeout never runs concurrently with the collector's own logic. The
// once-guard in combineDoneFuncs additionally guarantees that every returned
// completion callback is triggered at most once.
func (r *Registry) startCollector(
	ctx context.Context,
	collector Collector,
	cfg GatherConfig,
	metricOut chan<- Metric,
	outcomeCh chan<- collectorOutcome,
	readerWg *sync.WaitGroup,
) func() {
	metricCh := make(chan Metric, cfg.channelCapacity())
	// collected is closed when the collector's collection call has returned
	// and its metric channel has been closed.
	collected := make(chan struct{})

	var (
		once          sync.Once
		realDone      func()
		collectionErr error
	)

	// The collector goroutine runs the actual collection. It owns metricCh
	// and closes it when collection has returned, so a channel range in the
	// reader drains every buffered metric before observing the close.
	go func() {
		defer close(collected)
		defer close(metricCh)
		switch c := collector.(type) {
		case TransactionalCollector:
			defer recoverCollectorPanic(collector, metricCh, &collectionErr)
			done := c.CollectTransactional(metricCh)
			if done == nil {
				collectionErr = fmt.Errorf("transactional collector %q returned a nil done function", cfg.sourceName(collector))
				done = func() {}
			}
			realDone = done
		case ContextCollector:
			defer recoverCollectorPanic(collector, metricCh, &collectionErr)
			c.CollectWithContext(ctx, metricCh)
		default:
			// safeCollect recovers panics itself and reports them through an
			// invalid metric, mirroring the behavior of Gather.
			collectionErr = safeCollect(collector, metricCh)
		}
	}()

	// The completion callback always waits for collection to end before
	// releasing resources. It is invoked through a short-lived goroutine so
	// that calling done never blocks, even when a collector is still running
	// after a timeout.
	completion := func() {
		go func() {
			<-collected
			if realDone != nil {
				once.Do(realDone)
			}
		}()
	}

	go r.runReader(ctx, cfg.sourceName(collector), metricCh, collected, &collectionErr, metricOut, outcomeCh, readerWg)
	return completion
}

// recoverCollectorPanic recovers a panic from a ContextCollector or
// TransactionalCollector and turns it into a collection error. It mirrors the
// panic handling of safeCollect for regular collectors, including an invalid
// metric sent on a best-effort basis.
func recoverCollectorPanic(collector Collector, metricCh chan Metric, collectionErr *error) {
	recovered := recover()
	if recovered == nil {
		return
	}
	buf := make([]byte, 64<<10) // 64 KB.
	n := runtime.Stack(buf, false)
	*collectionErr = fmt.Errorf(
		"prometheus collector panic recovered: type=%T: error=%v\nstack trace=%s",
		collector, recovered, buf[:n],
	)
	// A non-blocking send avoids leaking this goroutine when the metric
	// channel is full after a cancellation.
	select {
	case metricCh <- NewInvalidMetric(NewInvalidDesc(*collectionErr), *collectionErr):
	default:
	}
}

// runReader forwards a collector's metrics to metricOut while the context is
// alive. The collector goroutine closes metricCh when it returns, so a range
// over the channel delivers every buffered metric before the closed-channel
// case fires. On cancellation the reader returns immediately and detaches a
// drainer goroutine that keeps the collector unblocked until it returns.
func (r *Registry) runReader(
	ctx context.Context,
	source string,
	metricCh chan Metric,
	collected <-chan struct{},
	collectionErr *error,
	metricOut chan<- Metric,
	outcomeCh chan<- collectorOutcome,
	readerWg *sync.WaitGroup,
) {
	defer readerWg.Done()

	// detach hands the collector's channel to a background goroutine after
	// the gather stopped reading it. The goroutine waits for the collector to
	// return, then drains the closed channel so the collector never blocks on
	// a full buffer and can exit. This is the only path on which an
	// un-interruptible collector outlives the gather.
	detach := func() {
		go func() {
			<-collected
			for range metricCh {
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			detach()
			return
		case metric, ok := <-metricCh:
			if !ok {
				// The channel is closed after the collector returned, which
				// happens-after the write of collectionErr, so the read here
				// is safely ordered.
				outcomeCh <- collectorOutcome{source: source, err: *collectionErr}
				return
			}
			select {
			case metricOut <- metric:
			case <-ctx.Done():
				detach()
				return
			}
		}
	}
}

// drainChannel consumes all currently buffered values from a channel without
// blocking. It is only called once no further sends can happen (after the
// collected signal) or once all reader goroutines have returned.
func drainChannel[T any](ch <-chan T) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// combineDoneFuncs returns a single callback that invokes every provided
// completion function exactly once. An empty input yields a non-nil no-op
// callback so that callers can always invoke the returned done function.
func combineDoneFuncs(doneFns []func()) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			for _, done := range doneFns {
				done()
			}
		})
	}
}

// wrapSourceError prefixes a collector error with its source identifier so that
// merged errors always retain their origin. A nil error passes through.
func wrapSourceError(source string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("[from collector %q] %w", source, err)
}
