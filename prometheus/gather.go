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
	"cmp"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"
	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus/internal"
)

// collectorEntry pairs a checked Collector with the XOR of its descriptor
// IDs. The ID is the stable ordering key within a registry generation.
type collectorEntry struct {
	id        uint64
	collector Collector
}

// gatherSnapshot is the immutable single-generation view of a Registry taken
// at the beginning of a Gather call. All data reachable from the snapshot is
// owned by the snapshot. Concurrent Register and Unregister calls only mutate
// the Registry itself and therefore only affect later Gather calls; they never
// modify an in-flight snapshot.
type gatherSnapshot struct {
	// checkedCollectors are the checked Collectors, ordered by collector ID
	// so that a Gather over the same registry generation always processes
	// them in the same sequence.
	checkedCollectors []collectorEntry
	// uncheckedCollectors are the unchecked Collectors in registration order.
	uncheckedCollectors []Collector
	// descOwnerByID maps each descriptor ID to the Collector that reported
	// it. It attributes a checked Metric to its producing Collector without
	// any per-metric allocation, because descriptor IDs are unique across
	// checked Collectors.
	descOwnerByID map[uint64]Collector
	// registeredDescIDs is non-nil only for pedantic registries. It is a copy
	// of the registry's descriptor ID set and used for the pedantic checks.
	registeredDescIDs map[uint64]struct{}
}

// snapshotGather builds the immutable single-generation view of the registry.
// It must be called while holding the registry's read lock.
func (r *Registry) snapshotGather() *gatherSnapshot {
	s := &gatherSnapshot{
		checkedCollectors:   make([]collectorEntry, 0, len(r.collectorsByID)),
		uncheckedCollectors: slices.Clone(r.uncheckedCollectors),
		descOwnerByID:       make(map[uint64]Collector, len(r.descIDs)),
	}
	for id, collector := range r.collectorsByID {
		s.checkedCollectors = append(s.checkedCollectors, collectorEntry{id: id, collector: collector})
		// The descriptor slices are immutable, so sharing them is safe.
		for _, desc := range r.descsByID[id] {
			s.descOwnerByID[desc.id] = collector
		}
	}
	// Sort the checked Collectors by the IDs of their descriptors, which
	// makes processing order deterministic for a given registry generation.
	slices.SortFunc(s.checkedCollectors, func(a, b collectorEntry) int {
		return cmp.Compare(a.id, b.id)
	})
	if r.pedanticChecksEnabled {
		s.registeredDescIDs = make(map[uint64]struct{}, len(r.descIDs))
		for id := range r.descIDs {
			s.registeredDescIDs[id] = struct{}{}
		}
	}
	return s
}

// CollectorConflictError reports that a Metric collected during Gather is
// inconsistent with a metric family another Collector already contributed to
// the same Gather generation. It names both the Collector that established the
// family and the Collector that produced the conflicting metric, so callers can
// attribute the conflict without reproducing the race manually.
//
// The wrapped error, accessible via errors.Unwrap and errors.Is/As, preserves
// the original conflict message describing the exact mismatch.
type CollectorConflictError struct {
	// Family is the fully-qualified name of the affected metric family.
	Family string
	// ExistingCollector is the Collector that first contributed to the family.
	// It may be nil if the family was created by a path without a known owner.
	ExistingCollector Collector
	// ConflictingCollector is the Collector that produced the conflicting metric.
	ConflictingCollector Collector
	// Err is the underlying consistency error with the original message.
	Err error
}

// newCollectorConflictError wraps a family-level consistency error with the
// names of the Collectors involved. A nil existing Collector is represented
// transparently in the message.
func newCollectorConflictError(family string, existing, conflicting Collector, err error) *CollectorConflictError {
	return &CollectorConflictError{
		Family:               family,
		ExistingCollector:    existing,
		ConflictingCollector: conflicting,
		Err:                  err,
	}
}

// conflictError preserves the original consistency error message when a
// conflict happens within a single Collector (or when the owner is unknown),
// and only adds the CollectorConflictError wrapper for a genuine conflict
// between two different Collectors. Keeping the same-Collector path unwrapped
// preserves the historical error strings that existing callers may match on.
func conflictError(family string, owner, conflicting Collector, err error) error {
	if owner == nil || owner == conflicting {
		return err
	}
	return newCollectorConflictError(family, owner, conflicting, err)
}

// Error implements error.
func (e *CollectorConflictError) Error() string {
	if e.ExistingCollector == nil {
		return fmt.Sprintf(
			"collector %T conflicts while gathering metric family %q: %s",
			e.ConflictingCollector, e.Family, e.Err,
		)
	}
	return fmt.Sprintf(
		"collectors %T and %T conflict while gathering metric family %q: %s",
		e.ExistingCollector, e.ConflictingCollector, e.Family, e.Err,
	)
}

// Unwrap returns the underlying consistency error.
func (e *CollectorConflictError) Unwrap() error {
	return e.Err
}

// collisionFamilyName returns the name of the already gathered family that a
// new family with the provided name would collide with according to the magic
// suffix rules, or an empty string if there is no such collision. It mirrors
// the checks in checkSuffixCollisions without duplicating the error messages.
func collisionFamilyName(mfs map[string]*dto.MetricFamily, newName string) string {
	switch {
	case strings.HasSuffix(newName, "_count"):
		name := newName[:len(newName)-6]
		if existing, ok := mfs[name]; ok {
			switch existing.GetType() {
			case dto.MetricType_SUMMARY, dto.MetricType_HISTOGRAM:
				return name
			}
		}
	case strings.HasSuffix(newName, "_sum"):
		name := newName[:len(newName)-4]
		if existing, ok := mfs[name]; ok {
			switch existing.GetType() {
			case dto.MetricType_SUMMARY, dto.MetricType_HISTOGRAM:
				return name
			}
		}
	case strings.HasSuffix(newName, "_bucket"):
		name := newName[:len(newName)-7]
		if existing, ok := mfs[name]; ok {
			if existing.GetType() == dto.MetricType_HISTOGRAM {
				return name
			}
		}
	}
	for _, suffix := range []string{"_count", "_sum", "_bucket"} {
		if _, ok := mfs[newName+suffix]; ok {
			return newName + suffix
		}
	}
	return ""
}

// Gather implements Gatherer.
//
// Before Collect, Gather calls CleanupExpired on registered collectors that
// implement ExpiredCleaner, so expired Vec metrics can be reclaimed on scrape.
// This includes collectors registered through wrapping collectors and nested
// registries. Cleanup panics are recovered and reported as collection errors.
//
// Gather first takes an immutable snapshot of the registry, so concurrent
// Register and Unregister calls only affect later Gather calls and never alter
// the descriptor-to-collector mapping used by the in-flight generation.
func (r *Registry) Gather() ([]*dto.MetricFamily, error) {
	return r.gather(context.Background())
}

// GatherWithContext behaves like Gather but honors the provided context.
//
// Once the context is canceled, GatherWithContext stops invoking any further
// Collectors. A Collector whose Collect method is already running is allowed
// to finish, because the Collector interface has no cancellation hook. All
// metrics that were successfully processed before the cancellation are
// retained in the returned slice, and the context's error is appended to the
// returned error. As with Gather, the returned slice may therefore be partial
// when a non-nil error is returned.
//
// A nil context is treated like context.Background. Passing
// context.Background is equivalent to calling Gather directly.
//
// This is an additive method: it does not change the Gatherer interface or
// the behavior of Gather. The experimental context-aware Gatherer wrapper in
// the exp module builds on top of it.
func (r *Registry) GatherWithContext(ctx context.Context) ([]*dto.MetricFamily, error) {
	return r.gather(ctx)
}

// uncheckedMetric pairs a Metric emitted by an unchecked Collector with that
// Collector. Checked Collectors are attributed through the snapshot's
// descOwnerByID map instead, which keeps the common hot path allocation-free.
type uncheckedMetric struct {
	metric    Metric
	collector Collector
}

// gather is the shared implementation behind Gather and GatherWithContext.
// It takes an immutable snapshot of the registry and runs one isolated
// generation of collection against it.
func (r *Registry) gather(ctx context.Context) ([]*dto.MetricFamily, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Honor an already-canceled context even on the empty-registry fast path.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mtx.RLock()
	if len(r.collectorsByID) == 0 && len(r.uncheckedCollectors) == 0 {
		// Fast path.
		r.mtx.RUnlock()
		return nil, nil
	}
	snapshot := r.snapshotGather()
	r.mtx.RUnlock()

	xxh := xxhashPool.Get().(*xxhash.Digest)
	defer xxhashPool.Put(xxh)

	var (
		// Checked metrics travel on a plain channel so they incur no wrapping
		// allocation; their producing Collector is resolved via the snapshot.
		checkedMetricChan   = make(chan Metric, capMetricChan)
		uncheckedMetricChan = make(chan uncheckedMetric, capMetricChan)
		metricHashes        = make(map[uint64]struct{}, r.lastMetricCount.Load())
		wg                  sync.WaitGroup
		safeErrs            = &SafeMultiError{} // To collect errors in a thread-safe way.
		collectorCount      = len(snapshot.checkedCollectors) + len(snapshot.uncheckedCollectors)
	)

	metricFamiliesByName := make(map[string]*dto.MetricFamily, len(snapshot.checkedCollectors))
	familyOwners := map[string]Collector{}

	// Every Collector runs in its own goroutine. This avoids worker-pool
	// starvation, where a fixed set of workers could all block on slow
	// Collectors while other Collectors wait for a free worker and the gather
	// loop in turn waits for their channels. A canceled context skips
	// not-yet-started Collectors; running ones finish because Collect has no
	// cancellation hook and their channels stay drained below.
	wg.Add(collectorCount)
	for _, entry := range snapshot.checkedCollectors {
		collector := entry.collector
		go func() {
			defer wg.Done()
			if err := ctx.Err(); err != nil {
				return
			}
			safeErrs.Append(safeCollect(collector, checkedMetricChan))
		}()
	}
	for _, collector := range snapshot.uncheckedCollectors {
		collector := collector
		go func() {
			// safeCollectUnchecked joins its forwarder before returning, so by
			// the time wg.Done runs no goroutine can still send on
			// uncheckedMetricChan. That ordering is what makes the close below
			// race-free.
			defer wg.Done()
			if err := ctx.Err(); err != nil {
				return
			}
			safeCollectUnchecked(collector, uncheckedMetricChan)
		}()
	}

	// Close both channels once all collectors have been collected or skipped.
	go func() {
		wg.Wait()
		close(checkedMetricChan)
		close(uncheckedMetricChan)
	}()

	// Drain both channels on a premature return so collector goroutines never
	// block on a send into an abandoned channel.
	defer func() {
		for range checkedMetricChan {
			continue
		}
		for range uncheckedMetricChan {
			continue
		}
	}()

	canceled := false
	// resolveCheckedCollector attributes a checked Metric to its producing
	// Collector through the frozen snapshot. A nil Metric is passed through so
	// that processMetric reports the original "collected nil metric" error.
	resolveCheckedCollector := func(metric Metric) Collector {
		if metric == nil || metric.Desc() == nil {
			return nil
		}
		return snapshot.descOwnerByID[metric.Desc().id]
	}
	// Copy the channel references so they can be niled out to drop them from
	// the select statements once closed.
	cmc := checkedMetricChan
	umc := uncheckedMetricChan

	// Block on the two channels until both are closed and drained. There is no
	// worker budget to manage because every Collector already has its own
	// goroutine, so a blocking select without a default case is sufficient.
	for cmc != nil || umc != nil {
		select {
		case metric, ok := <-cmc:
			if !ok {
				cmc = nil
				break
			}
			if !canceled && ctx.Err() != nil {
				// Stop processing further metrics. Buffers are still drained so
				// running collectors finish; processed families are retained.
				canceled = true
				safeErrs.Append(ctx.Err())
			}
			if canceled {
				break
			}
			safeErrs.Append(processMetric(
				metric,
				resolveCheckedCollector(metric),
				metricFamiliesByName,
				familyOwners,
				metricHashes,
				snapshot.registeredDescIDs,
				xxh,
			))
		case envelope, ok := <-umc:
			if !ok {
				umc = nil
				break
			}
			if !canceled && ctx.Err() != nil {
				canceled = true
				safeErrs.Append(ctx.Err())
			}
			if canceled {
				break
			}
			// Unchecked metrics skip the pedantic registered-descriptor checks.
			safeErrs.Append(processMetric(
				envelope.metric,
				envelope.collector,
				metricFamiliesByName,
				familyOwners,
				metricHashes,
				nil,
				xxh,
			))
		}
	}

	r.lastMetricCount.Store(int64(len(metricHashes)))
	return internal.NormalizeMetricFamilies(metricFamiliesByName), safeErrs.errs.MaybeUnwrap()
}

// safeCollect wraps Collector.Collect for a checked Collector, recovers
// panics, and reports them as collection errors plus an invalid metric. It
// matches the historical safeCollect behavior and sends plain Metrics.
func safeCollect(c Collector, ch chan<- Metric) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			buf := make([]byte, 64<<10) // 64 KB.
			n := runtime.Stack(buf, false)
			err = fmt.Errorf("prometheus collector panic recovered: type=%T: error=%v\nstack trace=%s", c, recovered, buf[:n])
			ch <- NewInvalidMetric(NewInvalidDesc(err), err)
		}
	}()
	cleanupWrappedExpired(c)
	c.Collect(ch)
	return err
}

// safeCollectUnchecked wraps Collector.Collect for an unchecked Collector,
// recovering panics like safeCollect while attributing every emitted metric to
// its producing Collector on the unchecked channel.
//
// The Collector interface only accepts a real chan<- Metric, so a single
// forwarder goroutine per unchecked Collector bridges that buffered Metric
// channel to the buffered envelope channel. The forwarder is started first and
// joined last on every return path, including the panic-recovery path, so it
// can never outlive this call and no send on the envelope channel races with
// the channel being closed by the gather loop.
func safeCollectUnchecked(c Collector, ch chan<- uncheckedMetric) (err error) {
	metricCh := make(chan Metric, capMetricChan)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for metric := range metricCh {
			ch <- uncheckedMetric{metric: metric, collector: c}
		}
	}()
	// Closing the Metric channel and joining the forwarder run in a defer that
	// is registered before the panic recovery defer, so they execute even when
	// Collect panics, guaranteeing the forwarder exits before this function
	// returns.
	defer func() {
		close(metricCh)
		<-done
	}()
	defer func() {
		if recovered := recover(); recovered != nil {
			buf := make([]byte, 64<<10) // 64 KB.
			n := runtime.Stack(buf, false)
			err = fmt.Errorf("prometheus collector panic recovered: type=%T: error=%v\nstack trace=%s", c, recovered, buf[:n])
			// Send through the Metric channel so the still-running forwarder
			// attributes the invalid metric like every other metric. The
			// deferred close below then releases the forwarder cleanly.
			metricCh <- NewInvalidMetric(NewInvalidDesc(err), err)
		}
	}()
	cleanupWrappedExpired(c)
	c.Collect(metricCh)
	return err
}

// AsCollectorConflictError returns the CollectorConflictError wrapping err, if
// any, so callers can inspect the conflicting Collectors programmatically.
func AsCollectorConflictError(err error) (*CollectorConflictError, bool) {
	var conflict *CollectorConflictError
	if errors.As(err, &conflict) {
		return conflict, true
	}
	return nil, false
}
