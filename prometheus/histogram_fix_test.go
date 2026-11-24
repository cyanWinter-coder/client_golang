// Copyright 2025 The Prometheus Authors
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
	"math"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestHistogramInvalidBucketBounds is a regression test for the requirement
// that custom bucket boundaries must be validated in ascending order and that
// any illegal boundary is rejected immediately at construction time.
func TestHistogramInvalidBucketBounds(t *testing.T) {
	testCases := map[string][]float64{
		"not strictly monotonic":       {1, 2, 2, 3},
		"not monotonic at all":         {1, 2, 4, 3, 5},
		"have +Inf in the middle":      {1, 2, math.Inf(+1), 3},
		"have -Inf bound":              {1, math.Inf(-1), 3},
		"have NaN bound":               {1, math.NaN(), 3},
		"have NaN as the only bound":   {math.NaN()},
		"NaN between equal bounds":     {1, math.NaN(), 1},
		"descending after valid start": {3, 2, 1},
	}
	for name, buckets := range testCases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("Buckets %v are invalid but NewHistogram did not panic.", buckets)
				}
			}()
			_ = prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "test_histogram",
				Help:    "helpless",
				Buckets: buckets,
			})
		})
	}
}

// TestHistogramValidBucketBounds documents the boundary configurations that
// must continue to construct successfully, including a trailing +Inf bound
// and a single-element bucket list.
func TestHistogramValidBucketBounds(t *testing.T) {
	testCases := map[string][]float64{
		"strictly increasing":  {1, 2, 3},
		"trailing +Inf":        {1, 2, math.Inf(+1)},
		"single bound":         {1},
		"single trailing +Inf": {math.Inf(+1)},
		"negative bounds":      {-2, -1, -0.5, 0, 0.5},
	}
	for name, buckets := range testCases {
		t.Run(name, func(t *testing.T) {
			hist := prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "test_histogram",
				Help:    "helpless",
				Buckets: buckets,
			})
			if got := testutil.CollectAndCount(hist); got != 1 {
				t.Errorf("got %d metrics after collection, want 1", got)
			}
		})
	}
}

// TestHistogramConcurrentConsistency is a regression test for the requirement
// that the cumulative count always matches the number of observations and
// that concurrent hot writes never make a serialized snapshot inconsistent
// (non-monotonic cumulative counts or a bucket total diverging from the
// sample count).
func TestHistogramConcurrentConsistency(t *testing.T) {
	const (
		goroutines   = 16
		observations = 5000
	)
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "test_histogram",
		Help:    "helpless",
		Buckets: []float64{0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10},
	})

	var (
		start sync.WaitGroup
		wg    sync.WaitGroup
	)
	start.Add(1)
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			start.Wait()
			for i := range observations {
				// Alternate plain and exemplar observations to exercise both
				// lock-free write paths under contention.
				if i%2 == 0 {
					hist.Observe(float64(i % 100))
				} else {
					hist.(prometheus.ExemplarObserver).ObserveWithExemplar(
						float64(i%100), prometheus.Labels{"id": "42"},
					)
				}
			}
		}()
	}

	// A scraper running concurrently must only ever observe a self-consistent
	// snapshot while the writers are active.
	type scrapeError struct {
		nonMonotonic  bool
		countExceeded bool
		writeErr      error
	}
	var (
		scrapeMu  sync.Mutex
		scrapeErr scrapeError
	)
	scraperStop := make(chan struct{})
	scraperDone := make(chan struct{})
	go func() {
		defer close(scraperDone)
		start.Wait()
		for {
			select {
			case <-scraperStop:
				return
			default:
				m := &dto.Metric{}
				if err := hist.Write(m); err != nil {
					scrapeMu.Lock()
					scrapeErr.writeErr = err
					scrapeMu.Unlock()
					return
				}
				his := m.GetHistogram()
				var previous uint64
				for _, b := range his.GetBucket() {
					// Cumulative counts must be non-decreasing and must never
					// exceed the snapshot sample count, even while writes are
					// still in flight.
					if b.GetCumulativeCount() < previous || b.GetCumulativeCount() > his.GetSampleCount() {
						scrapeMu.Lock()
						if b.GetCumulativeCount() < previous {
							scrapeErr.nonMonotonic = true
						} else {
							scrapeErr.countExceeded = true
						}
						scrapeMu.Unlock()
						return
					}
					previous = b.GetCumulativeCount()
				}
			}
		}
	}()

	start.Done()
	wg.Wait()
	close(scraperStop)
	<-scraperDone

	scrapeMu.Lock()
	if scrapeErr.writeErr != nil {
		t.Fatalf("unexpected Write error during concurrent scraping: %v", scrapeErr.writeErr)
	}
	if scrapeErr.nonMonotonic {
		t.Fatal("observed non-monotonic cumulative bucket counts during concurrent scraping")
	}
	if scrapeErr.countExceeded {
		t.Fatal("observed a bucket cumulative count exceeding the sample count during concurrent scraping")
	}
	scrapeMu.Unlock()

	want := uint64(goroutines * observations)
	m := &dto.Metric{}
	if err := hist.Write(m); err != nil {
		t.Fatalf("unexpected Write error: %v", err)
	}
	if got := m.GetHistogram().GetSampleCount(); got != want {
		t.Fatalf("got sample count %d, want %d (no observation may be lost or double-counted)", got, want)
	}
	assertCumulativeCountsMonotonic(t, m)
	for _, b := range m.GetHistogram().GetBucket() {
		if b.GetCumulativeCount() > want {
			t.Fatalf("bucket cumulative count %d exceeds the sample count %d", b.GetCumulativeCount(), want)
		}
	}
}

// TestHistogramRepeatedWriteIsStable is a regression test for the requirement
// that the serialized output matches the collected value and that repeated
// collection does not mutate the internal histogram state.
func TestHistogramRepeatedWriteIsStable(t *testing.T) {
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "test_histogram",
		Help:    "helpless",
		Buckets: []float64{0.5, 1, 2, 5},
	})
	for _, v := range []float64{0.1, 0.6, 1.5, 2.5, 6} {
		hist.Observe(v)
	}
	hist.(prometheus.ExemplarObserver).ObserveWithExemplar(0.2, prometheus.Labels{"trace": "abc"})

	first := &dto.Metric{}
	if err := hist.Write(first); err != nil {
		t.Fatalf("unexpected Write error: %v", err)
	}
	wantCount := first.GetHistogram().GetSampleCount()
	wantSum := first.GetHistogram().GetSampleSum()
	wantExemplar := first.GetHistogram().GetBucket()[0].GetExemplar()
	if wantExemplar == nil {
		t.Fatal("expected an exemplar in the first bucket, got none")
	}

	for range 10 {
		next := &dto.Metric{}
		if err := hist.Write(next); err != nil {
			t.Fatalf("unexpected Write error: %v", err)
		}
		if next.GetHistogram().GetSampleCount() != wantCount {
			t.Fatalf("sample count changed across repeated Write calls: got %d, want %d",
				next.GetHistogram().GetSampleCount(), wantCount)
		}
		if next.GetHistogram().GetSampleSum() != wantSum {
			t.Fatalf("sample sum changed across repeated Write calls: got %f, want %f",
				next.GetHistogram().GetSampleSum(), wantSum)
		}
		if len(next.GetHistogram().GetBucket()) != len(first.GetHistogram().GetBucket()) {
			t.Fatalf("bucket count changed across repeated Write calls: got %d, want %d",
				len(next.GetHistogram().GetBucket()), len(first.GetHistogram().GetBucket()))
		}
		for i, gotBucket := range next.GetHistogram().GetBucket() {
			wantBucket := first.GetHistogram().GetBucket()[i]
			if gotBucket.GetCumulativeCount() != wantBucket.GetCumulativeCount() {
				t.Fatalf("bucket %d count changed across repeated Write calls: got %d, want %d",
					i, gotBucket.GetCumulativeCount(), wantBucket.GetCumulativeCount())
			}
		}
		// The exemplar must remain present and unchanged across scrapes.
		if got := next.GetHistogram().GetBucket()[0].GetExemplar(); got == nil ||
			got.GetValue() != wantExemplar.GetValue() ||
			got.GetLabel()[0].GetValue() != wantExemplar.GetLabel()[0].GetValue() {
			t.Fatalf("exemplar changed across repeated Write calls: got %v, want %v", got, wantExemplar)
		}
	}
}

// TestHistogramExemplarTTL verifies that a classic histogram keeps only the
// most recent exemplar per bucket and drops exemplars once they exceed the
// configured retention window.
func TestHistogramExemplarTTL(t *testing.T) {
	const ttl = 50 * time.Millisecond
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "test_histogram",
		Help:        "helpless",
		Buckets:     []float64{1, 2, 3, 4},
		ExemplarTTL: ttl,
	})

	// Two exemplars in the first bucket: only the most recent one survives.
	hist.(prometheus.ExemplarObserver).ObserveWithExemplar(0.5, prometheus.Labels{"id": "old"})
	hist.(prometheus.ExemplarObserver).ObserveWithExemplar(0.6, prometheus.Labels{"id": "new"})

	m := &dto.Metric{}
	if err := hist.Write(m); err != nil {
		t.Fatalf("unexpected Write error: %v", err)
	}
	exemplar := m.GetHistogram().GetBucket()[0].GetExemplar()
	if exemplar == nil {
		t.Fatal("expected an exemplar in the first bucket, got none")
	}
	if got := exemplar.GetLabel()[0].GetValue(); got != "new" {
		t.Fatalf("got exemplar label %q, want %q (only the latest exemplar must be kept)", got, "new")
	}

	// Waiting beyond the TTL must drop the exemplar from the serialized
	// output without affecting the observed counts.
	time.Sleep(ttl + 25*time.Millisecond)
	expired := &dto.Metric{}
	if err := hist.Write(expired); err != nil {
		t.Fatalf("unexpected Write error: %v", err)
	}
	if e := expired.GetHistogram().GetBucket()[0].GetExemplar(); e != nil {
		t.Fatalf("expected exemplar to be expired, got %v", e)
	}
	if got := expired.GetHistogram().GetSampleCount(); got != 2 {
		t.Fatalf("got sample count %d after TTL expiry, want 2", got)
	}

	// A second scrape must filter the same exemplar again without mutating
	// any internal state, proving repeated collection is side-effect free.
	expiredAgain := &dto.Metric{}
	if err := hist.Write(expiredAgain); err != nil {
		t.Fatalf("unexpected Write error: %v", err)
	}
	if e := expiredAgain.GetHistogram().GetBucket()[0].GetExemplar(); e != nil {
		t.Fatalf("expected exemplar to stay expired on a repeated scrape, got %v", e)
	}
	if got := expiredAgain.GetHistogram().GetSampleCount(); got != 2 {
		t.Fatalf("sample count changed across repeated scrapes after expiry: got %d, want 2", got)
	}

	// A fresh exemplar after expiry must be exposed again.
	hist.(prometheus.ExemplarObserver).ObserveWithExemplar(0.7, prometheus.Labels{"id": "fresh"})
	fresh := &dto.Metric{}
	if err := hist.Write(fresh); err != nil {
		t.Fatalf("unexpected Write error: %v", err)
	}
	if e := fresh.GetHistogram().GetBucket()[0].GetExemplar(); e == nil {
		t.Fatal("expected a fresh exemplar after expiry, got none")
	} else if got := e.GetLabel()[0].GetValue(); got != "fresh" {
		t.Fatalf("got exemplar label %q, want %q", got, "fresh")
	}
}

// TestHistogramExemplarTTLDisabled verifies that a negative ExemplarTTL
// disables classic exemplar retention entirely while leaving observations
// themselves unaffected.
func TestHistogramExemplarTTLDisabled(t *testing.T) {
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "test_histogram",
		Help:        "helpless",
		Buckets:     []float64{1, 2},
		ExemplarTTL: -1,
	})
	hist.(prometheus.ExemplarObserver).ObserveWithExemplar(0.5, prometheus.Labels{"id": "1"})

	m := &dto.Metric{}
	if err := hist.Write(m); err != nil {
		t.Fatalf("unexpected Write error: %v", err)
	}
	for i, b := range m.GetHistogram().GetBucket() {
		if b.GetExemplar() != nil {
			t.Fatalf("bucket %d exposes an exemplar although retention is disabled", i)
		}
	}
	if got := m.GetHistogram().GetSampleCount(); got != 1 {
		t.Fatalf("got sample count %d, want 1", got)
	}
}

// TestHistogramExemplarDefaultNoTTL guards the historical contract that with
// the zero-value ExemplarTTL, exemplars are retained until replaced.
func TestHistogramExemplarDefaultNoTTL(t *testing.T) {
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "test_histogram",
		Help:    "helpless",
		Buckets: []float64{1, 2},
	})
	hist.(prometheus.ExemplarObserver).ObserveWithExemplar(0.5, prometheus.Labels{"id": "1"})

	// Wait beyond a plausible default window: the exemplar must still be
	// exposed because no TTL was configured.
	time.Sleep(75 * time.Millisecond)
	m := &dto.Metric{}
	if err := hist.Write(m); err != nil {
		t.Fatalf("unexpected Write error: %v", err)
	}
	if e := m.GetHistogram().GetBucket()[0].GetExemplar(); e == nil {
		t.Fatal("expected the exemplar to be retained with the default zero TTL, got none")
	}
}

// TestNativeHistogramConcurrentConsistency exercises the sparse bucket path
// under concurrent observations and scrapes. The serialized bucket population
// must always reconcile with the sample count, and the sample count must match
// the number of observations once all writers have finished.
func TestNativeHistogramConcurrentConsistency(t *testing.T) {
	const (
		goroutines   = 8
		observations = 3000
	)
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:                        "test_native_histogram",
		Help:                        "helpless",
		NativeHistogramBucketFactor: 1.1,
	})

	var (
		start sync.WaitGroup
		wg    sync.WaitGroup
	)
	start.Add(1)
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			start.Wait()
			for i := range observations {
				hist.Observe(float64((i % 200) + 1))
			}
		}()
	}

	var (
		scrapeMu      sync.Mutex
		mismatched    bool
		sawWriteError error
	)
	scraperStop := make(chan struct{})
	scraperDone := make(chan struct{})
	go func() {
		defer close(scraperDone)
		start.Wait()
		for {
			select {
			case <-scraperStop:
				return
			default:
				m := &dto.Metric{}
				if err := hist.Write(m); err != nil {
					scrapeMu.Lock()
					sawWriteError = err
					scrapeMu.Unlock()
					return
				}
				// Every completed observation is reflected in exactly one
				// sparse bucket, so the bucket population must never exceed
				// the sample count while a scrape is in flight.
				if sparseBucketPopulation(m.GetHistogram()) > m.GetHistogram().GetSampleCount() {
					scrapeMu.Lock()
					mismatched = true
					scrapeMu.Unlock()
					return
				}
			}
		}
	}()

	start.Done()
	wg.Wait()
	close(scraperStop)
	<-scraperDone

	scrapeMu.Lock()
	if sawWriteError != nil {
		t.Fatalf("unexpected Write error during concurrent scraping: %v", sawWriteError)
	}
	if mismatched {
		t.Fatal("sparse bucket population exceeded the sample count during concurrent scraping")
	}
	scrapeMu.Unlock()

	want := uint64(goroutines * observations)
	m := &dto.Metric{}
	if err := hist.Write(m); err != nil {
		t.Fatalf("unexpected Write error: %v", err)
	}
	if got := m.GetHistogram().GetSampleCount(); got != want {
		t.Fatalf("got sample count %d, want %d", got, want)
	}
	// The sum of all bucket populations (zero, positive and negative buckets)
	// must equal the total sample count. Note that sparse bucket deltas may
	// legitimately be negative because they encode differences between
	// adjacent cumulative bucket counts.
	if got, wantPop := sparseBucketPopulation(m.GetHistogram()), want; got != wantPop {
		t.Fatalf("got bucket population %d, want sample count %d", got, wantPop)
	}
}

// sparseBucketPopulation reconstructs the total number of observations stored
// in a native histogram's sparse buckets by decoding the delta-encoded bucket
// counts and adding the zero bucket population.
func sparseBucketPopulation(h *dto.Histogram) uint64 {
	var population int64
	for _, deltas := range [][]int64{h.GetPositiveDelta(), h.GetNegativeDelta()} {
		var cumulative int64
		for _, d := range deltas {
			cumulative += d
			population += cumulative
		}
	}
	return uint64(population) + h.GetZeroCount()
}

// assertCumulativeCountsMonotonic fails the test if classic bucket cumulative
// counts decrease from one bucket to the next.
func assertCumulativeCountsMonotonic(t *testing.T, m *dto.Metric) {
	t.Helper()
	var previous uint64
	for i, b := range m.GetHistogram().GetBucket() {
		if i > 0 && b.GetCumulativeCount() < previous {
			t.Errorf("cumulative count decreased at bucket %d: %d < %d",
				i, b.GetCumulativeCount(), previous)
		}
		previous = b.GetCumulativeCount()
	}
}
