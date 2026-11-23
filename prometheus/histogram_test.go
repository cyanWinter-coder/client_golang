// Copyright 2015 The Prometheus Authors
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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/prometheus/client_golang/prometheus/internal"
)

func benchmarkHistogramObserve(w int, b *testing.B) {
	b.StopTimer()

	wg := new(sync.WaitGroup)
	wg.Add(w)

	g := new(sync.WaitGroup)
	g.Add(1)

	s := NewHistogram(HistogramOpts{})

	for range w {
		go func() {
			g.Wait()

			for i := 0; i < b.N; i++ {
				s.Observe(float64(i))
			}

			wg.Done()
		}()
	}

	b.StartTimer()
	g.Done()
	wg.Wait()
}

func BenchmarkHistogramObserve1(b *testing.B) {
	benchmarkHistogramObserve(1, b)
}

func BenchmarkHistogramObserve2(b *testing.B) {
	benchmarkHistogramObserve(2, b)
}

func BenchmarkHistogramObserve4(b *testing.B) {
	benchmarkHistogramObserve(4, b)
}

func BenchmarkHistogramObserve8(b *testing.B) {
	benchmarkHistogramObserve(8, b)
}

func benchmarkHistogramWrite(w int, b *testing.B) {
	b.StopTimer()

	wg := new(sync.WaitGroup)
	wg.Add(w)

	g := new(sync.WaitGroup)
	g.Add(1)

	s := NewHistogram(HistogramOpts{})

	for i := range 1000000 {
		s.Observe(float64(i))
	}

	for range w {
		outs := make([]dto.Metric, b.N)

		go func(o []dto.Metric) {
			g.Wait()

			for i := 0; i < b.N; i++ {
				s.Write(&o[i])
			}

			wg.Done()
		}(outs)
	}

	b.StartTimer()
	g.Done()
	wg.Wait()
}

func BenchmarkHistogramWrite1(b *testing.B) {
	benchmarkHistogramWrite(1, b)
}

func BenchmarkHistogramWrite2(b *testing.B) {
	benchmarkHistogramWrite(2, b)
}

func BenchmarkHistogramWrite4(b *testing.B) {
	benchmarkHistogramWrite(4, b)
}

func BenchmarkHistogramWrite8(b *testing.B) {
	benchmarkHistogramWrite(8, b)
}

func TestHistogramNonMonotonicBuckets(t *testing.T) {
	testCases := map[string][]float64{
		"not strictly monotonic":  {1, 2, 2, 3},
		"not monotonic at all":    {1, 2, 4, 3, 5},
		"have +Inf in the middle": {1, 2, math.Inf(+1), 3},
	}
	for name, buckets := range testCases {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("Buckets %v are %s but NewHistogram did not panic.", buckets, name)
				}
			}()
			_ = NewHistogram(HistogramOpts{
				Name:    "test_histogram",
				Help:    "helpless",
				Buckets: buckets,
			})
		}()
	}
}

// Intentionally adding +Inf here to test if that case is handled correctly.
// Also, getCumulativeCounts depends on it.
var testBuckets = []float64{-2, -1, -0.5, 0, 0.5, 1, 2, math.Inf(+1)}

func TestHistogramConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	rand.New(rand.NewSource(42))

	it := func(n uint32) bool {
		mutations := int(n%1e4 + 1e4)
		concLevel := int(n%5 + 1)
		total := mutations * concLevel

		var start, end sync.WaitGroup
		start.Add(1)
		end.Add(concLevel)

		his := NewHistogram(HistogramOpts{
			Name:    "test_histogram",
			Help:    "helpless",
			Buckets: testBuckets,
		})

		allVars := make([]float64, total)
		var sampleSum float64
		for i := range concLevel {
			vals := make([]float64, mutations)
			for j := range mutations {
				v := rand.NormFloat64()
				vals[j] = v
				allVars[i*mutations+j] = v
				sampleSum += v
			}

			go func(vals []float64) {
				start.Wait()
				for _, v := range vals {
					if n%2 == 0 {
						his.Observe(v)
					} else {
						his.(ExemplarObserver).ObserveWithExemplar(v, Labels{"foo": "bar"})
					}
				}
				end.Done()
			}(vals)
		}
		sort.Float64s(allVars)
		start.Done()
		end.Wait()

		m := &dto.Metric{}
		his.Write(m)
		if got, want := int(*m.Histogram.SampleCount), total; got != want {
			t.Errorf("got sample count %d, want %d", got, want)
		}
		if got, want := *m.Histogram.SampleSum, sampleSum; math.Abs((got-want)/want) > 0.001 {
			t.Errorf("got sample sum %f, want %f", got, want)
		}

		wantCounts := getCumulativeCounts(allVars)
		wantBuckets := len(testBuckets)
		if !math.IsInf(m.Histogram.Bucket[len(m.Histogram.Bucket)-1].GetUpperBound(), +1) {
			wantBuckets--
		}

		if got := len(m.Histogram.Bucket); got != wantBuckets {
			t.Errorf("got %d buckets in protobuf, want %d", got, wantBuckets)
		}
		for i, wantBound := range testBuckets {
			if i == len(testBuckets)-1 {
				break // No +Inf bucket in protobuf.
			}
			if gotBound := *m.Histogram.Bucket[i].UpperBound; gotBound != wantBound {
				t.Errorf("got bound %f, want %f", gotBound, wantBound)
			}
			if gotCount, wantCount := *m.Histogram.Bucket[i].CumulativeCount, wantCounts[i]; gotCount != wantCount {
				t.Errorf("got count %d, want %d", gotCount, wantCount)
			}
		}
		return true
	}

	if err := quick.Check(it, nil); err != nil {
		t.Error(err)
	}
}

func TestHistogramVecConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	rand.New(rand.NewSource(42))

	it := func(n uint32) bool {
		mutations := int(n%1e4 + 1e4)
		concLevel := int(n%7 + 1)
		vecLength := int(n%3 + 1)

		var start, end sync.WaitGroup
		start.Add(1)
		end.Add(concLevel)

		his := NewHistogramVec(
			HistogramOpts{
				Name:    "test_histogram",
				Help:    "helpless",
				Buckets: []float64{-2, -1, -0.5, 0, 0.5, 1, 2, math.Inf(+1)},
			},
			[]string{"label"},
		)

		allVars := make([][]float64, vecLength)
		sampleSums := make([]float64, vecLength)
		for range concLevel {
			vals := make([]float64, mutations)
			picks := make([]int, mutations)
			for j := range mutations {
				v := rand.NormFloat64()
				vals[j] = v
				pick := rand.Intn(vecLength)
				picks[j] = pick
				allVars[pick] = append(allVars[pick], v)
				sampleSums[pick] += v
			}

			go func(vals []float64) {
				start.Wait()
				for i, v := range vals {
					his.WithLabelValues(string('A' + rune(picks[i]))).Observe(v)
				}
				end.Done()
			}(vals)
		}
		for _, vars := range allVars {
			sort.Float64s(vars)
		}
		start.Done()
		end.Wait()

		for i := range vecLength {
			m := &dto.Metric{}
			s := his.WithLabelValues(string('A' + rune(i)))
			s.(Histogram).Write(m)

			if got, want := len(m.Histogram.Bucket), len(testBuckets)-1; got != want {
				t.Errorf("got %d buckets in protobuf, want %d", got, want)
			}
			if got, want := int(*m.Histogram.SampleCount), len(allVars[i]); got != want {
				t.Errorf("got sample count %d, want %d", got, want)
			}
			if got, want := *m.Histogram.SampleSum, sampleSums[i]; math.Abs((got-want)/want) > 0.001 {
				t.Errorf("got sample sum %f, want %f", got, want)
			}

			wantCounts := getCumulativeCounts(allVars[i])

			for j, wantBound := range testBuckets {
				if j == len(testBuckets)-1 {
					break // No +Inf bucket in protobuf.
				}
				if gotBound := *m.Histogram.Bucket[j].UpperBound; gotBound != wantBound {
					t.Errorf("got bound %f, want %f", gotBound, wantBound)
				}
				if gotCount, wantCount := *m.Histogram.Bucket[j].CumulativeCount, wantCounts[j]; gotCount != wantCount {
					t.Errorf("got count %d, want %d", gotCount, wantCount)
				}
			}
		}
		return true
	}

	if err := quick.Check(it, nil); err != nil {
		t.Error(err)
	}
}

func getCumulativeCounts(vars []float64) []uint64 {
	counts := make([]uint64, len(testBuckets))
	for _, v := range vars {
		for i := range slices.Backward(testBuckets) {
			if v > testBuckets[i] {
				break
			}
			counts[i]++
		}
	}
	return counts
}

func TestBuckets(t *testing.T) {
	got := LinearBuckets(-15, 5, 6)
	want := []float64{-15, -10, -5, 0, 5, 10}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("linear buckets: got %v, want %v", got, want)
	}

	got = ExponentialBuckets(100, 1.2, 3)
	want = []float64{100, 120, 144}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("exponential buckets: got %v, want %v", got, want)
	}

	got = ExponentialBucketsRange(1, 100, 10)
	want = []float64{
		1.0, 1.6681, 2.7825, 4.6415, 7.7426, 12.9154, 21.5443,
		35.9381, 59.9484, 100.0000,
	}
	const epsilon = 0.0001
	if !internal.AlmostEqualFloat64s(got, want, epsilon) {
		t.Errorf("exponential buckets range: got %v, want %v (epsilon %f)", got, want, epsilon)
	}
}

func TestHistogramAtomicObserve(t *testing.T) {
	var (
		quit = make(chan struct{})
		his  = NewHistogram(HistogramOpts{
			Buckets: []float64{0.5, 10, 20},
		})
	)

	defer func() { close(quit) }()

	observe := func() {
		for {
			select {
			case <-quit:
				return
			default:
				his.Observe(1)
				time.Sleep(time.Nanosecond)
			}
		}
	}

	go observe()
	go observe()
	go observe()

	for range 100 {
		m := &dto.Metric{}
		if err := his.Write(m); err != nil {
			t.Fatal("unexpected error writing histogram:", err)
		}
		h := m.GetHistogram()
		if h.GetSampleCount() != uint64(h.GetSampleSum()) ||
			h.GetSampleCount() != h.GetBucket()[1].GetCumulativeCount() ||
			h.GetSampleCount() != h.GetBucket()[2].GetCumulativeCount() {
			t.Fatalf(
				"inconsistent counts in histogram: count=%d sum=%f buckets=[%d, %d]",
				h.GetSampleCount(), h.GetSampleSum(),
				h.GetBucket()[1].GetCumulativeCount(), h.GetBucket()[2].GetCumulativeCount(),
			)
		}
		runtime.Gosched()
	}
}

func TestHistogramExemplar(t *testing.T) {
	now := time.Now()

	histogram := NewHistogram(HistogramOpts{
		Name:    "test",
		Help:    "test help",
		Buckets: []float64{1, 2, 3, 4},
		now:     func() time.Time { return now },
	}).(*histogram)

	ts := timestamppb.New(now)
	if err := ts.CheckValid(); err != nil {
		t.Fatal(err)
	}
	expectedExemplars := []*dto.Exemplar{
		nil,
		{
			Label: []*dto.LabelPair{
				{Name: new("id"), Value: new("2")},
			},
			Value:     new(1.6),
			Timestamp: ts,
		},
		nil,
		{
			Label: []*dto.LabelPair{
				{Name: new("id"), Value: new("3")},
			},
			Value:     proto.Float64(4),
			Timestamp: ts,
		},
		{
			Label: []*dto.LabelPair{
				{Name: new("id"), Value: new("4")},
			},
			Value:     new(4.5),
			Timestamp: ts,
		},
	}

	histogram.ObserveWithExemplar(1.5, Labels{"id": "1"})
	histogram.ObserveWithExemplar(1.6, Labels{"id": "2"}) // To replace exemplar in bucket 0.
	histogram.ObserveWithExemplar(4, Labels{"id": "3"})
	histogram.ObserveWithExemplar(4.5, Labels{"id": "4"}) // Should go to +Inf bucket.

	for i, ex := range histogram.exemplars {
		var got, expected string
		if val := ex.Load(); val != nil {
			got = val.(*dto.Exemplar).String()
		}
		if expectedExemplars[i] != nil {
			expected = expectedExemplars[i].String()
		}
		if got != expected {
			t.Errorf("expected exemplar %s, got %s.", expected, got)
		}
	}
}

func TestNativeHistogram(t *testing.T) {
	now := time.Now()

	scenarios := []struct {
		name             string
		observations     []float64 // With simulated interval of 1m.
		factor           float64
		zeroThreshold    float64
		maxBuckets       uint32
		minResetDuration time.Duration
		maxZeroThreshold float64
		want             *dto.Histogram
	}{
		{
			name:         "no sparse buckets",
			observations: []float64{1, 2, 3},
			factor:       1,
			want: &dto.Histogram{
				SampleCount: proto.Uint64(3),
				SampleSum:   proto.Float64(6),
				Bucket: []*dto.Bucket{
					{CumulativeCount: proto.Uint64(0), UpperBound: new(0.005)},
					{CumulativeCount: proto.Uint64(0), UpperBound: new(0.01)},
					{CumulativeCount: proto.Uint64(0), UpperBound: new(0.025)},
					{CumulativeCount: proto.Uint64(0), UpperBound: new(0.05)},
					{CumulativeCount: proto.Uint64(0), UpperBound: new(0.1)},
					{CumulativeCount: proto.Uint64(0), UpperBound: new(0.25)},
					{CumulativeCount: proto.Uint64(0), UpperBound: new(0.5)},
					{CumulativeCount: proto.Uint64(1), UpperBound: proto.Float64(1)},
					{CumulativeCount: proto.Uint64(2), UpperBound: new(2.5)},
					{CumulativeCount: proto.Uint64(3), UpperBound: proto.Float64(5)},
					{CumulativeCount: proto.Uint64(3), UpperBound: proto.Float64(10)},
				},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:   "no observations",
			factor: 1.1,
			want: &dto.Histogram{
				SampleCount:      proto.Uint64(0),
				SampleSum:        proto.Float64(0),
				Schema:           proto.Int32(3),
				ZeroThreshold:    new(2.938735877055719e-39),
				ZeroCount:        proto.Uint64(0),
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:          "no observations and zero threshold of zero resulting in no-op span",
			factor:        1.1,
			zeroThreshold: NativeHistogramZeroThresholdZero,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(0),
				SampleSum:     proto.Float64(0),
				Schema:        proto.Int32(3),
				ZeroThreshold: proto.Float64(0),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(0)},
				},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "factor 1.1 results in schema 3",
			observations: []float64{0, 1, 2, 3},
			factor:       1.1,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(4),
				SampleSum:     proto.Float64(6),
				Schema:        proto.Int32(3),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(1)},
					{Offset: proto.Int32(7), Length: proto.Uint32(1)},
					{Offset: proto.Int32(4), Length: proto.Uint32(1)},
				},
				PositiveDelta:    []int64{1, 0, 0},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "factor 1.2 results in schema 2",
			observations: []float64{0, 1, 1.2, 1.4, 1.8, 2},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(6),
				SampleSum:     new(7.4),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name: "factor 4 results in schema -1",
			observations: []float64{
				0.0156251, 0.0625, // Bucket -2: (0.015625, 0.0625)
				0.1, 0.25, // Bucket -1: (0.0625, 0.25]
				0.5, 1, // Bucket 0: (0.25, 1]
				1.5, 2, 3, 3.5, // Bucket 1: (1, 4]
				5, 6, 7, // Bucket 2: (4, 16]
				33.33, // Bucket 3: (16, 64]
			},
			factor: 4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(14),
				SampleSum:     new(63.2581251),
				Schema:        proto.Int32(-1),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(-2), Length: proto.Uint32(6)},
				},
				PositiveDelta:    []int64{2, 0, 0, 2, -1, -2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name: "factor 17 results in schema -2",
			observations: []float64{
				0.0156251, 0.0625, // Bucket -1: (0.015625, 0.0625]
				0.1, 0.25, 0.5, 1, // Bucket 0: (0.0625, 1]
				1.5, 2, 3, 3.5, 5, 6, 7, // Bucket 1: (1, 16]
				33.33, // Bucket 2: (16, 256]
			},
			factor: 17,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(14),
				SampleSum:     new(63.2581251),
				Schema:        proto.Int32(-2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(-1), Length: proto.Uint32(4)},
				},
				PositiveDelta:    []int64{2, 2, 3, -6},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "negative buckets",
			observations: []float64{0, -1, -1.2, -1.4, -1.8, -2},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(6),
				SampleSum:     new(-7.4),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				NegativeDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "negative and positive buckets",
			observations: []float64{0, -1, -1.2, -1.4, -1.8, -2, 1, 1.2, 1.4, 1.8, 2},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(11),
				SampleSum:     proto.Float64(0),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				NegativeDelta: []int64{1, -1, 2, -2, 2},
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:          "wide zero bucket",
			observations:  []float64{0, -1, -1.2, -1.4, -1.8, -2, 1, 1.2, 1.4, 1.8, 2},
			factor:        1.2,
			zeroThreshold: 1.4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(11),
				SampleSum:     proto.Float64(0),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(1.4),
				ZeroCount:     proto.Uint64(7),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(4), Length: proto.Uint32(1)},
				},
				NegativeDelta: []int64{2},
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(4), Length: proto.Uint32(1)},
				},
				PositiveDelta:    []int64{2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "NaN observation",
			observations: []float64{0, 1, 1.2, 1.4, 1.8, 2, math.NaN()},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(7),
				SampleSum:     new(math.NaN()),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "+Inf observation",
			observations: []float64{0, 1, 1.2, 1.4, 1.8, 2, math.Inf(+1)},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(7),
				SampleSum:     new(math.Inf(+1)),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
					{Offset: proto.Int32(4092), Length: proto.Uint32(1)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2, -1},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "-Inf observation",
			observations: []float64{0, 1, 1.2, 1.4, 1.8, 2, math.Inf(-1)},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(7),
				SampleSum:     new(math.Inf(-1)),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(4097), Length: proto.Uint32(1)},
				},
				NegativeDelta: []int64{1},
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "limited buckets but nothing triggered",
			observations: []float64{0, 1, 1.2, 1.4, 1.8, 2},
			factor:       1.2,
			maxBuckets:   4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(6),
				SampleSum:     new(7.4),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "buckets limited by halving resolution",
			observations: []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 3},
			factor:       1.2,
			maxBuckets:   4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(8),
				SampleSum:     new(11.5),
				Schema:        proto.Int32(1),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, 2, -1, -2, 1},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by widening the zero bucket",
			observations:     []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 3},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(8),
				SampleSum:     new(11.5),
				Schema:        proto.Int32(2),
				ZeroThreshold: proto.Float64(1),
				ZeroCount:     proto.Uint64(2),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(1), Length: proto.Uint32(7)},
				},
				PositiveDelta:    []int64{1, 1, -2, 2, -2, 0, 1},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by widening the zero bucket twice",
			observations:     []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 3, 4},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(9),
				SampleSum:     new(15.5),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(1.189207115002721),
				ZeroCount:     proto.Uint64(3),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(2), Length: proto.Uint32(7)},
				},
				PositiveDelta:    []int64{2, -2, 2, -2, 0, 1, 0},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by reset",
			observations:     []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 3, 4},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			minResetDuration: 5 * time.Minute,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(2),
				SampleSum:     proto.Float64(7),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(7), Length: proto.Uint32(2)},
				},
				PositiveDelta:    []int64{1, 0},
				CreatedTimestamp: timestamppb.New(now.Add(8 * time.Minute)), // We expect reset to happen after 8 observations.
			},
		},
		{
			name:         "limited buckets but nothing triggered, negative observations",
			observations: []float64{0, -1, -1.2, -1.4, -1.8, -2},
			factor:       1.2,
			maxBuckets:   4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(6),
				SampleSum:     new(-7.4),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				NegativeDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "buckets limited by halving resolution, negative observations",
			observations: []float64{0, -1, -1.1, -1.2, -1.4, -1.8, -2, -3},
			factor:       1.2,
			maxBuckets:   4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(8),
				SampleSum:     new(-11.5),
				Schema:        proto.Int32(1),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				NegativeDelta:    []int64{1, 2, -1, -2, 1},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by widening the zero bucket, negative observations",
			observations:     []float64{0, -1, -1.1, -1.2, -1.4, -1.8, -2, -3},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(8),
				SampleSum:     new(-11.5),
				Schema:        proto.Int32(2),
				ZeroThreshold: proto.Float64(1),
				ZeroCount:     proto.Uint64(2),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(1), Length: proto.Uint32(7)},
				},
				NegativeDelta:    []int64{1, 1, -2, 2, -2, 0, 1},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by widening the zero bucket twice, negative observations",
			observations:     []float64{0, -1, -1.1, -1.2, -1.4, -1.8, -2, -3, -4},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(9),
				SampleSum:     new(-15.5),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(1.189207115002721),
				ZeroCount:     proto.Uint64(3),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(2), Length: proto.Uint32(7)},
				},
				NegativeDelta:    []int64{2, -2, 2, -2, 0, 1, 0},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by reset, negative observations",
			observations:     []float64{0, -1, -1.1, -1.2, -1.4, -1.8, -2, -3, -4},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			minResetDuration: 5 * time.Minute,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(2),
				SampleSum:     proto.Float64(-7),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(7), Length: proto.Uint32(2)},
				},
				NegativeDelta:    []int64{1, 0},
				CreatedTimestamp: timestamppb.New(now.Add(8 * time.Minute)), // We expect reset to happen after 8 observations.
			},
		},
		{
			name:             "buckets limited by halving resolution, then reset",
			observations:     []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 5, 5.1, 3, 4},
			factor:           1.2,
			maxBuckets:       4,
			minResetDuration: 9 * time.Minute,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(3),
				SampleSum:     new(12.1),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(7), Length: proto.Uint32(4)},
				},
				PositiveDelta:    []int64{1, 0, -1, 1},
				CreatedTimestamp: timestamppb.New(now.Add(9 * time.Minute)), // We expect reset to happen after 8 minutes.
			},
		},
		{
			name:             "buckets limited by widening the zero bucket, then reset",
			observations:     []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 5, 5.1, 3, 4},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			minResetDuration: 9 * time.Minute,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(3),
				SampleSum:     new(12.1),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(7), Length: proto.Uint32(4)},
				},
				PositiveDelta:    []int64{1, 0, -1, 1},
				CreatedTimestamp: timestamppb.New(now.Add(9 * time.Minute)), // We expect reset to happen after 8 minutes.
			},
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			var (
				ts         = now
				funcToCall func()
				whenToCall time.Duration
			)

			his := NewHistogram(HistogramOpts{
				Name:                            "name",
				Help:                            "help",
				NativeHistogramBucketFactor:     s.factor,
				NativeHistogramZeroThreshold:    s.zeroThreshold,
				NativeHistogramMaxBucketNumber:  s.maxBuckets,
				NativeHistogramMinResetDuration: s.minResetDuration,
				NativeHistogramMaxZeroThreshold: s.maxZeroThreshold,
				now:                             func() time.Time { return ts },
				afterFunc: func(d time.Duration, f func()) *time.Timer {
					funcToCall = f
					whenToCall = d
					return nil
				},
			})

			ts = ts.Add(time.Minute)
			for _, o := range s.observations {
				his.Observe(o)
				ts = ts.Add(time.Minute)
				whenToCall -= time.Minute
				if funcToCall != nil && whenToCall <= 0 {
					funcToCall()
					funcToCall = nil
				}
			}
			m := &dto.Metric{}
			if err := his.Write(m); err != nil {
				t.Fatal("unexpected error writing metric", err)
			}
			got := m.Histogram
			if !proto.Equal(s.want, got) {
				t.Errorf("want histogram %q, got %q", s.want, got)
			}
		})
	}
}

func TestNativeHistogramConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	rand.New(rand.NewSource(42))

	it := func(n uint32) bool {
		ts := time.Now().Add(30 * time.Second).Unix()

		mutations := int(n%1e4 + 1e4)
		concLevel := int(n%5 + 1)
		total := mutations * concLevel

		var start, end sync.WaitGroup
		start.Add(1)
		end.Add(concLevel)

		his := NewHistogram(HistogramOpts{
			Name:                            "test_native_histogram",
			Help:                            "This help is sparse.",
			NativeHistogramBucketFactor:     1.05,
			NativeHistogramZeroThreshold:    0.0000001,
			NativeHistogramMaxBucketNumber:  50,
			NativeHistogramMinResetDuration: time.Hour, // Comment out to test for totals below.
			NativeHistogramMaxZeroThreshold: 0.001,
			now: func() time.Time {
				return time.Unix(atomic.LoadInt64(&ts), 0)
			},
		})

		allVars := make([]float64, total)
		var sampleSum float64
		for i := range concLevel {
			vals := make([]float64, mutations)
			for j := range mutations {
				v := rand.NormFloat64()
				vals[j] = v
				allVars[i*mutations+j] = v
				sampleSum += v
			}

			go func(vals []float64) {
				start.Wait()
				for i, v := range vals {
					// An observation every 1 to 10 seconds.
					atomic.AddInt64(&ts, rand.Int63n(10)+1)
					if i%2 == 0 {
						his.Observe(v)
					} else {
						his.(ExemplarObserver).ObserveWithExemplar(v, Labels{"foo": "bar"})
					}
				}
				end.Done()
			}(vals)
		}
		sort.Float64s(allVars)
		start.Done()
		end.Wait()

		m := &dto.Metric{}
		his.Write(m)

		// Uncomment these tests for totals only if you have disabled histogram resets above.
		//
		// if got, want := int(*m.Histogram.SampleCount), total; got != want {
		// 	t.Errorf("got sample count %d, want %d", got, want)
		// }
		// if got, want := *m.Histogram.SampleSum, sampleSum; math.Abs((got-want)/want) > 0.001 {
		// 	t.Errorf("got sample sum %f, want %f", got, want)
		// }

		sumBuckets := int(m.Histogram.GetZeroCount())
		current := 0
		for _, delta := range m.Histogram.GetNegativeDelta() {
			current += int(delta)
			if current < 0 {
				t.Fatalf("negative bucket population negative: %d", current)
			}
			sumBuckets += current
		}
		current = 0
		for _, delta := range m.Histogram.GetPositiveDelta() {
			current += int(delta)
			if current < 0 {
				t.Fatalf("positive bucket population negative: %d", current)
			}
			sumBuckets += current
		}
		if got, want := sumBuckets, int(*m.Histogram.SampleCount); got != want {
			t.Errorf("got bucket population sum %d, want %d", got, want)
		}

		return true
	}

	if err := quick.Check(it, nil); err != nil {
		t.Error(err)
	}
}

func TestGetLe(t *testing.T) {
	scenarios := []struct {
		key    int
		schema int32
		want   float64
	}{
		{
			key:    -1,
			schema: -1,
			want:   0.25,
		},
		{
			key:    0,
			schema: -1,
			want:   1,
		},
		{
			key:    1,
			schema: -1,
			want:   4,
		},
		{
			key:    512,
			schema: -1,
			want:   math.MaxFloat64,
		},
		{
			key:    513,
			schema: -1,
			want:   math.Inf(+1),
		},
		{
			key:    -1,
			schema: 0,
			want:   0.5,
		},
		{
			key:    0,
			schema: 0,
			want:   1,
		},
		{
			key:    1,
			schema: 0,
			want:   2,
		},
		{
			key:    1024,
			schema: 0,
			want:   math.MaxFloat64,
		},
		{
			key:    1025,
			schema: 0,
			want:   math.Inf(+1),
		},
		{
			key:    -1,
			schema: 2,
			want:   0.8408964152537144,
		},
		{
			key:    0,
			schema: 2,
			want:   1,
		},
		{
			key:    1,
			schema: 2,
			want:   1.189207115002721,
		},
		{
			key:    4096,
			schema: 2,
			want:   math.MaxFloat64,
		},
		{
			key:    4097,
			schema: 2,
			want:   math.Inf(+1),
		},
	}

	for i, s := range scenarios {
		got := getLe(s.key, s.schema)
		if s.want != got {
			t.Errorf("%d. key %d, schema %d, want upper bound of %g, got %g", i, s.key, s.schema, s.want, got)
		}
	}
}

func TestHistogramCreatedTimestamp(t *testing.T) {
	now := time.Now()

	histogram := NewHistogram(HistogramOpts{
		Name:    "test",
		Help:    "test help",
		Buckets: []float64{1, 2, 3, 4},
		now:     func() time.Time { return now },
	})

	var metric dto.Metric
	if err := histogram.Write(&metric); err != nil {
		t.Fatal(err)
	}

	if metric.Histogram.CreatedTimestamp.AsTime().Unix() != now.Unix() {
		t.Errorf("expected created timestamp %d, got %d", now.Unix(), metric.Histogram.CreatedTimestamp.AsTime().Unix())
	}
}

func TestHistogramVecCreatedTimestamp(t *testing.T) {
	now := time.Now()

	histogramVec := NewHistogramVec(HistogramOpts{
		Name:    "test",
		Help:    "test help",
		Buckets: []float64{1, 2, 3, 4},
		now:     func() time.Time { return now },
	}, []string{"label"})
	histogram := histogramVec.WithLabelValues("value").(Histogram)

	var metric dto.Metric
	if err := histogram.Write(&metric); err != nil {
		t.Fatal(err)
	}

	if metric.Histogram.CreatedTimestamp.AsTime().Unix() != now.Unix() {
		t.Errorf("expected created timestamp %d, got %d", now.Unix(), metric.Histogram.CreatedTimestamp.AsTime().Unix())
	}
}

func TestHistogramVecCreatedTimestampWithDeletes(t *testing.T) {
	now := time.Now()

	histogramVec := NewHistogramVec(HistogramOpts{
		Name:    "test",
		Help:    "test help",
		Buckets: []float64{1, 2, 3, 4},
		now:     func() time.Time { return now },
	}, []string{"label"})

	// First use of "With" should populate CT.
	histogramVec.WithLabelValues("1")
	expected := map[string]time.Time{"1": now}

	now = now.Add(1 * time.Hour)
	expectCTsForMetricVecValues(t, histogramVec.MetricVec, dto.MetricType_HISTOGRAM, expected)

	// Two more labels at different times.
	histogramVec.WithLabelValues("2")
	expected["2"] = now

	now = now.Add(1 * time.Hour)

	histogramVec.WithLabelValues("3")
	expected["3"] = now

	now = now.Add(1 * time.Hour)
	expectCTsForMetricVecValues(t, histogramVec.MetricVec, dto.MetricType_HISTOGRAM, expected)

	// Recreate metric instance should reset created timestamp to now.
	histogramVec.DeleteLabelValues("1")
	histogramVec.WithLabelValues("1")
	expected["1"] = now

	now = now.Add(1 * time.Hour)
	expectCTsForMetricVecValues(t, histogramVec.MetricVec, dto.MetricType_HISTOGRAM, expected)
}

func TestNewConstHistogramWithCreatedTimestamp(t *testing.T) {
	metricDesc := NewDesc(
		"sample_value",
		"sample value",
		nil,
		nil,
	)
	buckets := map[float64]uint64{25: 100, 50: 200}
	createdTs := time.Unix(1719670764, 123)

	h, err := NewConstHistogramWithCreatedTimestamp(metricDesc, 100, 200, buckets, createdTs)
	if err != nil {
		t.Fatal(err)
	}

	var metric dto.Metric
	if err := h.Write(&metric); err != nil {
		t.Fatal(err)
	}

	if metric.Histogram.CreatedTimestamp.AsTime().UnixMicro() != createdTs.UnixMicro() {
		t.Errorf("Expected created timestamp %v, got %v", createdTs, &metric.Histogram.CreatedTimestamp)
	}
}

func TestNativeHistogramExemplar(t *testing.T) {
	// Test the histogram with positive NativeHistogramExemplarTTL and NativeHistogramMaxExemplars
	h := NewHistogram(HistogramOpts{
		Name:                        "test",
		Help:                        "test help",
		Buckets:                     []float64{1, 2, 3, 4},
		NativeHistogramBucketFactor: 1.1,
		NativeHistogramMaxExemplars: 3,
		NativeHistogramExemplarTTL:  10 * time.Second,
	}).(*histogram)

	tcs := []struct {
		name           string
		addFunc        func(*histogram)
		expectedValues []float64
	}{
		{
			name: "add exemplars to the limit",
			addFunc: func(h *histogram) {
				// Set explicit mock timestamps so the "oldest" ordering is
				// deterministic on Windows, where time.Now() has ~15ms
				// resolution and rapid calls may return the same value.
				// h.now is intentionally left set to base+2ms so subsequent
				// closest-pair subcases (which don't depend on TTL) inherit a
				// stable timestamp without triggering the 10s TTL check.
				base := time.Now()
				h.now = func() time.Time { return base }
				h.ObserveWithExemplar(1, Labels{"id": "1"})
				h.now = func() time.Time { return base.Add(time.Millisecond) }
				h.ObserveWithExemplar(3, Labels{"id": "1"})
				h.now = func() time.Time { return base.Add(2 * time.Millisecond) }
				h.ObserveWithExemplar(5, Labels{"id": "1"})
			},
			expectedValues: []float64{1, 3, 5},
		},
		{
			name: "remove exemplar in closest pair, the removed index equals to inserted index",
			addFunc: func(h *histogram) {
				h.ObserveWithExemplar(4, Labels{"id": "1"})
			},
			expectedValues: []float64{1, 3, 4},
		},
		{
			name: "remove exemplar in closest pair, the removed index is bigger than inserted index",
			addFunc: func(h *histogram) {
				h.ObserveWithExemplar(0, Labels{"id": "1"})
			},
			expectedValues: []float64{0, 1, 4},
		},
		{
			name: "remove exemplar with oldest timestamp, the removed index is smaller than inserted index",
			addFunc: func(h *histogram) {
				h.now = func() time.Time { return time.Now().Add(time.Second * 11) }
				h.ObserveWithExemplar(6, Labels{"id": "1"})
			},
			expectedValues: []float64{0, 4, 6},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc.addFunc(h)
			compareNativeExemplarValues(t, h.nativeExemplars.exemplars, tc.expectedValues)
		})
	}

	// Test the histogram with negative NativeHistogramExemplarTTL
	h = NewHistogram(HistogramOpts{
		Name:                        "test",
		Help:                        "test help",
		Buckets:                     []float64{1, 2, 3, 4},
		NativeHistogramBucketFactor: 1.1,
		NativeHistogramMaxExemplars: 3,
		NativeHistogramExemplarTTL:  -1 * time.Second,
	}).(*histogram)

	tcs = []struct {
		name           string
		addFunc        func(*histogram)
		expectedValues []float64
	}{
		{
			name: "add exemplars to the limit",
			addFunc: func(h *histogram) {
				// Set explicit mock timestamps so the "oldest" ordering is
				// deterministic on Windows, where time.Now() has ~15ms
				// resolution and rapid calls may return the same value.
				base := time.Now()
				h.now = func() time.Time { return base }
				h.ObserveWithExemplar(1, Labels{"id": "1"})
				h.now = func() time.Time { return base.Add(time.Millisecond) }
				h.ObserveWithExemplar(3, Labels{"id": "1"})
				h.now = func() time.Time { return base.Add(2 * time.Millisecond) }
				h.ObserveWithExemplar(5, Labels{"id": "1"})
			},
			expectedValues: []float64{1, 3, 5},
		},
		{
			name: "remove exemplar with oldest timestamp, the removed index is smaller than inserted index",
			addFunc: func(h *histogram) {
				h.now = func() time.Time { return time.Now().Add(time.Second) }
				h.ObserveWithExemplar(4, Labels{"id": "1"})
			},
			expectedValues: []float64{3, 4, 5},
		},
		{
			name: "remove exemplar with oldest timestamp, the removed index equals to inserted index",
			addFunc: func(h *histogram) {
				h.now = func() time.Time { return time.Now().Add(2 * time.Second) }
				h.ObserveWithExemplar(0, Labels{"id": "1"})
			},
			expectedValues: []float64{0, 4, 5},
		},
		{
			name: "remove exemplar with oldest timestamp, the removed index is bigger than inserted index",
			addFunc: func(h *histogram) {
				h.now = func() time.Time { return time.Now().Add(3 * time.Second) }
				h.ObserveWithExemplar(3, Labels{"id": "1"})
			},
			expectedValues: []float64{0, 3, 4},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc.addFunc(h)
			compareNativeExemplarValues(t, h.nativeExemplars.exemplars, tc.expectedValues)
		})
	}

	// Test the histogram with negative NativeHistogramMaxExemplars
	h = NewHistogram(HistogramOpts{
		Name:                        "test",
		Help:                        "test help",
		Buckets:                     []float64{1, 2, 3, 4},
		NativeHistogramBucketFactor: 1.1,
		NativeHistogramMaxExemplars: -1,
		NativeHistogramExemplarTTL:  -1 * time.Second,
	}).(*histogram)

	tcs = []struct {
		name           string
		addFunc        func(*histogram)
		expectedValues []float64
	}{
		{
			name: "add exemplars to the limit, but no effect",
			addFunc: func(h *histogram) {
				h.ObserveWithExemplar(1, Labels{"id": "1"})
				h.ObserveWithExemplar(3, Labels{"id": "1"})
				h.ObserveWithExemplar(5, Labels{"id": "1"})
			},
			expectedValues: []float64{},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc.addFunc(h)
			compareNativeExemplarValues(t, h.nativeExemplars.exemplars, tc.expectedValues)
		})
	}
}

func compareNativeExemplarValues(t *testing.T, exps []*dto.Exemplar, values []float64) {
	if len(exps) != len(values) {
		t.Errorf("the count of exemplars is not %d", len(values))
	}
	for i, e := range exps {
		if e.GetValue() != values[i] {
			t.Errorf("the %dth exemplar value %v is not as expected: %v", i, e.GetValue(), values[i])
		}
	}
}

var resultFindBucket int

func benchmarkFindBucket(b *testing.B, l int) {
	h := &histogram{upperBounds: make([]float64, l)}
	for i := range h.upperBounds {
		h.upperBounds[i] = float64(i)
	}
	v := float64(l / 2)

	for b.Loop() {
		resultFindBucket = h.findBucket(v)
	}
}

func BenchmarkFindBucketShort(b *testing.B) {
	benchmarkFindBucket(b, 20)
}

func BenchmarkFindBucketMid(b *testing.B) {
	benchmarkFindBucket(b, 40)
}

func BenchmarkFindBucketLarge(b *testing.B) {
	benchmarkFindBucket(b, 100)
}

func BenchmarkFindBucketHuge(b *testing.B) {
	benchmarkFindBucket(b, 500)
}

func BenchmarkFindBucketInf(b *testing.B) {
	h := &histogram{upperBounds: make([]float64, 500)}
	for i := range h.upperBounds {
		h.upperBounds[i] = float64(i)
	}
	v := 1000.5

	for b.Loop() {
		resultFindBucket = h.findBucket(v)
	}
}

func BenchmarkFindBucketLow(b *testing.B) {
	h := &histogram{upperBounds: make([]float64, 500)}
	for i := range h.upperBounds {
		h.upperBounds[i] = float64(i)
	}
	v := -1.1

	for b.Loop() {
		resultFindBucket = h.findBucket(v)
	}
}

func TestFindBucket(t *testing.T) {
	smallHistogram := &histogram{upperBounds: []float64{1, 2, 3, 4, 5}}
	largeHistogram := &histogram{upperBounds: make([]float64, 50)}
	for i := range largeHistogram.upperBounds {
		largeHistogram.upperBounds[i] = float64(i)
	}

	tests := []struct {
		h        *histogram
		v        float64
		expected int
	}{
		{smallHistogram, -1, 0},
		{smallHistogram, 0.5, 0},
		{smallHistogram, 2.5, 2},
		{smallHistogram, 5.5, 5},
		{largeHistogram, -1, 0},
		{largeHistogram, 25.5, 26},
		{largeHistogram, 49.5, 50},
		{largeHistogram, 50.5, 50},
		{largeHistogram, 5000.5, 50},
	}

	for _, tt := range tests {
		result := tt.h.findBucket(tt.v)
		if result != tt.expected {
			t.Errorf("findBucket(%v) = %d; expected %d", tt.v, result, tt.expected)
		}
	}
}

func syncMapToMap(syncmap *sync.Map) (m map[int]int64) {
	m = map[int]int64{}
	syncmap.Range(func(key, value any) bool {
		m[key.(int)] = *value.(*int64)
		return true
	})
	return m
}

func TestConstNativeHistogram(t *testing.T) {
	now := time.Now()

	scenarios := []struct {
		name             string
		observations     []float64 // With simulated interval of 1m.
		factor           float64
		zeroThreshold    float64
		maxBuckets       uint32
		minResetDuration time.Duration
		maxZeroThreshold float64
		want             *dto.Histogram
	}{
		{
			name:   "no observations",
			factor: 1.1,
			want: &dto.Histogram{
				SampleCount:      proto.Uint64(0),
				SampleSum:        proto.Float64(0),
				Schema:           proto.Int32(3),
				ZeroThreshold:    new(2.938735877055719e-39),
				ZeroCount:        proto.Uint64(0),
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:          "no observations and zero threshold of zero resulting in no-op span",
			factor:        1.1,
			zeroThreshold: NativeHistogramZeroThresholdZero,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(0),
				SampleSum:     proto.Float64(0),
				Schema:        proto.Int32(3),
				ZeroThreshold: proto.Float64(0),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(0)},
				},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "factor 1.1 results in schema 3",
			observations: []float64{0, 1, 2, 3},
			factor:       1.1,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(4),
				SampleSum:     proto.Float64(6),
				Schema:        proto.Int32(3),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(1)},
					{Offset: proto.Int32(7), Length: proto.Uint32(1)},
					{Offset: proto.Int32(4), Length: proto.Uint32(1)},
				},
				PositiveDelta:    []int64{1, 0, 0},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "factor 1.2 results in schema 2",
			observations: []float64{0, 1, 1.2, 1.4, 1.8, 2},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(6),
				SampleSum:     new(7.4),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name: "factor 4 results in schema -1",
			observations: []float64{
				0.0156251, 0.0625, // Bucket -2: (0.015625, 0.0625)
				0.1, 0.25, // Bucket -1: (0.0625, 0.25]
				0.5, 1, // Bucket 0: (0.25, 1]
				1.5, 2, 3, 3.5, // Bucket 1: (1, 4]
				5, 6, 7, // Bucket 2: (4, 16]
				33.33, // Bucket 3: (16, 64]
			},
			factor: 4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(14),
				SampleSum:     new(63.2581251),
				Schema:        proto.Int32(-1),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(-2), Length: proto.Uint32(6)},
				},
				PositiveDelta:    []int64{2, 0, 0, 2, -1, -2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name: "factor 17 results in schema -2",
			observations: []float64{
				0.0156251, 0.0625, // Bucket -1: (0.015625, 0.0625]
				0.1, 0.25, 0.5, 1, // Bucket 0: (0.0625, 1]
				1.5, 2, 3, 3.5, 5, 6, 7, // Bucket 1: (1, 16]
				33.33, // Bucket 2: (16, 256]
			},
			factor: 17,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(14),
				SampleSum:     new(63.2581251),
				Schema:        proto.Int32(-2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(-1), Length: proto.Uint32(4)},
				},
				PositiveDelta:    []int64{2, 2, 3, -6},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "negative buckets",
			observations: []float64{0, -1, -1.2, -1.4, -1.8, -2},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(6),
				SampleSum:     new(-7.4),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				NegativeDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "negative and positive buckets",
			observations: []float64{0, -1, -1.2, -1.4, -1.8, -2, 1, 1.2, 1.4, 1.8, 2},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(11),
				SampleSum:     proto.Float64(0),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				NegativeDelta: []int64{1, -1, 2, -2, 2},
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:          "wide zero bucket",
			observations:  []float64{0, -1, -1.2, -1.4, -1.8, -2, 1, 1.2, 1.4, 1.8, 2},
			factor:        1.2,
			zeroThreshold: 1.4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(11),
				SampleSum:     proto.Float64(0),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(1.4),
				ZeroCount:     proto.Uint64(7),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(4), Length: proto.Uint32(1)},
				},
				NegativeDelta: []int64{2},
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(4), Length: proto.Uint32(1)},
				},
				PositiveDelta:    []int64{2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "NaN observation",
			observations: []float64{0, 1, 1.2, 1.4, 1.8, 2, math.NaN()},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(7),
				SampleSum:     new(math.NaN()),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "+Inf observation",
			observations: []float64{0, 1, 1.2, 1.4, 1.8, 2, math.Inf(+1)},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(7),
				SampleSum:     new(math.Inf(+1)),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
					{Offset: proto.Int32(4092), Length: proto.Uint32(1)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2, -1},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "-Inf observation",
			observations: []float64{0, 1, 1.2, 1.4, 1.8, 2, math.Inf(-1)},
			factor:       1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(7),
				SampleSum:     new(math.Inf(-1)),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(4097), Length: proto.Uint32(1)},
				},
				NegativeDelta: []int64{1},
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "limited buckets but nothing triggered",
			observations: []float64{0, 1, 1.2, 1.4, 1.8, 2},
			factor:       1.2,
			maxBuckets:   4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(6),
				SampleSum:     new(7.4),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "buckets limited by halving resolution",
			observations: []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 3},
			factor:       1.2,
			maxBuckets:   4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(8),
				SampleSum:     new(11.5),
				Schema:        proto.Int32(1),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				PositiveDelta:    []int64{1, 2, -1, -2, 1},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by widening the zero bucket",
			observations:     []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 3},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(8),
				SampleSum:     new(11.5),
				Schema:        proto.Int32(2),
				ZeroThreshold: proto.Float64(1),
				ZeroCount:     proto.Uint64(2),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(1), Length: proto.Uint32(7)},
				},
				PositiveDelta:    []int64{1, 1, -2, 2, -2, 0, 1},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by widening the zero bucket twice",
			observations:     []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 3, 4},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(9),
				SampleSum:     new(15.5),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(1.189207115002721),
				ZeroCount:     proto.Uint64(3),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(2), Length: proto.Uint32(7)},
				},
				PositiveDelta:    []int64{2, -2, 2, -2, 0, 1, 0},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by reset",
			observations:     []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 3, 4},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			minResetDuration: 5 * time.Minute,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(2),
				SampleSum:     proto.Float64(7),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(7), Length: proto.Uint32(2)},
				},
				PositiveDelta:    []int64{1, 0},
				CreatedTimestamp: timestamppb.New(now.Add(8 * time.Minute)), // We expect reset to happen after 8 observations.
			},
		},
		{
			name:         "limited buckets but nothing triggered, negative observations",
			observations: []float64{0, -1, -1.2, -1.4, -1.8, -2},
			factor:       1.2,
			maxBuckets:   4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(6),
				SampleSum:     new(-7.4),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				NegativeDelta:    []int64{1, -1, 2, -2, 2},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:         "buckets limited by halving resolution, negative observations",
			observations: []float64{0, -1, -1.1, -1.2, -1.4, -1.8, -2, -3},
			factor:       1.2,
			maxBuckets:   4,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(8),
				SampleSum:     new(-11.5),
				Schema:        proto.Int32(1),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(1),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(0), Length: proto.Uint32(5)},
				},
				NegativeDelta:    []int64{1, 2, -1, -2, 1},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by widening the zero bucket, negative observations",
			observations:     []float64{0, -1, -1.1, -1.2, -1.4, -1.8, -2, -3},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(8),
				SampleSum:     new(-11.5),
				Schema:        proto.Int32(2),
				ZeroThreshold: proto.Float64(1),
				ZeroCount:     proto.Uint64(2),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(1), Length: proto.Uint32(7)},
				},
				NegativeDelta:    []int64{1, 1, -2, 2, -2, 0, 1},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by widening the zero bucket twice, negative observations",
			observations:     []float64{0, -1, -1.1, -1.2, -1.4, -1.8, -2, -3, -4},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(9),
				SampleSum:     new(-15.5),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(1.189207115002721),
				ZeroCount:     proto.Uint64(3),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(2), Length: proto.Uint32(7)},
				},
				NegativeDelta:    []int64{2, -2, 2, -2, 0, 1, 0},
				CreatedTimestamp: timestamppb.New(now),
			},
		},
		{
			name:             "buckets limited by reset, negative observations",
			observations:     []float64{0, -1, -1.1, -1.2, -1.4, -1.8, -2, -3, -4},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			minResetDuration: 5 * time.Minute,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(2),
				SampleSum:     proto.Float64(-7),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				NegativeSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(7), Length: proto.Uint32(2)},
				},
				NegativeDelta:    []int64{1, 0},
				CreatedTimestamp: timestamppb.New(now.Add(8 * time.Minute)), // We expect reset to happen after 8 observations.
			},
		},
		{
			name:             "buckets limited by halving resolution, then reset",
			observations:     []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 5, 5.1, 3, 4},
			factor:           1.2,
			maxBuckets:       4,
			minResetDuration: 9 * time.Minute,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(3),
				SampleSum:     new(12.1),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(7), Length: proto.Uint32(4)},
				},
				PositiveDelta:    []int64{1, 0, -1, 1},
				CreatedTimestamp: timestamppb.New(now.Add(9 * time.Minute)), // We expect reset to happen after 8 minutes.
			},
		},
		{
			name:             "buckets limited by widening the zero bucket, then reset",
			observations:     []float64{0, 1, 1.1, 1.2, 1.4, 1.8, 2, 5, 5.1, 3, 4},
			factor:           1.2,
			maxBuckets:       4,
			maxZeroThreshold: 1.2,
			minResetDuration: 9 * time.Minute,
			want: &dto.Histogram{
				SampleCount:   proto.Uint64(3),
				SampleSum:     new(12.1),
				Schema:        proto.Int32(2),
				ZeroThreshold: new(2.938735877055719e-39),
				ZeroCount:     proto.Uint64(0),
				PositiveSpan: []*dto.BucketSpan{
					{Offset: proto.Int32(7), Length: proto.Uint32(4)},
				},
				PositiveDelta:    []int64{1, 0, -1, 1},
				CreatedTimestamp: timestamppb.New(now.Add(9 * time.Minute)), // We expect reset to happen after 8 minutes.
			},
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			var (
				ts         = now
				funcToCall func()
				whenToCall time.Duration
			)

			his := NewHistogram(HistogramOpts{
				Name:                            "name",
				Help:                            "help",
				NativeHistogramBucketFactor:     s.factor,
				NativeHistogramZeroThreshold:    s.zeroThreshold,
				NativeHistogramMaxBucketNumber:  s.maxBuckets,
				NativeHistogramMinResetDuration: s.minResetDuration,
				NativeHistogramMaxZeroThreshold: s.maxZeroThreshold,
				now:                             func() time.Time { return ts },
				afterFunc: func(d time.Duration, f func()) *time.Timer {
					funcToCall = f
					whenToCall = d
					return nil
				},
			})

			ts = ts.Add(time.Minute)
			for _, o := range s.observations {
				his.Observe(o)
				ts = ts.Add(time.Minute)
				whenToCall -= time.Minute
				if funcToCall != nil && whenToCall <= 0 {
					funcToCall()
					funcToCall = nil
				}
			}
			_his := his.(*histogram)
			n := atomic.LoadUint64(&_his.countAndHotIdx)
			hotIdx := n >> 63
			cold := _his.counts[hotIdx]
			consthist, err := NewConstNativeHistogram(_his.Desc(),
				cold.count,
				math.Float64frombits(cold.sumBits),
				syncMapToMap(&cold.nativeHistogramBucketsPositive),
				syncMapToMap(&cold.nativeHistogramBucketsNegative),
				cold.nativeHistogramZeroBucket,
				cold.nativeHistogramSchema,
				math.Float64frombits(cold.nativeHistogramZeroThresholdBits),
				_his.lastResetTime,
			)
			if err != nil {
				t.Fatal("unexpected error writing metric", err)
			}
			m2 := &dto.Metric{}

			if err := consthist.Write(m2); err != nil {
				t.Fatal("unexpected error writing metric", err)
			}
			got := m2.Histogram
			if !proto.Equal(s.want, got) {
				t.Errorf("want histogram %q, got %q", s.want, got)
			}
		})
	}
}

// TestHistogramWritePanicCausesWaitForCooldownDeadlock is a regression test for
// https://github.com/prometheus/client_golang/issues/2147.
func TestHistogramWritePanicCausesWaitForCooldownDeadlock(t *testing.T) {
	h := NewHistogram(HistogramOpts{
		Name:                        "test_request_duration_seconds",
		Help:                        "Request duration in seconds.",
		NativeHistogramBucketFactor: 1.1,
	}).(*histogram)
	h.Observe(0.5)

	// Trigger a panic inside Write after flipping countAndHotIdx (e.g. out == nil,
	// which panics at `out.Histogram = his`). Because cold counts and native
	// buckets are merged via defer right after waitForCooldown, counts[0] is
	// still merged into counts[1] before `defer h.mtx.Unlock()` runs.
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic from Write(nil)")
			}
		}()
		_ = h.Write(nil)
	}()

	writeDone := make(chan error, 1)
	var out dto.Metric
	go func() {
		writeDone <- h.Write(&out)
	}()

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("unexpected Write error: %v", err)
		}
		if got := out.GetHistogram().GetSampleCount(); got != 1 {
			t.Fatalf("expected sample count 1, got %d", got)
		}
		if got := out.GetHistogram().GetPositiveDelta(); len(got) != 1 || got[0] != 1 {
			t.Fatalf("expected positive delta [1], got %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Write timed out spinning in waitForCooldown")
	}
}

// validateNativeHistogramSnapshot is the test-side counterpart of the
// invariants enforced by validateNativeHistogramEncoding. It is deliberately
// implemented independently (decoding the deltas again from scratch) so that a
// bug in the production check cannot hide a bad snapshot from the tests.
func validateNativeHistogramSnapshot(h *dto.Histogram) error {
	if h.Schema == nil {
		return nil
	}
	if schema := h.GetSchema(); schema < nativeHistogramSchemaMinimum || schema > nativeHistogramSchemaMaximum {
		return fmt.Errorf("schema %d out of range", schema)
	}
	if threshold := h.GetZeroThreshold(); math.IsNaN(threshold) || math.IsInf(threshold, +1) || threshold < 0 {
		return fmt.Errorf("invalid zero threshold %v", threshold)
	}
	var population uint64
	for _, side := range []struct {
		name   string
		spans  []*dto.BucketSpan
		deltas []int64
	}{
		{"positive", h.GetPositiveSpan(), h.GetPositiveDelta()},
		{"negative", h.GetNegativeSpan(), h.GetNegativeDelta()},
	} {
		var spanLength uint64
		for _, span := range side.spans {
			spanLength += uint64(span.GetLength())
		}
		if spanLength != uint64(len(side.deltas)) {
			return fmt.Errorf("%s spans cover %d buckets but %d deltas are encoded", side.name, spanLength, len(side.deltas))
		}
		var current int64
		for i, delta := range side.deltas {
			current += delta
			if current < 0 {
				return fmt.Errorf("%s bucket %d has a negative population of %d", side.name, i, current)
			}
			population += uint64(current)
		}
	}
	population += h.GetZeroCount()
	if math.IsNaN(h.GetSampleSum()) {
		if population > h.GetSampleCount() {
			return fmt.Errorf("NaN-sum bucket population %d exceeds sample count %d", population, h.GetSampleCount())
		}
		return nil
	}
	if population != h.GetSampleCount() {
		return fmt.Errorf("bucket population %d does not equal sample count %d", population, h.GetSampleCount())
	}
	return nil
}

// TestNativeHistogramWriteInvariants repeatedly validates the protobuf emitted
// by Write while observations, bucket-limit driven schema changes, zero-bucket
// widening and resets all happen concurrently. Every published snapshot must be
// a parseable native histogram. The test is deterministic in its values and
// seeds, so a regression reproduces on every run.
func TestNativeHistogramWriteInvariants(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	const (
		iterations     = 10
		writesPerIter  = 1000
		observersCount = 6
	)

	// Values straddle bucket boundaries and include both signs. The bucket
	// limiting strategy, and in particular the reset path, is exercised by the
	// iteration-specific options below.
	values := make([]float64, 0, 128)
	for exp := -12; exp <= 12; exp++ {
		v := math.Ldexp(1, exp)
		values = append(values, v, math.Nextafter(v, 0), -v, math.Nextafter(-v, 0))
	}
	values = append(values, 0, math.MaxFloat64, -math.MaxFloat64)

	for iter := range iterations {
		t.Run(fmt.Sprintf("iteration_%d", iter), func(t *testing.T) {
			his := NewHistogram(HistogramOpts{
				Name:                            "test_native_histogram_invariants",
				Help:                            "Native histogram invariant test.",
				NativeHistogramBucketFactor:     1.05,
				NativeHistogramMaxBucketNumber:  uint32(4 + iter%5),
				NativeHistogramMaxZeroThreshold: 0.5,
				NativeHistogramMinResetDuration: time.Duration(iter%2) * time.Millisecond,
			})

			var wg sync.WaitGroup
			stop := make(chan struct{})
			for observer := range observersCount {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					r := rand.New(rand.NewSource(seed))
					for {
						select {
						case <-stop:
							return
						default:
							his.Observe(values[r.Intn(len(values))])
						}
					}
				}(int64(observer))
			}

			for i := range writesPerIter {
				var out dto.Metric
				if err := his.Write(&out); err != nil {
					close(stop)
					wg.Wait()
					t.Fatalf("write %d returned an error: %v", i, err)
				}
				if err := validateNativeHistogramSnapshot(out.GetHistogram()); err != nil {
					close(stop)
					wg.Wait()
					t.Fatalf("write %d produced an inconsistent histogram: %v\n%v", i, err, out.GetHistogram())
				}
			}
			close(stop)
			wg.Wait()
		})
	}
}

// TestNativeHistogramWriteInvariantsWithNaN verifies that NaN observations,
// which increment the sample count without landing in a bucket, still produce a
// consistent native histogram where the bucket populations never exceed the
// sample count, even while ±Inf observations keep landing in buckets.
func TestNativeHistogramWriteInvariantsWithNaN(t *testing.T) {
	his := NewHistogram(HistogramOpts{
		Name:                           "test_native_histogram_nan",
		Help:                           "Native histogram NaN invariant test.",
		NativeHistogramBucketFactor:    1.1,
		NativeHistogramMaxBucketNumber: 5,
	})

	values := []float64{0.5, 1, 2, 4, math.NaN(), math.Inf(1), math.Inf(-1)}
	for i := range 1000 {
		his.Observe(values[i%len(values)])
	}

	var out dto.Metric
	if err := his.Write(&out); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if !math.IsNaN(out.GetHistogram().GetSampleSum()) {
		t.Fatalf("expected a NaN sample sum, got %v", out.GetHistogram().GetSampleSum())
	}
	if err := validateNativeHistogramSnapshot(out.GetHistogram()); err != nil {
		t.Fatalf("inconsistent histogram: %v\n%v", err, out.GetHistogram())
	}
}

// TestNewConstNativeHistogramRejectsInvalidInput verifies that the constant
// native histogram constructor rejects inputs that would otherwise produce a
// protobuf message that Prometheus cannot parse, or that would silently move a
// bucket to an index the caller never asked for.
func TestNewConstNativeHistogramRejectsInvalidInput(t *testing.T) {
	desc := NewDesc("test_native_histogram", "help", nil, nil)
	now := time.Now()

	scenarios := []struct {
		name          string
		count         uint64
		sum           float64
		positive      map[int]int64
		negative      map[int]int64
		zeroBucket    uint64
		schema        int32
		zeroThreshold float64
	}{
		{
			name:     "negative positive bucket population",
			count:    3,
			sum:      3,
			positive: map[int]int64{0: 5, 1: -2},
			schema:   2,
		},
		{
			name:     "negative negative bucket population",
			count:    1,
			sum:      1,
			negative: map[int]int64{0: -1},
			schema:   2,
		},
		{
			name:          "NaN zero threshold",
			count:         1,
			sum:           1,
			positive:      map[int]int64{0: 1},
			schema:        2,
			zeroThreshold: math.NaN(),
		},
		{
			name:          "+Inf zero threshold",
			count:         1,
			sum:           1,
			positive:      map[int]int64{0: 1},
			schema:        2,
			zeroThreshold: math.Inf(1),
		},
		{
			name:          "negative zero threshold",
			count:         1,
			sum:           1,
			positive:      map[int]int64{0: 1},
			schema:        2,
			zeroThreshold: -1,
		},
		{
			name:     "bucket index outside int32 range",
			count:    1,
			sum:      1,
			positive: map[int]int64{1 << 33: 1},
			schema:   2,
		},
		{
			name:     "bucket gap outside int32 range",
			count:    2,
			sum:      2,
			positive: map[int]int64{math.MinInt32: 1, math.MaxInt32: 1},
			schema:   2,
		},
		{
			name:       "bucket populations exceed count",
			count:      1,
			sum:        1,
			positive:   map[int]int64{0: 1},
			zeroBucket: 1,
			schema:     2,
		},
		{
			name:     "bucket populations below count for finite sum",
			count:    3,
			sum:      3,
			positive: map[int]int64{0: 1},
			schema:   2,
		},
		{
			name:     "invalid schema",
			count:    1,
			sum:      1,
			positive: map[int]int64{0: 1},
			schema:   nativeHistogramSchemaMaximum + 1,
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			_, err := NewConstNativeHistogram(
				desc, s.count, s.sum, s.positive, s.negative, s.zeroBucket,
				s.schema, s.zeroThreshold, now,
			)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}

// TestNewConstNativeHistogramAcceptsValidInput guards the valid side of the
// validation added in TestNewConstNativeHistogramRejectsInvalidInput, so the
// constructor does not start rejecting legitimate histograms, and checks that
// every accepted value encodes to a snapshot that passes the invariants.
func TestNewConstNativeHistogramAcceptsValidInput(t *testing.T) {
	desc := NewDesc("test_native_histogram", "help", nil, nil)
	now := time.Now()

	scenarios := []struct {
		name          string
		count         uint64
		sum           float64
		positive      map[int]int64
		negative      map[int]int64
		zeroBucket    uint64
		schema        int32
		zeroThreshold float64
	}{
		{
			name:          "empty histogram",
			schema:        2,
			zeroThreshold: 0.001,
		},
		{
			name:          "zero threshold of zero",
			count:         1,
			sum:           1,
			positive:      map[int]int64{0: 1},
			schema:        2,
			zeroThreshold: 0,
		},
		{
			name:          "extreme but encodable bucket indices",
			count:         2,
			sum:           2,
			positive:      map[int]int64{math.MinInt32: 1, math.MinInt32 + 1: 1},
			schema:        2,
			zeroThreshold: 0.001,
		},
		{
			name:          "NaN sum allows bucket populations below the count",
			count:         3,
			sum:           math.NaN(),
			positive:      map[int]int64{0: 1},
			schema:        2,
			zeroThreshold: 0.001,
		},
		{
			name:          "negative and positive buckets",
			count:         4,
			sum:           2,
			positive:      map[int]int64{0: 1, 2: 1},
			negative:      map[int]int64{0: 1, 1: 1},
			schema:        2,
			zeroThreshold: 0.001,
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			m, err := NewConstNativeHistogram(
				desc, s.count, s.sum, s.positive, s.negative, s.zeroBucket,
				s.schema, s.zeroThreshold, now,
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var out dto.Metric
			if err := m.Write(&out); err != nil {
				t.Fatalf("unexpected write error: %v", err)
			}
			if err := validateNativeHistogramSnapshot(out.GetHistogram()); err != nil {
				t.Fatalf("accepted value produced an invalid snapshot: %v\n%v", err, out.GetHistogram())
			}
		})
	}
}

// TestNewConstHistogramRejectsInvalidBuckets verifies that constant classic
// histograms reject bucket maps that Prometheus cannot parse.
func TestNewConstHistogramRejectsInvalidBuckets(t *testing.T) {
	desc := NewDesc("test_const_histogram", "help", nil, nil)
	now := time.Now()

	scenarios := []struct {
		name    string
		count   uint64
		buckets map[float64]uint64
	}{
		{
			name:    "NaN upper bound",
			count:   1,
			buckets: map[float64]uint64{math.NaN(): 1},
		},
		{
			name:    "decreasing cumulative counts",
			count:   10,
			buckets: map[float64]uint64{1: 9, 2: 3},
		},
		{
			name:    "+Inf bound not carrying the sample count",
			count:   10,
			buckets: map[float64]uint64{1: 5, math.Inf(1): 7},
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			if _, err := NewConstHistogram(desc, s.count, 0, s.buckets); err == nil {
				t.Error("NewConstHistogram: expected an error, got nil")
			}
			if _, err := NewConstHistogramWithCreatedTimestamp(desc, s.count, 0, s.buckets, now); err == nil {
				t.Error("NewConstHistogramWithCreatedTimestamp: expected an error, got nil")
			}
		})
	}
}

// TestNewHistogramRejectsUnencodableConfig verifies that invalid configuration
// is rejected at construction time instead of producing a histogram whose
// encoding every exporter rejects.
func TestNewHistogramRejectsUnencodableConfig(t *testing.T) {
	scenarios := []struct {
		name string
		opts HistogramOpts
	}{
		{
			name: "NaN bucket factor",
			opts: HistogramOpts{Name: "a", Help: "h", NativeHistogramBucketFactor: math.NaN()},
		},
		{
			name: "NaN zero threshold",
			opts: HistogramOpts{Name: "b", Help: "h", NativeHistogramBucketFactor: 1.1, NativeHistogramZeroThreshold: math.NaN()},
		},
		{
			name: "+Inf zero threshold",
			opts: HistogramOpts{Name: "c", Help: "h", NativeHistogramBucketFactor: 1.1, NativeHistogramZeroThreshold: math.Inf(1)},
		},
		{
			name: "NaN maximum zero threshold",
			opts: HistogramOpts{Name: "d", Help: "h", NativeHistogramBucketFactor: 1.1, NativeHistogramMaxZeroThreshold: math.NaN()},
		},
		{
			name: "NaN classic bucket",
			opts: HistogramOpts{Name: "e", Help: "h", Buckets: []float64{1, math.NaN(), 2}},
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected NewHistogram to panic")
				}
			}()
			NewHistogram(s.opts)
		})
	}
}

// TestNewHistogramAcceptsNegativeZeroThreshold guards the documented behavior
// that any negative NativeHistogramZeroThreshold selects a zero-width zero
// bucket. The rejection of unencodable configuration must not break it.
func TestNewHistogramAcceptsNegativeZeroThreshold(t *testing.T) {
	his := NewHistogram(HistogramOpts{
		Name:                         "test_negative_zero_threshold",
		Help:                         "Negative zero threshold selects a zero-width zero bucket.",
		NativeHistogramBucketFactor:  1.1,
		NativeHistogramZeroThreshold: NativeHistogramZeroThresholdZero,
	})
	his.Observe(1)

	var out dto.Metric
	if err := his.Write(&out); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if got := out.GetHistogram().GetZeroThreshold(); got != 0 {
		t.Errorf("expected a zero-width zero bucket (threshold 0), got %v", got)
	}
	if err := validateNativeHistogramSnapshot(out.GetHistogram()); err != nil {
		t.Fatalf("inconsistent histogram: %v\n%v", err, out.GetHistogram())
	}
}

// encodeOpenMetrics2 gathers the provided registry and encodes every metric
// family with the OpenMetrics 2.0 encoder, which validates exactly the native
// and classic histogram invariants that Prometheus' own parser enforces: spans
// matching bucket counts, non-negative bucket populations, an in-range schema,
// a finite non-negative zero threshold, and monotonically increasing classic
// buckets. It returns the encoded bytes or the first error.
func encodeOpenMetrics2(t *testing.T, g Gatherer) []byte {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatalf("gather failed: %v", err)
	}
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.FmtOpenMetrics_2_0_0)
	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			t.Fatalf("encoding %q failed: %v", mf.GetName(), err)
		}
	}
	if closer, ok := enc.(expfmt.Closer); ok {
		if err := closer.Close(); err != nil {
			t.Fatalf("finalizing OpenMetrics 2.0 output failed: %v", err)
		}
	}
	if buf.Len() == 0 {
		t.Fatal("encoder produced no output")
	}
	return buf.Bytes()
}

// TestHistogramEncodingRoundTrip proves that the protobuf emitted by both the
// classic and the native histogram paths is accepted by the OpenMetrics 2.0
// encoder, which applies the same invariants as Prometheus' ingestion parser.
// It covers the exact edge cases the validation is meant to guarantee: bucket
// boundaries, NaN and ±Inf observations, both signs, bucket-limit driven schema
// reductions and zero-bucket widening, plus the constant constructors.
func TestHistogramEncodingRoundTrip(t *testing.T) {
	reg := NewPedanticRegistry()

	live := NewHistogram(HistogramOpts{
		Name:                            "live_native_histogram",
		Help:                            "Native histogram with observations.",
		NativeHistogramBucketFactor:     1.05,
		NativeHistogramMaxBucketNumber:  8,
		NativeHistogramMaxZeroThreshold: 0.5,
		NativeHistogramMinResetDuration: time.Millisecond,
	})
	for exp := -20; exp <= 20; exp++ {
		v := math.Ldexp(1, exp)
		live.Observe(v)
		live.Observe(math.Nextafter(v, 0))
		live.Observe(-v)
	}
	live.Observe(0)
	live.Observe(math.NaN())
	live.Observe(math.Inf(1))
	live.Observe(math.Inf(-1))

	liveClassic := NewHistogram(HistogramOpts{
		Name:    "live_classic_histogram",
		Help:    "Classic histogram with observations.",
		Buckets: []float64{-1, 0, 1, 10},
	})
	for _, v := range []float64{-2, -1, 0, 0.5, 1, 5, 100, math.NaN(), math.Inf(1)} {
		liveClassic.Observe(v)
	}

	constNative := MustNewConstNativeHistogram(
		NewDesc("const_native_histogram", "Constant native histogram.", nil, nil),
		5, 1.5, map[int]int64{0: 1, 2: 1}, map[int]int64{0: 2}, 1, 2, 0.001, time.Now(),
	)
	constClassic := MustNewConstHistogram(
		NewDesc("const_classic_histogram", "Constant classic histogram.", nil, nil),
		6, 3.5, map[float64]uint64{-1: 1, 0: 2, 1: 4, 10: 6},
	)

	// The constant metrics are not Collectors themselves; wrap them so they can
	// be registered alongside the live histograms.
	constNativeCollector := CollectorFunc(func(ch chan<- Metric) { ch <- constNative })
	constClassicCollector := CollectorFunc(func(ch chan<- Metric) { ch <- constClassic })

	for _, c := range []Collector{live, liveClassic, constNativeCollector, constClassicCollector} {
		if err := reg.Register(c); err != nil {
			t.Fatalf("registering collector failed: %v", err)
		}
	}

	out := encodeOpenMetrics2(t, reg)
	for _, name := range []string{
		"live_native_histogram",
		"live_classic_histogram",
		"const_native_histogram",
		"const_classic_histogram",
	} {
		if !bytes.Contains(out, []byte(name)) {
			t.Errorf("encoded output does not contain metric %q:\n%s", name, out)
		}
	}
}
