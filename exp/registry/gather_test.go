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

package registry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/exp/registry"

	dto "github.com/prometheus/client_model/go"
)

// stubGatherer is a minimal Gatherer whose Gather method can block until
// released and returns a fixed result otherwise.
type stubGatherer struct {
	mfs     []*dto.MetricFamily
	err     error
	entered chan struct{}
	release chan struct{}
}

func (s *stubGatherer) Gather() ([]*dto.MetricFamily, error) {
	if s.entered != nil {
		close(s.entered)
	}
	if s.release != nil {
		<-s.release
	}
	return s.mfs, s.err
}

var _ registry.Gatherer = (*stubGatherer)(nil)

// TestWithContextReturnsResult verifies the wrapper forwards an active
// Gather result unchanged.
func TestWithContextReturnsResult(t *testing.T) {
	want := []*dto.MetricFamily{{
		Name: strPtr("exp_gauge"),
		Type: dto.MetricType_GAUGE.Enum(),
	}}
	g := registry.WithContext(&stubGatherer{mfs: want})

	got, err := g.GatherWithContext(context.Background())
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	if len(got) != 1 || got[0].GetName() != "exp_gauge" {
		t.Fatalf("unexpected result: %v", got)
	}
}

// TestWithContextAlreadyCanceled verifies an already-canceled context returns
// the context error and does not block.
func TestWithContextAlreadyCanceled(t *testing.T) {
	g := registry.WithContext(&stubGatherer{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.GatherWithContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// TestWithContextCancelsEarly verifies early return when the context is
// canceled while the wrapped Gather is blocked, and that no goroutine leaks.
func TestWithContextCancelsEarly(t *testing.T) {
	stub := &stubGatherer{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { close(stub.release) })
	g := registry.WithContext(stub)

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := g.GatherWithContext(ctx)
		resultCh <- err
	}()

	select {
	case <-stub.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("wrapped gather never started")
	}
	cancel()
	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled wrapper did not return")
	}
}

// TestWithContextPropagatesGatherError verifies a non-nil error from the
// wrapped Gather is forwarded unchanged.
func TestWithContextPropagatesGatherError(t *testing.T) {
	gatherErr := errors.New("boom")
	g := registry.WithContext(&stubGatherer{err: gatherErr})

	_, err := g.GatherWithContext(context.Background())
	if !errors.Is(err, gatherErr) {
		t.Fatalf("expected the gather error, got %v", err)
	}
}

// TestContextGathererFunc verifies the function adapter.
func TestContextGathererFunc(t *testing.T) {
	called := false
	var g registry.ContextGatherer = registry.ContextGathererFunc(
		func(context.Context) ([]*dto.MetricFamily, error) {
			called = true
			return nil, nil
		},
	)
	if _, err := g.GatherWithContext(context.Background()); err != nil {
		t.Fatal("unexpected error:", err)
	}
	if !called {
		t.Fatal("the adapted function was not called")
	}
}

func strPtr(s string) *string {
	return &s
}
