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

// Package registry provides experimental additions around the stable
// prometheus Gatherer, in particular context-aware gathering.
//
// This package is experimental and may contain breaking changes or be removed
// in the future. It only depends on the stable Gatherer interface, so it works
// with any Gatherer implementation and does not require a specific
// client_golang module version.
package registry

import (
	"context"

	dto "github.com/prometheus/client_model/go"
)

// Gatherer is the minimal subset of the stable prometheus.Gatherer interface
// that this package needs. It is redeclared here so the package does not import
// the root client_golang module, keeping the exp module independently
// versioned. A prometheus.Gatherer satisfies it directly.
type Gatherer interface {
	Gather() ([]*dto.MetricFamily, error)
}

// ContextGatherer is the experimental context-aware counterpart of the stable
// Gatherer. GatherWithContext returns the gathered metric families and honors
// the provided context.
//
// Because the stable Gatherer interface has no cancellation hook, honoring the
// context is cooperative: when the context is canceled, GatherWithContext
// returns promptly with the context error without waiting for an in-flight
// Gather call to finish. That background Gather is allowed to complete and its
// result is discarded, so no goroutine is left blocked on a channel. Metrics
// that the underlying Gather produced before cancellation are returned only if
// the Gather itself returned in time; otherwise the call returns the context
// error with a nil slice.
type ContextGatherer interface {
	GatherWithContext(ctx context.Context) ([]*dto.MetricFamily, error)
}

// ContextGathererFunc adapts a function to the ContextGatherer interface.
type ContextGathererFunc func(context.Context) ([]*dto.MetricFamily, error)

// GatherWithContext implements ContextGatherer.
func (f ContextGathererFunc) GatherWithContext(ctx context.Context) ([]*dto.MetricFamily, error) {
	return f(ctx)
}

// WithContext turns a stable Gatherer into a ContextGatherer. The wrapped
// Gather runs in a goroutine so a canceled context can return early. If the
// context is already canceled when the method is called, the underlying
// Gather is never started.
func WithContext(g Gatherer) ContextGatherer {
	return ContextGathererFunc(func(ctx context.Context) ([]*dto.MetricFamily, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		type gatherResult struct {
			mfs []*dto.MetricFamily
			err error
		}
		// The buffered channel guarantees the background Gather goroutine can
		// always send its result and exit, even when this method returns early
		// because the context was canceled.
		done := make(chan gatherResult, 1)
		go func() {
			mfs, err := g.Gather()
			done <- gatherResult{mfs: mfs, err: err}
		}()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-done:
			return res.mfs, res.err
		}
	})
}
