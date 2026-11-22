// Copyright 2020 The Prometheus Authors
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

package testutil

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil/promlint"
)

func TestCollectAndLintGood(t *testing.T) {
	cnt := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "some_total",
			Help: "A value that represents a counter.",
			ConstLabels: prometheus.Labels{
				"label1": "value1",
			},
		},
		[]string{"foo"},
	)
	cnt.WithLabelValues("bar")
	cnt.WithLabelValues("baz")

	problems, err := CollectAndLint(cnt)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(problems) > 0 {
		t.Error("Unexpected lint problems:", problems)
	}
}

func TestCollectAndLintBad(t *testing.T) {
	cnt := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "someThing_ms",
			Help: "A value that represents a counter.",
			ConstLabels: prometheus.Labels{
				"label1": "value1",
			},
		},
		[]string{"fooBar"},
	)
	cnt.WithLabelValues("bar")
	cnt.WithLabelValues("baz")

	problems, err := CollectAndLint(cnt)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(problems) < 5 {
		// The exact nature of the lint problems found is tested within
		// the promlint package itself. Here we only want to make sure
		// that the collector successfully hits the linter and that at
		// least the five problems that the linter could recognize at
		// the time of writing this test are flagged.
		t.Error("Not enough lint problems found.")
	}
}

func TestCollectAndScan(t *testing.T) {
	ctx := context.Background()

	t.Run("good", func(t *testing.T) {
		cnt := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "some_total",
				Help: "A value that represents a counter.",
			},
			[]string{"foo"},
		)
		cnt.WithLabelValues("bar")

		diagnostics, err := CollectAndScan(ctx, cnt)
		if err != nil {
			t.Fatal("Unexpected error:", err)
		}
		if len(diagnostics) > 0 {
			t.Fatal("Unexpected diagnostics:", diagnostics)
		}
	})

	t.Run("bad", func(t *testing.T) {
		cnt := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "someThing_ms",
				Help: "A value that represents a counter.",
			},
			[]string{"fooBar"},
		)
		cnt.WithLabelValues("bar")

		diagnostics, err := CollectAndScan(ctx, cnt)
		if err != nil {
			t.Fatal("Unexpected error:", err)
		}

		// Every finding carries a stable rule code and a path, which is what
		// differentiates the structured scan from the legacy lint output.
		rules := map[promlint.Rule]bool{}
		for _, d := range diagnostics {
			if d.Rule == "" {
				t.Fatalf("diagnostic without rule: %+v", d)
			}
			if d.Path == "" {
				t.Fatalf("diagnostic without path: %+v", d)
			}
			rules[d.Rule] = true
		}
		for _, want := range []promlint.Rule{
			promlint.RuleCamelCase,
			promlint.RuleUnitAbbreviations,
			promlint.RuleCounter,
		} {
			if !rules[want] {
				t.Fatalf("expected rule %q in %+v", want, diagnostics)
			}
		}
	})

	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		cnt := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "some_total",
			Help: "A value that represents a counter.",
		})

		_, err := CollectAndScan(ctx, cnt)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})
}
