// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promlint_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus/testutil/promlint"
)

func TestScanReturnsEveryFinding(t *testing.T) {
	const in = `
# HELP request_duration_seconds H.
# TYPE request_duration_seconds histogram
request_duration_seconds_bucket{le="1"} 1
# HELP instance_memory_limit_b H.
# TYPE instance_memory_limit_b gauge
instance_memory_limit_b 1
# HELP some_counter C.
# TYPE some_counter counter
some_counter 1
`

	diagnostics, err := promlint.New(strings.NewReader(in)).Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []promlint.Diagnostic{
		{
			Metric:   "instance_memory_limit_b",
			Rule:     promlint.RuleUnitAbbreviations,
			Severity: promlint.SeverityWarning,
			Path:     "name",
			Text:     "metric names should not contain abbreviated units",
		},
		{
			Metric:   "some_counter",
			Rule:     promlint.RuleCounter,
			Severity: promlint.SeverityWarning,
			Path:     "name",
			Text:     `counter metrics should have "_total" suffix`,
		},
		{
			Metric:   "some_counter",
			Rule:     promlint.RuleMetricTypeInName,
			Severity: promlint.SeverityWarning,
			Path:     "name",
			Text:     `metric name should not include type 'counter'`,
		},
	}

	if !reflect.DeepEqual(want, diagnostics) {
		t.Fatalf("unexpected diagnostics:\n- want: %+v\n-  got: %+v", want, diagnostics)
	}
}

func TestScanRuleCodes(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []promlint.Diagnostic
	}{
		{
			name: "help",
			in: `
# TYPE go_goroutines gauge
go_goroutines 24
`,
			want: []promlint.Diagnostic{{
				Metric:   "go_goroutines",
				Rule:     promlint.RuleHelp,
				Severity: promlint.SeverityWarning,
				Path:     "help",
				Text:     "no help text",
			}},
		},
		{
			name: "metric units",
			in: `
# HELP x_kilobytes H.
# TYPE x_kilobytes gauge
x_kilobytes 1
`,
			want: []promlint.Diagnostic{{
				Metric:   "x_kilobytes",
				Rule:     promlint.RuleMetricUnits,
				Severity: promlint.SeverityWarning,
				Path:     "name",
				Text:     `use base unit "bytes" instead of "kilobytes"`,
			}},
		},
		{
			name: "counter reset",
			in: `
# HELP x_bytes_total H.
# TYPE x_bytes_total gauge
x_bytes_total 1
`,
			want: []promlint.Diagnostic{{
				Metric:   "x_bytes_total",
				Rule:     promlint.RuleCounterReset,
				Severity: promlint.SeverityWarning,
				Path:     "name",
				Text:     `non-counter metrics should not have "_total" suffix`,
			}},
		},
		{
			name: "duplicate help",
			in: `
# HELP x_bytes H.
# HELP x_bytes H.
# TYPE x_bytes gauge
x_bytes 1
`,
			want: []promlint.Diagnostic{{
				Metric:   "x_bytes",
				Rule:     promlint.RuleDuplicateHelp,
				Severity: promlint.SeverityError,
				Path:     "help",
				Line:     3,
				Text:     `second HELP line for metric name "x_bytes"`,
			}},
		},
		{
			name: "duplicate type",
			in: `
# HELP x_bytes H.
# TYPE x_bytes gauge
# TYPE x_bytes gauge
x_bytes 1
`,
			want: []promlint.Diagnostic{{
				Metric:   "x_bytes",
				Rule:     promlint.RuleDuplicateType,
				Severity: promlint.SeverityError,
				Path:     "type",
				Line:     4,
				Text:     `second TYPE line for metric name "x_bytes"`,
			}},
		},
		{
			name: "histogram reserved suffix",
			in: `
# HELP x_bytes_bucket H.
# TYPE x_bytes_bucket gauge
x_bytes_bucket 1
`,
			want: []promlint.Diagnostic{{
				Metric:   "x_bytes_bucket",
				Rule:     promlint.RuleHistogramSummaryReserved,
				Severity: promlint.SeverityWarning,
				Path:     "name",
				Text:     `non-histogram metrics should not have "_bucket" suffix`,
			}},
		},
		{
			name: "histogram reserved label",
			in: `
# HELP x_bytes H.
# TYPE x_bytes gauge
x_bytes{le="1"} 1
`,
			want: []promlint.Diagnostic{{
				Metric:   "x_bytes",
				Rule:     promlint.RuleHistogramSummaryReserved,
				Severity: promlint.SeverityWarning,
				Path:     "metric[0].label[0].name",
				Text:     `non-histogram metrics should not have "le" label`,
			}},
		},
		{
			name: "duplicate metric",
			in: `
# HELP x_total H.
# TYPE x_total counter
x_total{bar="abc"} 1
x_total{bar="abc"} 2
`,
			want: []promlint.Diagnostic{{
				Metric:   "x_total",
				Rule:     promlint.RuleDuplicateMetric,
				Severity: promlint.SeverityError,
				Path:     "metric[0]",
				Text:     "metric not unique",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diagnostics, err := promlint.New(strings.NewReader(tt.in)).Scan(context.Background())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(tt.want, diagnostics) {
				t.Fatalf("unexpected diagnostics:\n- want: %+v\n-  got: %+v", tt.want, diagnostics)
			}
		})
	}
}

func TestScanDeterministicOrdering(t *testing.T) {
	families := []*dto.MetricFamily{
		namedFamily("zeta"),
		namedFamily("alpha"),
		namedFamily("middle"),
	}

	var first []promlint.Diagnostic
	for i := range 20 {
		diagnostics, err := promlint.NewWithMetricFamilies(reverse(families)).Scan(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if i == 0 {
			first = diagnostics
			continue
		}
		if !reflect.DeepEqual(first, diagnostics) {
			t.Fatalf("scan %d returned a different order:\n- first: %+v\n-   got: %+v", i, first, diagnostics)
		}
	}

	var metrics []string
	for _, d := range first {
		metrics = append(metrics, d.Metric)
	}
	if want := []string{"alpha", "middle", "zeta"}; !reflect.DeepEqual(want, metrics) {
		t.Fatalf("unexpected metric order: want %v, got %v", want, metrics)
	}
}

func TestScanSortsByPathWithinFamily(t *testing.T) {
	family := &dto.MetricFamily{
		Name: new("x_total"),
		Help: new("H."),
		Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{
			{Label: []*dto.LabelPair{
				{Name: new("a"), Value: new("1")},
				{Name: new("b"), Value: new("1")},
			}},
			{Label: []*dto.LabelPair{
				{Name: new("a"), Value: new("2")},
				{Name: new("b"), Value: new("2")},
			}},
			{Label: []*dto.LabelPair{
				{Name: new("a"), Value: new("1")},
				{Name: new("b"), Value: new("1")},
			}},
		},
	}

	diagnostics, err := promlint.NewWithMetricFamilies([]*dto.MetricFamily{family}).Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var paths []string
	for _, d := range diagnostics {
		if d.Rule != promlint.RuleDuplicateMetric {
			continue
		}
		paths = append(paths, d.Path)
	}
	if want := []string{"metric[0]"}; !reflect.DeepEqual(want, paths) {
		t.Fatalf("unexpected duplicate paths: want %v, got %v", want, paths)
	}
}

func TestSortDiagnosticsNumericPaths(t *testing.T) {
	var metrics []*dto.Metric
	for range 12 {
		metrics = append(metrics, &dto.Metric{
			Label: []*dto.LabelPair{{Name: new("a"), Value: new("1")}},
		})
	}
	family := &dto.MetricFamily{
		Name:   new("x_total"),
		Help:   new("H."),
		Type:   dto.MetricType_COUNTER.Enum(),
		Metric: metrics,
	}

	diagnostics, err := promlint.NewWithMetricFamilies([]*dto.MetricFamily{family}).Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var paths []string
	for _, d := range diagnostics {
		if d.Rule == promlint.RuleDuplicateMetric {
			paths = append(paths, d.Path)
		}
	}
	// The validation reports every later copy of a series, so the last metric
	// has no later duplicate to compare against.
	want := []string{
		"metric[0]", "metric[1]", "metric[2]", "metric[3]", "metric[4]", "metric[5]",
		"metric[6]", "metric[7]", "metric[8]", "metric[9]", "metric[10]",
	}
	if !reflect.DeepEqual(want, paths) {
		t.Fatalf("unexpected duplicate paths: want %v, got %v", want, paths)
	}
}

func TestScanReportsRemainingMetricsAfterMalformedLine(t *testing.T) {
	const in = `# HELP a_total A.
# TYPE a_total counter
a_total 1
a_total{broken
# HELP b_total B.
# TYPE b_total gauge
b_total 1
`

	diagnostics, err := promlint.New(strings.NewReader(in)).Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var malformed []promlint.Diagnostic
	seen := map[string]bool{}
	for _, d := range diagnostics {
		seen[d.Metric] = true
		if d.Rule == promlint.RuleMalformedMetricFamily {
			malformed = append(malformed, d)
		}
	}

	if len(malformed) != 1 {
		t.Fatalf("expected exactly one malformed diagnostic, got %+v", diagnostics)
	}
	if malformed[0].Line != 4 || malformed[0].Severity != promlint.SeverityError {
		t.Fatalf("unexpected malformed diagnostic: %+v", malformed[0])
	}
	if !strings.Contains(malformed[0].Text, "line 4") {
		t.Fatalf("expected the diagnostic to reference line 4, got %q", malformed[0].Text)
	}
	if !seen["b_total"] {
		t.Fatalf("expected b_total to be scanned after the malformed line, got %+v", diagnostics)
	}
	if malformed[0].Metric != "a_total" {
		t.Fatalf("expected the malformed line to be attributed to a_total, got %+v", malformed[0])
	}
	// a_total is valid apart from the malformed line, so it must not produce
	// a spurious "no help text" finding: the recovered metadata is reused.
	for _, d := range diagnostics {
		if d.Metric == "a_total" && d.Rule != promlint.RuleMalformedMetricFamily {
			t.Fatalf("unexpected diagnostic for the family preceding the malformed line: %+v", d)
		}
	}
}

func TestScanRecoversHelpAfterMalformedLine(t *testing.T) {
	// The malformed line removes the HELP metadata of the family from the
	// parsed segment, so a naive recovery would report a spurious "no help
	// text" finding for b_total.
	const in = `
# HELP b_total B.
# TYPE b_total counter
b_total{broken
b_total 1
`

	diagnostics, err := promlint.New(strings.NewReader(in)).Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rules := map[promlint.Rule]int{}
	for _, d := range diagnostics {
		rules[d.Rule]++
	}
	if rules[promlint.RuleMalformedMetricFamily] != 1 {
		t.Fatalf("expected one malformed diagnostic, got %+v", diagnostics)
	}
	if rules[promlint.RuleHelp] != 0 {
		t.Fatalf("expected recovered help text, got %+v", diagnostics)
	}
}

func TestScanReportsPanickingFamilyWithoutAborting(t *testing.T) {
	l := promlint.NewWithMetricFamilies([]*dto.MetricFamily{
		namedFamily("good"),
		namedFamily("bad"),
	})
	l.AddCustomValidations(func(mf *dto.MetricFamily) []error {
		if mf.GetName() == "bad" {
			panic("boom")
		}
		return nil
	})

	diagnostics, err := l.Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var gotBad, gotGood bool
	for _, d := range diagnostics {
		switch {
		case d.Metric == "bad" && d.Rule == promlint.RuleMalformedMetricFamily:
			gotBad = true
			if !strings.Contains(d.Text, "boom") {
				t.Fatalf("expected the panic value in %q", d.Text)
			}
		case d.Metric == "good" && d.Rule == promlint.RuleHelp:
			gotGood = true
		}
	}
	if !gotBad {
		t.Fatalf("expected a malformed diagnostic for the panicking family, got %+v", diagnostics)
	}
	if !gotGood {
		t.Fatalf("expected the remaining family to be scanned, got %+v", diagnostics)
	}
}

func TestScanContextCancellation(t *testing.T) {
	t.Run("pre-canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		diagnostics, err := promlint.New(strings.NewReader("# TYPE a gauge\na 1\n")).Scan(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if len(diagnostics) != 0 {
			t.Fatalf("expected no diagnostics, got %+v", diagnostics)
		}
	})

	t.Run("cancel during read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		r := &cancelOnReadReader{cancel: cancel, data: strings.Repeat("# TYPE a gauge\na 1\n", 1024)}

		diagnostics, err := promlint.New(r).Scan(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if len(diagnostics) != 0 {
			t.Fatalf("expected no diagnostics, got %+v", diagnostics)
		}
	})

	t.Run("cancel while scanning families", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var families []*dto.MetricFamily
		for i := range 100 {
			families = append(families, namedFamily(fmt.Sprintf("metric_%03d", i)))
		}
		l := promlint.NewWithMetricFamilies(families)
		l.AddCustomValidations(func(*dto.MetricFamily) []error {
			cancel()
			return nil
		})

		diagnostics, err := l.Scan(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if len(diagnostics) >= len(families) {
			t.Fatalf("expected the scan to stop early, got %d diagnostics", len(diagnostics))
		}
	})

	t.Run("nil context", func(t *testing.T) {
		//nolint:staticcheck // A nil context is documented to mean Background.
		_, err := promlint.New(strings.NewReader("# HELP a A.\n# TYPE a gauge\na 1\n")).Scan(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestScanWithCustomValidations(t *testing.T) {
	l := promlint.New(strings.NewReader(`
# HELP mc_something_total H.
# TYPE mc_something_total counter
mc_something_total 10
`))
	l.AddCustomValidations(func(mf *dto.MetricFamily) []error {
		if !strings.HasPrefix(mf.GetName(), "memcached_") {
			return []error{errors.New("expected metric name to start with 'memcached_'")}
		}
		return nil
	})

	diagnostics, err := l.Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []promlint.Diagnostic{{
		Metric:   "mc_something_total",
		Rule:     promlint.RuleCustom,
		Severity: promlint.SeverityWarning,
		Text:     "expected metric name to start with 'memcached_'",
	}}
	if !reflect.DeepEqual(want, diagnostics) {
		t.Fatalf("unexpected diagnostics:\n- want: %+v\n-  got: %+v", want, diagnostics)
	}
}

func TestScanDuplicateHelpKeepsScanning(t *testing.T) {
	const in = `
# HELP a_total A.
# HELP a_total A.
# TYPE a_total counter
a_total 1
# HELP b_total B.
# TYPE b_total counter
b_total 1
`

	diagnostics, err := promlint.New(strings.NewReader(in)).Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(diagnostics) != 1 || diagnostics[0].Rule != promlint.RuleDuplicateHelp || diagnostics[0].Metric != "a_total" {
		t.Fatalf("unexpected diagnostics: %+v", diagnostics)
	}
}

func TestScanConflictingMetadataInMetricFamilies(t *testing.T) {
	first := &dto.MetricFamily{
		Name: new("x_total"),
		Help: new("first"),
		Type: dto.MetricType_COUNTER.Enum(),
		Metric: []*dto.Metric{{
			Counter: &dto.Counter{Value: ptrFloat(1)},
		}},
	}
	second := &dto.MetricFamily{
		Name: new("x_total"),
		Help: new("second"),
		Type: dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{{
			Gauge: &dto.Gauge{Value: ptrFloat(2)},
		}},
	}

	diagnostics, err := promlint.NewWithMetricFamilies([]*dto.MetricFamily{first, second}).Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []promlint.Diagnostic{
		{
			Metric:   "x_total",
			Rule:     promlint.RuleDuplicateHelp,
			Severity: promlint.SeverityError,
			Path:     "help",
			Text:     `conflicting help text for metric family "x_total"`,
		},
		{
			Metric:   "x_total",
			Rule:     promlint.RuleCounterReset,
			Severity: promlint.SeverityWarning,
			Path:     "name",
			Text:     `non-counter metrics should not have "_total" suffix`,
		},
		{
			Metric:   "x_total",
			Rule:     promlint.RuleDuplicateType,
			Severity: promlint.SeverityError,
			Path:     "type",
			Text:     `conflicting type metadata for metric family "x_total"`,
		},
	}
	if !reflect.DeepEqual(want, diagnostics) {
		t.Fatalf("unexpected diagnostics:\n- want: %+v\n-  got: %+v", want, diagnostics)
	}
}

func TestScanUnnamedMetricFamily(t *testing.T) {
	// The built-in validations must tolerate a metric family without a name;
	// the former implementation dereferenced the Name field directly and
	// panicked for input built with the public NewWithMetricFamilies API.
	families := []*dto.MetricFamily{{}}

	if _, err := promlint.NewWithMetricFamilies(families).Lint(); err != nil {
		t.Fatalf("unexpected lint error: %v", err)
	}

	diagnostics, err := promlint.NewWithMetricFamilies(families).Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rules := map[promlint.Rule]bool{}
	for _, d := range diagnostics {
		if d.Rule == promlint.RuleMalformedMetricFamily {
			t.Fatalf("an unnamed metric family is not malformed: %+v", d)
		}
		rules[d.Rule] = true
	}
	if !rules[promlint.RuleHelp] {
		t.Fatalf("expected a help finding for the unnamed family, got %+v", diagnostics)
	}
}

func TestScanNilMetricFamily(t *testing.T) {
	diagnostics, err := promlint.NewWithMetricFamilies([]*dto.MetricFamily{nil, namedFamily("good")}).Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var gotNil, gotGood bool
	for _, d := range diagnostics {
		switch {
		case d.Rule == promlint.RuleMalformedMetricFamily && d.Text == "nil metric family":
			gotNil = true
		case d.Metric == "good" && d.Rule == promlint.RuleHelp:
			gotGood = true
		}
	}
	if !gotNil {
		t.Fatalf("expected a diagnostic for the nil family, got %+v", diagnostics)
	}
	if !gotGood {
		t.Fatalf("expected the remaining family to be scanned, got %+v", diagnostics)
	}
}

func TestScanStructuredCustomValidation(t *testing.T) {
	l := promlint.NewWithMetricFamilies([]*dto.MetricFamily{namedFamily("mc_something")})
	l.AddCustomValidations(func(*dto.MetricFamily) []error {
		return []error{promlint.NewIssue(
			promlint.RuleCustom,
			promlint.SeverityInfo,
			"name",
			"expected metric name to start with 'memcached_'",
		)}
	})

	diagnostics, err := l.Scan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []promlint.Diagnostic{
		{
			Metric:   "mc_something",
			Rule:     promlint.RuleHelp,
			Severity: promlint.SeverityWarning,
			Path:     "help",
			Text:     "no help text",
		},
		{
			Metric:   "mc_something",
			Rule:     promlint.RuleCustom,
			Severity: promlint.SeverityInfo,
			Path:     "name",
			Text:     "expected metric name to start with 'memcached_'",
		},
	}
	if !reflect.DeepEqual(want, diagnostics) {
		t.Fatalf("unexpected diagnostics:\n- want: %+v\n-  got: %+v", want, diagnostics)
	}
}

func TestDiagnosticString(t *testing.T) {
	d := promlint.Diagnostic{
		Metric:   "x_total",
		Path:     "name",
		Line:     3,
		Rule:     promlint.RuleCounter,
		Severity: promlint.SeverityWarning,
		Text:     "no help text",
	}
	if want, got := "x_total:name:3: no help text [counter/warning]", d.String(); want != got {
		t.Fatalf("unexpected String(): want %q, got %q", want, got)
	}
}

func TestScanDoesNotAffectLint(t *testing.T) {
	inputs := []string{
		"# TYPE go_goroutines gauge\ngo_goroutines 24\n",
		"# HELP go_goroutines\n# TYPE go_goroutines gauge\ngo_goroutines 24\n",
		"# HELP go_goroutines\n# TYPE go_goroutines gauge\ngo_goroutines 24\n# TYPE go_threads gauge\ngo_threads 10\n",
		"# HELP x_amperes Test metric.\n# TYPE x_amperes untyped\nx_amperes 10\n",
		"# HELP x_gigabytes Test metric.\n# TYPE x_gigabytes untyped\nx_gigabytes 10\n",
		"# HELP x_bytes Test metric.\n# TYPE x_bytes counter\nx_bytes 10\n",
		"# HELP x_bytes_total Test metric.\n# TYPE x_bytes_total gauge\nx_bytes_total 10\n",
		"# HELP x_bytes_bucket Test metric.\n# TYPE x_bytes_bucket gauge\nx_bytes_bucket 10\n",
		"# HELP x_bytes Test metric.\n# TYPE x_bytes gauge\nx_bytes{le=\"1\"} 10\n",
		"# HELP x_bytes Test metric.\n# TYPE x_bytes gauge\nx_bytes{quantile=\"1\"} 10\n",
		"# HELP http_requests_counter Test metric.\n# TYPE http_requests_counter counter\nhttp_requests_counter 10\n",
		"# HELP request_duration::_seconds Test metric.\n# TYPE request_duration::_seconds histogram\nrequest_duration::_seconds 10\n",
		"# HELP requestDuration_seconds Test metric.\n# TYPE requestDuration_seconds histogram\nrequestDuration_seconds 10\n",
		"# HELP request_duration_seconds Test metric.\n# TYPE request_duration_seconds histogram\nrequest_duration_seconds{httpService=\"foo\"} 10\n",
		"# HELP instance_memory_limit_b Test metric.\n# TYPE instance_memory_limit_b gauge\ninstance_memory_limit_b 10\n",
		"# HELP not_unique_total the helptext\n# TYPE not_unique_total counter\nnot_unique_total{bar=\"abc\", spam=\"xyz\"} 1\nnot_unique_total{bar=\"abc\", spam=\"xyz\"} 2\n",
	}

	for _, in := range inputs {
		t.Run(strings.TrimSpace(in), func(t *testing.T) {
			problems, err := promlint.New(strings.NewReader(in)).Lint()
			if err != nil {
				t.Fatalf("unexpected lint error: %v", err)
			}

			diagnostics, err := promlint.New(strings.NewReader(in)).Scan(context.Background())
			if err != nil {
				t.Fatalf("unexpected scan error: %v", err)
			}

			var got []promlint.Problem
			for _, d := range diagnostics {
				got = append(got, promlint.Problem{Metric: d.Metric, Text: d.Text})
			}
			if !reflect.DeepEqual(problems, got) {
				t.Fatalf("Scan and Lint report different findings:\n- lint: %+v\n- scan: %+v", problems, got)
			}
		})
	}
}

// cancelOnReadReader cancels its context after the first read.
type cancelOnReadReader struct {
	cancel func()
	data   string
	done   bool
}

func (r *cancelOnReadReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, errors.New("read after cancellation")
	}
	r.done = true
	n := copy(p, r.data)
	r.cancel()
	return n, nil
}

func FuzzScan(f *testing.F) {
	seeds := []string{
		"",
		"# HELP a A.\n# TYPE a gauge\na 1\n",
		"# HELP a A.\n# HELP a A.\n# TYPE a gauge\na{bad\n# TYPE b counter\nb 2\n",
		"# TYPE a gauge\na{nope\n",
		"\n\n\n# HELP\n# TYPE\n",
		"a 1",
		"# HELP a A.\n# TYPE a gauge\na 1\na{le=\"1\"} 2\n",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, in string) {
		l := promlint.New(strings.NewReader(in))

		first, err := l.Scan(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// A scan must never depend on map iteration order, so scanning the
		// same input again must produce the same diagnostics in the same
		// order.
		second, err := promlint.New(strings.NewReader(in)).Scan(context.Background())
		if err != nil {
			t.Fatalf("unexpected error on the second scan: %v", err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("scan is not deterministic:\n- first: %+v\n- second: %+v", first, second)
		}

		for _, d := range first {
			if d.Rule == "" || d.Severity < promlint.SeverityInfo || d.Severity > promlint.SeverityError {
				t.Fatalf("diagnostic with invalid rule or severity: %+v", d)
			}
		}
	})
}

func namedFamily(name string) *dto.MetricFamily {
	return &dto.MetricFamily{
		Name: new(name),
		Type: dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{{
			Gauge: &dto.Gauge{Value: ptrFloat(1)},
		}},
	}
}

// ptrFloat returns a pointer to f. A helper is needed because new(1) is not a
// valid way to take the address of an untyped constant.
func ptrFloat(f float64) *float64 { return new(f) }

func reverse(families []*dto.MetricFamily) []*dto.MetricFamily {
	out := make([]*dto.MetricFamily, len(families))
	for i, f := range families {
		out[len(families)-1-i] = f
	}
	return out
}
