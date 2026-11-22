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

package validations_test

import (
	"errors"
	"reflect"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus/testutil/promlint/validations"
)

func TestValidationIssues(t *testing.T) {
	str := func(s string) *string { return &s }

	tests := []struct {
		name string
		fn   func(*dto.MetricFamily) []error
		mf   *dto.MetricFamily
		want []validations.Issue
	}{
		{
			name: "no help text",
			fn:   validations.LintHelp,
			mf:   &dto.MetricFamily{Name: str("x")},
			want: []validations.Issue{{
				Rule:     validations.RuleHelp,
				Severity: validations.SeverityWarning,
				Path:     "help",
				Message:  "no help text",
			}},
		},
		{
			name: "counter without total",
			fn:   validations.LintCounter,
			mf: &dto.MetricFamily{
				Name: str("x"),
				Type: dto.MetricType_COUNTER.Enum(),
			},
			want: []validations.Issue{{
				Rule:     validations.RuleCounter,
				Severity: validations.SeverityWarning,
				Path:     "name",
				Message:  `counter metrics should have "_total" suffix`,
			}},
		},
		{
			name: "counter reset",
			fn:   validations.LintCounter,
			mf: &dto.MetricFamily{
				Name: str("x_total"),
				Type: dto.MetricType_GAUGE.Enum(),
			},
			want: []validations.Issue{{
				Rule:     validations.RuleCounterReset,
				Severity: validations.SeverityWarning,
				Path:     "name",
				Message:  `non-counter metrics should not have "_total" suffix`,
			}},
		},
		{
			name: "non-base unit",
			fn:   validations.LintMetricUnits,
			mf:   &dto.MetricFamily{Name: str("x_kilobytes")},
			want: []validations.Issue{{
				Rule:     validations.RuleMetricUnits,
				Severity: validations.SeverityWarning,
				Path:     "name",
				Message:  `use base unit "bytes" instead of "kilobytes"`,
			}},
		},
		{
			name: "abbreviated unit",
			fn:   validations.LintUnitAbbreviations,
			mf:   &dto.MetricFamily{Name: str("x_kb")},
			want: []validations.Issue{{
				Rule:     validations.RuleUnitAbbreviations,
				Severity: validations.SeverityWarning,
				Path:     "name",
				Message:  "metric names should not contain abbreviated units",
			}},
		},
		{
			name: "histogram reserved label",
			fn:   validations.LintHistogramSummaryReserved,
			mf: &dto.MetricFamily{
				Name: str("x"),
				Type: dto.MetricType_GAUGE.Enum(),
				Metric: []*dto.Metric{{
					Label: []*dto.LabelPair{{
						Name:  str("le"),
						Value: str("1"),
					}},
				}},
			},
			want: []validations.Issue{{
				Rule:     validations.RuleHistogramSummaryReserved,
				Severity: validations.SeverityWarning,
				Path:     "metric[0].label[0].name",
				Message:  `non-histogram metrics should not have "le" label`,
			}},
		},
		{
			name: "duplicate metric",
			fn:   validations.LintDuplicateMetric,
			mf: &dto.MetricFamily{
				Name: str("x_total"),
				Metric: []*dto.Metric{
					{Label: []*dto.LabelPair{{Name: str("a"), Value: str("1")}}},
					{Label: []*dto.LabelPair{{Name: str("a"), Value: str("1")}}},
				},
			},
			want: []validations.Issue{{
				Rule:     validations.RuleDuplicateMetric,
				Severity: validations.SeverityError,
				Path:     "metric[0]",
				Message:  "metric not unique",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := tt.fn(tt.mf)
			if len(errs) != len(tt.want) {
				t.Fatalf("expected %d issues, got %d: %v", len(tt.want), len(errs), errs)
			}
			for i, err := range errs {
				var issue *validations.Issue
				if !errors.As(err, &issue) {
					t.Fatalf("error %d is not an *Issue: %T", i, err)
				}
				if !reflect.DeepEqual(tt.want[i], *issue) {
					t.Fatalf("issue %d: want %+v, got %+v", i, tt.want[i], *issue)
				}
			}
		})
	}
}

func TestIssueErrorTextMatchesMessage(t *testing.T) {
	issue := validations.NewIssue(validations.RuleHelp, validations.SeverityWarning, "help", "no help text")
	if want, got := "no help text", issue.Error(); want != got {
		t.Fatalf("unexpected Error(): want %q, got %q", want, got)
	}
}

func TestSeverityString(t *testing.T) {
	for severity, want := range map[validations.Severity]string{
		validations.SeverityInfo:    "info",
		validations.SeverityWarning: "warning",
		validations.SeverityError:   "error",
		validations.Severity(99):    "unknown",
	} {
		if got := severity.String(); got != want {
			t.Fatalf("Severity(%d).String(): want %q, got %q", severity, want, got)
		}
	}
}
