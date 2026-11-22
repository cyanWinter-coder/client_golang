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

package validations

import (
	"fmt"
	"strconv"
)

// Severity describes how strongly a lint finding affects the scanned metrics.
type Severity int

const (
	// SeverityInfo marks a style recommendation that does not change how a
	// metric is interpreted.
	SeverityInfo Severity = iota
	// SeverityWarning marks a convention violation that still results in a
	// valid exposition but makes the metrics harder to consume.
	SeverityWarning
	// SeverityError marks input that cannot be interpreted as a valid metric
	// exposition, for example unparseable text or duplicate series.
	SeverityError
)

// String returns the lower-case name of the severity.
func (s Severity) String() string {
	switch s {
	case SeverityInfo:
		return "info"
	case SeverityWarning:
		return "warning"
	case SeverityError:
		return "error"
	default:
		return "unknown"
	}
}

// Rule is a stable identifier for a lint rule. Rule values are part of the
// public API and must not change between releases so that automated tooling
// can rely on them.
type Rule string

// Stable rule identifiers reported by the built-in validations and by the
// promlint scanner. Each value identifies exactly one rule.
const (
	// RuleHelp reports a metric family without help text.
	RuleHelp Rule = "help"
	// RuleDuplicateHelp reports repeated or conflicting help text.
	RuleDuplicateHelp Rule = "duplicate-help"
	// RuleDuplicateType reports repeated type metadata.
	RuleDuplicateType Rule = "duplicate-type"
	// RuleMetricUnits reports a metric name that uses a non-base unit.
	RuleMetricUnits Rule = "metric-units"
	// RuleUnitAbbreviations reports a metric name that uses an abbreviated
	// unit.
	RuleUnitAbbreviations Rule = "unit-abbreviations"
	// RuleCounter reports counter metrics without the "_total" suffix.
	RuleCounter Rule = "counter"
	// RuleCounterReset reports non-counter metrics carrying the "_total"
	// suffix. PromQL and other consumers treat such series as counters, so
	// their resets are misdetected.
	RuleCounterReset Rule = "counter-reset"
	// RuleHistogramSummaryReserved reports names and labels reserved for
	// histograms and summaries.
	RuleHistogramSummaryReserved Rule = "histogram-summary-reserved"
	// RuleMetricTypeInName reports metric names that contain the metric type.
	RuleMetricTypeInName Rule = "metric-type-in-name"
	// RuleReservedChars reports reserved characters in metric names.
	RuleReservedChars Rule = "reserved-chars"
	// RuleCamelCase reports camelCase metric or label names.
	RuleCamelCase Rule = "camel-case"
	// RuleDuplicateMetric reports duplicate series within a metric family.
	RuleDuplicateMetric Rule = "duplicate-metric"
	// RuleMalformedMetricFamily reports a metric family or exposition line
	// that cannot be interpreted.
	RuleMalformedMetricFamily Rule = "malformed-metric-family"
	// RuleCustom identifies findings produced by custom validations that do
	// not carry a rule of their own.
	RuleCustom Rule = "custom"
)

// Field paths used by the built-in validations.
const (
	pathName = "name"
	pathHelp = "help"
)

// Issue is a structured lint finding. It implements the error interface so
// that the legacy validation functions can keep returning []error while
// callers that need structure can recover it with errors.As.
type Issue struct {
	// Rule is the stable identifier of the rule that produced the Issue.
	Rule Rule
	// Severity is the impact of the Issue.
	Severity Severity
	// Path is the location of the Issue within the metric family, for
	// example "name", "help", or "metric[0].label[1].name".
	Path string
	// Message is the human-readable description of the Issue.
	Message string
}

// Error implements the error interface. It returns only the human-readable
// message so that the output of the legacy validation functions does not
// change.
func (i *Issue) Error() string {
	return i.Message
}

// MetricPath returns the field path of the metric at index i.
func MetricPath(i int) string {
	return "metric[" + strconv.Itoa(i) + "]"
}

// LabelPath returns the field path of the label at indices i and j.
func LabelPath(i, j int) string {
	return MetricPath(i) + ".label[" + strconv.Itoa(j) + "]"
}

// NewIssue creates an Issue with a fixed message.
func NewIssue(rule Rule, severity Severity, path, message string) *Issue {
	return &Issue{
		Rule:     rule,
		Severity: severity,
		Path:     path,
		Message:  message,
	}
}

// NewIssuef creates an Issue with a formatted message.
func NewIssuef(rule Rule, severity Severity, path, format string, args ...any) *Issue {
	return &Issue{
		Rule:     rule,
		Severity: severity,
		Path:     path,
		Message:  fmt.Sprintf(format, args...),
	}
}
