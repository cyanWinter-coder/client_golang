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

package prometheus_test

import (
	"bytes"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/prometheus/client_golang/prometheus"
)

type uid573DescriptorCollector struct {
	descs   []*prometheus.Desc
	metrics []prometheus.Metric
}

func (c *uid573DescriptorCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *uid573DescriptorCollector) Collect(ch chan<- prometheus.Metric) {
	for _, metric := range c.metrics {
		ch <- metric
	}
}

func TestUID573DescriptorNameValidationAtRegistration(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		metric    string
		labels    []string
		wantInErr string
	}{
		{
			name:      "metric name contains hyphen",
			metric:    "requests-total",
			wantInErr: "not a valid metric name",
		},
		{
			name:      "metric name starts with digit",
			metric:    "1requests_total",
			wantInErr: "not a valid metric name",
		},
		{
			name:      "metric name contains space",
			metric:    "requests total",
			wantInErr: "not a valid metric name",
		},
		{
			name:      "variable label contains hyphen",
			metric:    "requests_total",
			labels:    []string{"request-id"},
			wantInErr: "not a valid label name",
		},
		{
			name:      "variable label starts with digit",
			metric:    "requests_total",
			labels:    []string{"1id"},
			wantInErr: "not a valid label name",
		},
		{
			name:      "reserved label prefix",
			metric:    "requests_total",
			labels:    []string{"__reserved"},
			wantInErr: "not a valid label name",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			desc := prometheus.NewDesc(tc.metric, "help", tc.labels, nil)
			if desc.Err() == nil {
				t.Fatal("expected NewDesc to record a validation error")
			}

			reg := prometheus.NewRegistry()
			err := reg.Register(&uid573DescriptorCollector{descs: []*prometheus.Desc{desc}})
			if err == nil {
				t.Fatal("expected Register to reject invalid descriptor")
			}
			if !strings.Contains(err.Error(), tc.wantInErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantInErr)
			}
		})
	}
}

func TestUID573DescriptorNameValidationAcceptsCanonicalNames(t *testing.T) {
	t.Parallel()

	desc := prometheus.NewDesc(
		"requests:rate5m",
		"help",
		[]string{"method", "status_code"},
		prometheus.Labels{"cluster_id": "local"},
	)
	if err := desc.Err(); err != nil {
		t.Fatalf("canonical descriptor should be valid: %v", err)
	}

	reg := prometheus.NewRegistry()
	if err := reg.Register(&uid573DescriptorCollector{descs: []*prometheus.Desc{desc}}); err != nil {
		t.Fatalf("canonical descriptor should register: %v", err)
	}
}

func TestUID573DescriptorMetadataConflicts(t *testing.T) {
	t.Parallel()

	t.Run("help text", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewRegistry()
		first := &uid573DescriptorCollector{descs: []*prometheus.Desc{
			prometheus.NewDesc("help_conflict_metric", "first help", nil, nil),
		}}
		second := &uid573DescriptorCollector{descs: []*prometheus.Desc{
			prometheus.NewDesc("help_conflict_metric", "second help", nil, nil),
		}}

		if err := reg.Register(first); err != nil {
			t.Fatal(err)
		}
		err := reg.Register(second)
		if err == nil {
			t.Fatal("expected help text conflict")
		}
		if !strings.Contains(err.Error(), "help text mismatch") {
			t.Fatalf("expected explicit help text mismatch, got: %v", err)
		}
	})

	t.Run("metric type", func(t *testing.T) {
		t.Parallel()

		reg := prometheus.NewRegistry()
		if err := reg.Register(prometheus.NewCounter(prometheus.CounterOpts{
			Name: "type_conflict_metric",
			Help: "same help",
		})); err != nil {
			t.Fatal(err)
		}

		err := reg.Register(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "type_conflict_metric",
			Help: "same help",
		}))
		if err == nil {
			t.Fatal("expected metric type conflict")
		}
		if !strings.Contains(err.Error(), "type mismatch") {
			t.Fatalf("expected explicit type mismatch, got: %v", err)
		}
		if !strings.Contains(err.Error(), "COUNTER") || !strings.Contains(err.Error(), "GAUGE") {
			t.Fatalf("error should name both types, got: %v", err)
		}
	})

	t.Run("declared descriptor type option", func(t *testing.T) {
		t.Parallel()

		counterDesc := prometheus.V2.NewDesc(
			"custom_declared_type_metric",
			"same help",
			prometheus.UnconstrainedLabels(nil),
			nil,
			prometheus.WithType(dto.MetricType_COUNTER),
		)
		gaugeDesc := prometheus.V2.NewDesc(
			"custom_declared_type_metric",
			"same help",
			prometheus.UnconstrainedLabels(nil),
			nil,
			prometheus.WithType(dto.MetricType_GAUGE),
		)
		if counterDesc.Type() != dto.MetricType_COUNTER {
			t.Fatalf("got type %s, want COUNTER", counterDesc.Type())
		}

		reg := prometheus.NewRegistry()
		if err := reg.Register(&uid573DescriptorCollector{descs: []*prometheus.Desc{counterDesc}}); err != nil {
			t.Fatal(err)
		}
		err := reg.Register(&uid573DescriptorCollector{descs: []*prometheus.Desc{gaugeDesc}})
		if err == nil || !strings.Contains(err.Error(), "type mismatch") {
			t.Fatalf("expected declared type mismatch, got %v", err)
		}
	})
}

func TestUID573DescriptorsWithDifferentLabelDimensionsMerge(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	firstDesc := prometheus.NewDesc("label_dimension_metric", "same help", []string{"a"}, nil)
	secondDesc := prometheus.NewDesc("label_dimension_metric", "same help", []string{"c", "b"}, nil)

	firstMetric := prometheus.MustNewConstMetric(firstDesc, prometheus.GaugeValue, 1, "a1")
	secondMetric := prometheus.MustNewConstMetric(secondDesc, prometheus.GaugeValue, 2, "c2", "b2")

	if err := reg.Register(&uid573DescriptorCollector{
		descs:   []*prometheus.Desc{firstDesc},
		metrics: []prometheus.Metric{firstMetric},
	}); err != nil {
		t.Fatalf("register first label dimension: %v", err)
	}
	if err := reg.Register(&uid573DescriptorCollector{
		descs:   []*prometheus.Desc{secondDesc},
		metrics: []prometheus.Metric{secondMetric},
	}); err != nil {
		t.Fatalf("register second label dimension: %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather merged metric family: %v", err)
	}
	if len(families) != 1 {
		t.Fatalf("expected one metric family, got %d", len(families))
	}
	family := families[0]
	if family.GetName() != "label_dimension_metric" {
		t.Fatalf("unexpected family name %q", family.GetName())
	}
	if family.GetHelp() != "same help" {
		t.Fatalf("unexpected help %q", family.GetHelp())
	}
	if len(family.Metric) != 2 {
		t.Fatalf("expected two merged metrics, got %d", len(family.Metric))
	}

	seenDimensions := map[string]bool{}
	for _, metric := range family.Metric {
		names := make([]string, 0, len(metric.Label))
		for _, label := range metric.Label {
			names = append(names, label.GetName())
		}
		seenDimensions[strings.Join(names, ",")] = true
	}
	if !seenDimensions["a"] || !seenDimensions["b,c"] {
		t.Fatalf("missing merged label dimensions: %v", seenDimensions)
	}
}

func TestUID573DescriptorsWithReorderedLabelDimensionsRegisterSeparately(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	firstDesc := prometheus.NewDesc("reordered_labels_metric", "same help", []string{"a", "b"}, nil)
	secondDesc := prometheus.NewDesc("reordered_labels_metric", "same help", []string{"b", "a"}, nil)

	firstMetric := prometheus.MustNewConstMetric(firstDesc, prometheus.GaugeValue, 1, "a1", "b1")
	secondMetric := prometheus.MustNewConstMetric(secondDesc, prometheus.GaugeValue, 2, "b2", "a2")

	if err := reg.Register(&uid573DescriptorCollector{
		descs:   []*prometheus.Desc{firstDesc},
		metrics: []prometheus.Metric{firstMetric},
	}); err != nil {
		t.Fatalf("register first label order: %v", err)
	}
	if err := reg.Register(&uid573DescriptorCollector{
		descs:   []*prometheus.Desc{secondDesc},
		metrics: []prometheus.Metric{secondMetric},
	}); err != nil {
		t.Fatalf("reordered descriptor should merge instead of being treated as the same collector: %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather reordered label family: %v", err)
	}
	if len(families) != 1 || len(families[0].Metric) != 2 {
		t.Fatalf("expected one family with two metrics, got %#v", families)
	}
}

func TestUID573FailedDescriptorRegistrationLeavesRegistryUnchanged(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	existing := prometheus.NewGauge(prometheus.GaugeOpts{Name: "atomic_existing", Help: "existing help"})
	if err := reg.Register(existing); err != nil {
		t.Fatal(err)
	}

	invalid := &uid573DescriptorCollector{descs: []*prometheus.Desc{
		prometheus.NewDesc("atomic_new_metric", "new help", nil, nil),
		prometheus.NewDesc("bad-name", "invalid help", nil, nil),
	}}
	if err := reg.Register(invalid); err == nil {
		t.Fatal("expected invalid registration to fail")
	}

	// The valid descriptor from the failed batch must not have been retained,
	// so registering it now is a normal first registration rather than a
	// duplicate.
	newDesc := prometheus.NewDesc("atomic_new_metric", "new help", nil, nil)
	valid := &uid573DescriptorCollector{
		descs:   []*prometheus.Desc{newDesc},
		metrics: []prometheus.Metric{prometheus.MustNewConstMetric(newDesc, prometheus.GaugeValue, 1)},
	}
	if err := reg.Register(valid); err != nil {
		t.Fatalf("failed batch changed registry state: %v", err)
	}

	// A help conflict also has to leave the original metadata authoritative.
	conflictingHelp := &uid573DescriptorCollector{descs: []*prometheus.Desc{
		prometheus.NewDesc("atomic_existing", "different help", nil, nil),
	}}
	if err := reg.Register(conflictingHelp); err == nil {
		t.Fatal("expected help conflict")
	}
	matchingHelp := prometheus.NewCounter(prometheus.CounterOpts{Name: "atomic_other", Help: "other"})
	if err := reg.Register(matchingHelp); err != nil {
		t.Fatal(err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather after failed registrations: %v", err)
	}
	names := map[string]string{}
	for _, family := range families {
		names[family.GetName()] = family.GetHelp()
	}
	if names["atomic_existing"] != "existing help" {
		t.Fatalf("existing metadata changed: %q", names["atomic_existing"])
	}
	if _, ok := names["atomic_new_metric"]; !ok {
		t.Fatal("valid descriptor from retry was not registered")
	}
}

func TestUID573MergedExpositionParses(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	firstDesc := prometheus.NewDesc("uid573_parse_metric", "help", []string{"a"}, nil)
	secondDesc := prometheus.NewDesc("uid573_parse_metric", "help", []string{"b", "c"}, nil)
	if err := reg.Register(&uid573DescriptorCollector{
		descs:   []*prometheus.Desc{firstDesc},
		metrics: []prometheus.Metric{prometheus.MustNewConstMetric(firstDesc, prometheus.GaugeValue, 1, "a")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(&uid573DescriptorCollector{
		descs:   []*prometheus.Desc{secondDesc},
		metrics: []prometheus.Metric{prometheus.MustNewConstMetric(secondDesc, prometheus.GaugeValue, 2, "b", "c")},
	}); err != nil {
		t.Fatal(err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	encoder := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		if err := encoder.Encode(family); err != nil {
			t.Fatal(err)
		}
	}

	parser := expfmt.NewTextParser(model.LegacyValidation)
	parsed, err := parser.TextToMetricFamilies(&buf)
	if err != nil {
		t.Fatalf("legacy parser rejected exposition %q: %v", buf.String(), err)
	}
	if len(parsed) != 1 {
		t.Fatalf("parsed %d metric families, want 1", len(parsed))
	}
}
