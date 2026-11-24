// Copyright 2016 The Prometheus Authors
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
	"fmt"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"

	"github.com/prometheus/client_golang/prometheus/internal"
)

// Desc is the descriptor used by every Prometheus Metric. It is essentially
// the immutable meta-data of a Metric. The normal Metric implementations
// included in this package manage their Desc under the hood. Users only have to
// deal with Desc if they use advanced features like the ExpvarCollector or
// custom Collectors and Metrics.
//
// Descriptors registered with the same registry have to fulfill certain
// consistency and uniqueness criteria if they share the same fully-qualified
// name: They must have the same help string and metric type (and unit, if one
// is set). Their label dimensions may differ; the registry treats them as the
// same metric family and Gather merges their series.
//
// Descriptors that share the same fully-qualified name, label names, and
// constant label values are considered equal.
//
// Use NewDesc to create new Desc instances.
type Desc struct {
	// fqName has been built from Namespace, Subsystem, and Name.
	fqName string
	// help provides some helpful information about this metric.
	help string
	// unit provides the unit of this metric.
	unit string
	// The declared type of this metric. A Desc created with NewDesc
	// defaults to dto.MetricType_UNTYPED because its type is often
	// determined by the metric implementation or sample.
	metricType dto.MetricType
	// constLabelPairs contains precalculated DTO label pairs based on
	// the constant labels.
	constLabelPairs []*dto.LabelPair
	// variableLabels contains names of labels and normalization function for
	// which the metric maintains variable values.
	variableLabels *compiledLabels
	// id is a hash of the fqName, the declared label dimension, and the
	// values of the ConstLabels. It must be unique among all registered
	// descriptors and can therefore be used as an identifier of the
	// descriptor.
	id uint64
	// The dimHash field is retained as the historical hash of the sorted
	// label names and Help string. Registry family metadata is keyed by
	// fqName so label dimensions can merge, but keeping the field preserves
	// the internal descriptor surface.
	dimHash uint64
	// err is an error that occurred during construction. It is reported on
	// registration time.
	err error
}

// DescOpt allows setting optional fields for NewDesc.
type DescOpt func(*Desc)

// WithUnit sets the unit for a Desc.
func WithUnit(unit string) DescOpt {
	return func(d *Desc) {
		d.unit = unit
	}
}

// WithType sets the declared metric type for a Desc. Most callers should use a
// typed constructor such as NewCounter, NewGauge, NewHistogram, or NewSummary,
// which sets the type automatically. Custom Desc implementations can use this
// option when their metric type is known at descriptor construction time.
func WithType(metricType dto.MetricType) DescOpt {
	return withType(metricType)
}

// The withType option sets the declared metric type for a Desc. It is the
// internal implementation shared by typed constructors and WithType.
func withType(metricType dto.MetricType) DescOpt {
	return func(d *Desc) {
		d.metricType = metricType
	}
}

// NewDesc allocates and initializes a new Desc. Errors are recorded in the Desc
// and will be reported on registration time. variableLabels and constLabels can
// be nil if no such labels should be set. fqName must not be empty.
//
// variableLabels only contain the label names. Their label values are variable
// and therefore not part of the Desc. (They are managed within the Metric.)
//
// For constLabels, the label values are constant. Therefore, they are fully
// specified in the Desc. See the Collector example for a usage pattern.
func NewDesc(fqName, help string, variableLabels []string, constLabels Labels) *Desc {
	return V2.NewDesc(fqName, help, UnconstrainedLabels(variableLabels), constLabels)
}

// NewDesc allocates and initializes a new Desc. Errors are recorded in the Desc
// and will be reported on registration time. variableLabels and constLabels can
// be nil if no such labels should be set. fqName must not be empty.
//
// variableLabels only contain the label names and normalization functions. Their
// label values are variable and therefore not part of the Desc. (They are managed
// within the Metric.)
//
// For constLabels, the label values are constant. Therefore, they are fully
// specified in the Desc. See the Collector example for a usage pattern.
func (v2) NewDesc(fqName, help string, variableLabels ConstrainableLabels, constLabels Labels, opts ...DescOpt) *Desc {
	d := &Desc{
		fqName:         fqName,
		help:           help,
		variableLabels: variableLabels.compile(),
		metricType:     dto.MetricType_UNTYPED,
	}

	for _, opt := range opts {
		opt(d)
	}
	if !model.UTF8Validation.IsValidMetricName(fqName) {
		d.err = fmt.Errorf("%q is not a valid metric name", fqName)
		return d
	}
	// labelValues contains the label values of const labels (in order of
	// their sorted label names) plus the fqName (at position 0).
	labelValues := make([]string, 1, len(constLabels)+1)
	labelValues[0] = fqName
	labelNames := make([]string, 0, len(constLabels)+len(d.variableLabels.names))
	labelNameSet := map[string]struct{}{}
	// First add only the const label names and sort them...
	for labelName := range constLabels {
		if !checkLabelName(labelName) {
			d.err = fmt.Errorf("%q is not a valid label name for metric %q", labelName, fqName)
			return d
		}
		labelNames = append(labelNames, labelName)
		labelNameSet[labelName] = struct{}{}
	}
	sort.Strings(labelNames)
	// ... so that we can now add const label values in the order of their names.
	for _, labelName := range labelNames {
		labelValues = append(labelValues, constLabels[labelName])
	}
	// Validate the const label values. They can't have a wrong cardinality, so
	// use in len(labelValues) as expectedNumberOfValues.
	if err := validateLabelValues(labelValues, len(labelValues)); err != nil {
		d.err = err
		return d
	}
	// Now add the variable label names, but prefix them with something that
	// cannot be in a regular label name. That prevents matching the label
	// dimension with a different mix between preset and variable labels.
	for _, label := range d.variableLabels.names {
		if !checkLabelName(label) {
			d.err = fmt.Errorf("%q is not a valid label name for metric %q", label, fqName)
			return d
		}
		labelNames = append(labelNames, "$"+label)
		labelNameSet[label] = struct{}{}
	}
	if len(labelNames) != len(labelNameSet) {
		d.err = fmt.Errorf("duplicate label names in constant and variable labels for metric %q", fqName)
		return d
	}

	// Hash the label dimension without sorting variable label names. Metric
	// families are identified by fqName alone, but descriptor/collector
	// identity has to distinguish declarations that provide the same label
	// names in different orders so those collectors can be registered
	// separately and merged during Gather. Constant label names are already
	// sorted, and variable names retain their declared order.
	xxh := xxhash.New()
	for _, val := range labelValues {
		xxh.WriteString(val)
		xxh.Write(separatorByteSlice)
	}
	xxh.WriteString("label-names")
	xxh.Write(separatorByteSlice)
	for _, labelName := range labelNames {
		xxh.WriteString(labelName)
		xxh.Write(separatorByteSlice)
	}
	d.id = xxh.Sum64()

	// Sort labelNames so the dimension hash is independent of label order.
	sort.Strings(labelNames)
	xxh.Reset()
	xxh.WriteString(help)
	xxh.Write(separatorByteSlice)
	xxh.WriteString(d.unit)
	xxh.Write(separatorByteSlice)
	for _, labelName := range labelNames {
		xxh.WriteString(labelName)
		xxh.Write(separatorByteSlice)
	}
	d.dimHash = xxh.Sum64()

	d.constLabelPairs = make([]*dto.LabelPair, 0, len(constLabels))
	for n, v := range constLabels {
		d.constLabelPairs = append(d.constLabelPairs, &dto.LabelPair{
			Name:  new(n),
			Value: new(v),
		})
	}
	sort.Sort(internal.LabelPairSorter(d.constLabelPairs))
	if d.err == nil {
		d.err = validateDescriptorNames(d)
	}
	return d
}

// NewInvalidDesc returns an invalid descriptor, i.e. a descriptor with the
// provided error set. If a collector returning such a descriptor is registered,
// registration will fail with the provided error. NewInvalidDesc can be used by
// a Collector to signal inability to describe itself.
func NewInvalidDesc(err error) *Desc {
	return &Desc{
		err:        err,
		metricType: dto.MetricType_UNTYPED,
	}
}

// Err returns an error that occurred during construction, if any.
//
// Calling this method is optional. It can be used to detect construction
// errors early, before invoking other methods on the Desc. If an error is
// present, later operations may not behave as expected.
func (d *Desc) Err() error {
	return d.err
}

// Type returns the declared metric type. Descs created with NewDesc and no
// WithType option return dto.MetricType_UNTYPED. The typed constructors in
// this package return their corresponding metric type.
func (d *Desc) Type() dto.MetricType {
	return d.metricType
}

func (d *Desc) String() string {
	lpStrings := make([]string, 0, len(d.constLabelPairs))
	for _, lp := range d.constLabelPairs {
		lpStrings = append(
			lpStrings,
			fmt.Sprintf("%s=%q", lp.GetName(), lp.GetValue()),
		)
	}
	vlStrings := []string{}
	if d.variableLabels != nil {
		vlStrings = make([]string, 0, len(d.variableLabels.names))
		for _, vl := range d.variableLabels.names {
			if fn, ok := d.variableLabels.labelConstraints[vl]; ok && fn != nil {
				vlStrings = append(vlStrings, fmt.Sprintf("c(%s)", vl))
			} else {
				vlStrings = append(vlStrings, vl)
			}
		}
	}
	return fmt.Sprintf(
		"Desc{fqName: %q, help: %q, unit: %q, constLabels: {%s}, variableLabels: {%s}}",
		d.fqName,
		d.help,
		d.unit,
		strings.Join(lpStrings, ","),
		strings.Join(vlStrings, ","),
	)
}
