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
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus/testutil/promlint"
)

// This example scans an exposition that contains several independent problems
// and prints one line per finding. The stable rule code lets callers dispatch
// on the category of a finding, for example to apply a targeted fix.
func ExampleLinter_Scan() {
	const in = `
# HELP request_duration_seconds Request duration.
# TYPE request_duration_seconds histogram
request_duration_seconds_bucket{le="1"} 1
# HELP instance_memory_limit_b Memory limit.
# TYPE instance_memory_limit_b gauge
instance_memory_limit_b 1
# HELP some_counter A counter.
# TYPE some_counter counter
some_counter 1
`

	diagnostics, err := promlint.New(strings.NewReader(in)).Scan(context.Background())
	if err != nil {
		fmt.Println("scan failed:", err)
		return
	}

	for _, d := range diagnostics {
		fmt.Printf("%s %s %s\n", d.Metric, d.Rule, d.Severity)
	}
	// Output:
	// instance_memory_limit_b unit-abbreviations warning
	// some_counter counter warning
	// some_counter metric-type-in-name warning
}
