// Copyright 2019 The Prometheus Authors
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

//go:build !windows && !js && !wasip1 && !darwin

package prometheus

import (
	"github.com/prometheus/procfs"
)

func canCollectProcess() bool {
	_, err := procfs.NewDefaultFS()
	return err == nil
}

func (c *processCollector) processCollect(ch chan<- Metric) {
	pid, err := c.pidFn()
	if err != nil {
		c.reportFatalAndLogError(ch, err)
		return
	}

	p, err := procfs.NewProc(pid)
	if err != nil {
		c.reportFatalAndLogError(ch, err)
		return
	}

	// Each metric group is collected independently: a failure reading one
	// source (for example /proc/<pid>/stat) must not prevent the remaining
	// metrics from being emitted.
	if stat, err := p.Stat(); err == nil {
		// CPUTime converts clock ticks to seconds using the platform's
		// user HZ value, so the value is already expressed in seconds.
		c.emitMetric(ch, c.cpuTotal, CounterValue, stat.CPUTime())
		c.emitMetric(ch, c.vsize, GaugeValue, float64(stat.VirtualMemory()))
		c.emitMetric(ch, c.rss, GaugeValue, float64(stat.ResidentMemory()))

		// StartTime combines the kernel boot time with the process start
		// time measured in clock ticks since boot, both converted to
		// seconds since the Unix epoch.
		if startTime, err := stat.StartTime(); err == nil {
			c.emitMetric(ch, c.startTime, GaugeValue, startTime)
		} else {
			c.reportAndLogError(ch, c.startTime, err)
		}
	} else {
		c.reportAndLogError(ch, c.cpuTotal, err)
		c.reportAndLogError(ch, c.vsize, err)
		c.reportAndLogError(ch, c.rss, err)
		c.reportAndLogError(ch, c.startTime, err)
	}

	// FileDescriptorsLen counts the entries in /proc/<pid>/fd, which is the
	// portable source for the number of open file descriptors on Linux.
	if fds, err := p.FileDescriptorsLen(); err == nil {
		c.emitMetric(ch, c.openFDs, GaugeValue, float64(fds))
	} else {
		c.reportAndLogError(ch, c.openFDs, err)
	}

	if limits, err := p.Limits(); err == nil {
		c.emitMetric(ch, c.maxFDs, GaugeValue, float64(limits.OpenFiles))
		c.emitMetric(ch, c.maxVsize, GaugeValue, float64(limits.AddressSpace))
	} else {
		c.reportAndLogError(ch, c.maxFDs, err)
		c.reportAndLogError(ch, c.maxVsize, err)
	}

	if netstat, err := p.Netstat(); err == nil {
		var inOctets, outOctets float64
		if netstat.InOctets != nil {
			inOctets = *netstat.InOctets
		}
		if netstat.OutOctets != nil {
			outOctets = *netstat.OutOctets
		}
		c.emitMetric(ch, c.inBytes, CounterValue, inOctets)
		c.emitMetric(ch, c.outBytes, CounterValue, outOctets)
	} else {
		// Missing network accounting is common inside restricted containers;
		// skip both network counters without affecting the other metrics.
		c.reportAndLogError(ch, c.inBytes, err)
		c.reportAndLogError(ch, c.outBytes, err)
	}
}

// describe returns all descriptions of the collector for others than windows, js, wasip1 and darwin.
// Ensure that this list of descriptors is kept in sync with the metrics collected
// in the processCollect method. Any changes to the metrics in processCollect
// (such as adding or removing metrics) should be reflected in this list of descriptors.
func (c *processCollector) describe(ch chan<- *Desc) {
	ch <- c.cpuTotal
	ch <- c.openFDs
	ch <- c.maxFDs
	ch <- c.vsize
	ch <- c.maxVsize
	ch <- c.rss
	ch <- c.startTime
	ch <- c.inBytes
	ch <- c.outBytes
}
