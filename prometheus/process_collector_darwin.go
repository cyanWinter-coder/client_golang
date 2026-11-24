// Copyright 2024 The Prometheus Authors
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

//go:build darwin && !ios

package prometheus

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// errNotImplemented is returned by stub functions that replace cgo functions, when cgo
// isn't available.
var errNotImplemented = errors.New("not implemented")

type memoryInfo struct {
	vsize uint64 // Virtual memory size in bytes
	rss   uint64 // Resident memory size in bytes
}

func canCollectProcess() bool {
	return true
}

func getSoftLimit(which int) (uint64, error) {
	rlimit := syscall.Rlimit{}

	if err := syscall.Getrlimit(which, &rlimit); err != nil {
		return 0, err
	}

	return rlimit.Cur, nil
}

func getOpenFileCount() (float64, error) {
	// Alternately, the undocumented proc_pidinfo(PROC_PIDLISTFDS) can be used to
	// return a list of open fds, but that requires a way to call C APIs.  The
	// benefits, however, include fewer system calls and not failing when at the
	// open file soft limit.

	dir, err := os.Open("/dev/fd")
	if err != nil {
		return 0.0, err
	}
	defer dir.Close()

	// Avoid ReadDir(), as it calls stat(2) on each descriptor.  Not only is
	// that info not used, but KQUEUE descriptors fail stat(2), which causes
	// the whole method to fail.
	names, err := dir.Readdirnames(0)
	if err != nil {
		return 0.0, err
	}

	// Subtract 1 to ignore the open /dev/fd descriptor above.
	return float64(len(names) - 1), nil
}

func (c *processCollector) processCollect(ch chan<- Metric) {
	pid, err := c.pidFn()
	if err != nil {
		c.reportFatalAndLogError(ch, err)
		return
	}

	if procs, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid); err == nil {
		if len(procs) == 1 {
			// P_starttime is a timeval relative to the Unix epoch, so the
			// nanosecond-to-second conversion yields seconds since 1970.
			startTime := float64(procs[0].Proc.P_starttime.Nano() / 1e9)
			c.emitMetric(ch, c.startTime, GaugeValue, startTime)
		} else {
			err = fmt.Errorf("sysctl() returned %d proc structs (expected 1)", len(procs))
			c.reportAndLogError(ch, c.startTime, err)
		}
	} else {
		c.reportAndLogError(ch, c.startTime, err)
	}

	if pid != os.Getpid() {
		c.reportFatalAndLogError(ch, fmt.Errorf("collecting metrics for pid %d is not supported on darwin: process metrics collection is limited to the current process (pid %d)", pid, os.Getpid()))
		return
	}

	// The proc structure returned by kern.proc.pid above has an Rusage member,
	// but it is not filled in, so it needs to be fetched by getrusage(2).  For
	// that call, the UTime, STime, and Maxrss members are filled out, but not
	// Ixrss, Idrss, or Isrss for the memory usage.  Memory stats will require
	// access to the C API to call task_info(TASK_BASIC_INFO).
	rusage := unix.Rusage{}

	if err := unix.Getrusage(syscall.RUSAGE_SELF, &rusage); err == nil {
		// Both timeval structs count seconds and microseconds since the Unix
		// epoch; time.Duration.Seconds converts their sum to seconds of CPU
		// time, which is the unit expected by process_cpu_seconds_total.
		cpuTime := time.Duration(rusage.Stime.Nano() + rusage.Utime.Nano()).Seconds()
		c.emitMetric(ch, c.cpuTotal, CounterValue, cpuTime)
	} else {
		c.reportAndLogError(ch, c.cpuTotal, err)
	}

	if memInfo, err := getMemory(); err == nil {
		c.emitMetric(ch, c.rss, GaugeValue, float64(memInfo.rss))
		c.emitMetric(ch, c.vsize, GaugeValue, float64(memInfo.vsize))
	} else if !errors.Is(err, errNotImplemented) {
		// Don't report an error when support is not compiled in.
		c.reportAndLogError(ch, c.rss, err)
		c.reportAndLogError(ch, c.vsize, err)
	}

	// Readdirnames returns one entry per open descriptor including the
	// /dev/fd directory opened above, so the descriptor count is the number
	// of entries minus one.
	if fds, err := getOpenFileCount(); err == nil {
		c.emitMetric(ch, c.openFDs, GaugeValue, fds)
	} else {
		c.reportAndLogError(ch, c.openFDs, err)
	}

	if openFiles, err := getSoftLimit(syscall.RLIMIT_NOFILE); err == nil {
		c.emitMetric(ch, c.maxFDs, GaugeValue, float64(openFiles))
	} else {
		c.reportAndLogError(ch, c.maxFDs, err)
	}

	if addressSpace, err := getSoftLimit(syscall.RLIMIT_AS); err == nil {
		c.emitMetric(ch, c.maxVsize, GaugeValue, float64(addressSpace))
	} else {
		c.reportAndLogError(ch, c.maxVsize, err)
	}

	// TODO: socket(PF_SYSTEM) to fetch "com.apple.network.statistics" might
	//  be able to get the per-process network send/receive counts.
}
