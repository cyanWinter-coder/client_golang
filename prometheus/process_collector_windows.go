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

package prometheus

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// windowsToUnixEpochSeconds is the difference between the Windows FILETIME
// epoch (1601-01-01 UTC) and the Unix epoch (1970-01-01 UTC), in seconds.
//
// 11644473600 seconds = 369 years of leap-year-adjusted days between the two
// epochs. Subtracting it converts a FILETIME-based timestamp into seconds
// since the Unix epoch, matching process_start_time_seconds on other
// platforms.
const windowsToUnixEpochSeconds = 11644473600

func canCollectProcess() bool {
	return true
}

var (
	modpsapi    = syscall.NewLazyDLL("psapi.dll")
	modkernel32 = syscall.NewLazyDLL("kernel32.dll")

	procGetProcessMemoryInfo  = modpsapi.NewProc("GetProcessMemoryInfo")
	procGetProcessHandleCount = modkernel32.NewProc("GetProcessHandleCount")

	openProcess     = windows.OpenProcess
	closeHandle     = windows.CloseHandle
	getProcessTimes = windows.GetProcessTimes
)

type processMemoryCounters struct {
	// System interface description
	// https://docs.microsoft.com/en-us/windows/desktop/api/psapi/ns-psapi-process_memory_counters_ex

	// Refer to the Golang internal implementation
	// https://golang.org/src/internal/syscall/windows/psapi_windows.go
	_                          uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
	PrivateUsage               uintptr
}

func getProcessMemoryInfo(handle windows.Handle) (processMemoryCounters, error) {
	mem := processMemoryCounters{}
	r1, _, err := procGetProcessMemoryInfo.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&mem)),
		uintptr(unsafe.Sizeof(mem)),
	)
	if r1 != 1 {
		return mem, err
	} else {
		return mem, nil
	}
}

func getProcessHandleCount(handle windows.Handle) (uint32, error) {
	var count uint32
	r1, _, err := procGetProcessHandleCount.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&count)),
	)
	if r1 != 1 {
		return 0, err
	} else {
		return count, nil
	}
}

func (c *processCollector) processCollect(ch chan<- Metric) {
	pid, err := c.pidFn()
	if err != nil {
		c.reportFatalAndLogError(ch, err)
		return
	}

	h, err := openProcess(windows.PROCESS_QUERY_INFORMATION|windows.PROCESS_VM_READ, false, uint32(pid))
	if err != nil {
		c.reportFatalAndLogError(ch, err)
		return
	}
	defer closeHandle(h)

	var startTime, exitTime, kernelTime, userTime windows.Filetime
	if err := getProcessTimes(h, &startTime, &exitTime, &kernelTime, &userTime); err != nil {
		// Skip every time-based metric together: they all come from the same
		// call, so a partial export would be misleading.
		c.reportAndLogError(ch, c.startTime, err)
		c.reportAndLogError(ch, c.cpuTotal, err)
	} else {
		// Filetime.Nanoseconds returns nanoseconds since the Windows epoch
		// (1601-01-01). Convert to seconds since the Unix epoch so the metric
		// has the same meaning as on Linux and Darwin.
		c.emitMetric(ch, c.startTime, GaugeValue, filetimeToUnixSeconds(startTime))
		// FILETIME intervals are independent of the epoch; the 1e7 divisor
		// converts the 100-nanosecond ticks to seconds.
		c.emitMetric(ch, c.cpuTotal, CounterValue, fileTimeToSeconds(kernelTime)+fileTimeToSeconds(userTime))
	}

	mem, err := getProcessMemoryInfo(h)
	if err != nil {
		c.reportAndLogError(ch, c.vsize, err)
		c.reportAndLogError(ch, c.rss, err)
	} else {
		c.emitMetric(ch, c.vsize, GaugeValue, float64(mem.PrivateUsage))
		c.emitMetric(ch, c.rss, GaugeValue, float64(mem.WorkingSetSize))
	}

	handles, err := getProcessHandleCount(h)
	if err != nil {
		c.reportAndLogError(ch, c.openFDs, err)
	} else {
		// On Windows the equivalent of open file descriptors is the number of
		// open process handles.
		c.emitMetric(ch, c.openFDs, GaugeValue, float64(handles))
	}
	// Windows has a hard-coded system-wide handle limit, not a per-process
	// rlimit, so the maximum is a constant and never fails to collect.
	c.emitMetric(ch, c.maxFDs, GaugeValue, float64(16*1024*1024))
}

// describe returns all descriptions of the collector for windows.
// Ensure that this list of descriptors is kept in sync with the metrics collected
// in the processCollect method. Any changes to the metrics in processCollect
// (such as adding or removing metrics) should be reflected in this list of descriptors.
func (c *processCollector) describe(ch chan<- *Desc) {
	ch <- c.cpuTotal
	ch <- c.openFDs
	ch <- c.maxFDs
	ch <- c.vsize
	ch <- c.rss
	ch <- c.startTime
}

func fileTimeToSeconds(ft windows.Filetime) float64 {
	return float64(uint64(ft.HighDateTime)<<32+uint64(ft.LowDateTime)) / 1e7
}

// filetimeToUnixSeconds converts a FILETIME instant to seconds since the
// Unix epoch. FILETIME counts 100-nanosecond intervals since 1601-01-01 UTC,
// while process_start_time_seconds is documented as seconds since the Unix
// epoch.
func filetimeToUnixSeconds(ft windows.Filetime) float64 {
	return fileTimeToSeconds(ft) - windowsToUnixEpochSeconds
}
