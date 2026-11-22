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

package push

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
)

// fakeDoer is an HTTPDoer that records every request, its body, and the number
// of times idle connections were closed. Responses come from handle, which is
// called with the zero-based attempt index.
type fakeDoer struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	closes   int
	handle   func(req *http.Request, attempt int) (*http.Response, error)
}

func (d *fakeDoer) Do(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
		_ = req.Body.Close()
	}
	d.mu.Lock()
	attempt := len(d.requests)
	d.requests = append(d.requests, req)
	d.bodies = append(d.bodies, body)
	handle := d.handle
	d.mu.Unlock()
	if handle == nil {
		return okResponse(), nil
	}
	return handle(req, attempt)
}

// CloseIdleConnections makes fakeDoer look like *http.Client, which is the
// interface a Pusher uses to isolate retries.
func (d *fakeDoer) CloseIdleConnections() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closes++
}

func (d *fakeDoer) requestCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.requests)
}

func (d *fakeDoer) closeCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closes
}

func (d *fakeDoer) requestAt(t *testing.T, i int) *http.Request {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	if i >= len(d.requests) {
		t.Fatalf("request %d does not exist, only %d recorded", i, len(d.requests))
	}
	return d.requests[i]
}

func (d *fakeDoer) bodyAt(t *testing.T, i int) []byte {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	if i >= len(d.bodies) {
		t.Fatalf("body %d does not exist, only %d recorded", i, len(d.bodies))
	}
	return d.bodies[i]
}

func okResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
	}
}

func statusResponse(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

// doerFunc adapts a function to HTTPDoer.
type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

func TestNewRejectsInvalidOptions(t *testing.T) {
	for _, test := range []struct {
		name    string
		opt     PusherOption
		wantErr error
	}{
		{
			name:    "negative retries",
			opt:     WithRetryPolicy(RetryPolicy{MaxRetries: -1}),
			wantErr: errInvalidRetryPolicy,
		},
		{
			name:    "negative min backoff",
			opt:     WithRetryPolicy(RetryPolicy{MaxRetries: 1, MinBackoff: -time.Second}),
			wantErr: errInvalidRetryPolicy,
		},
		{
			name:    "min backoff above max backoff",
			opt:     WithRetryPolicy(RetryPolicy{MaxRetries: 1, MinBackoff: time.Second, MaxBackoff: time.Millisecond}),
			wantErr: errInvalidRetryPolicy,
		},
		{
			name:    "negative timeout",
			opt:     WithTimeout(-time.Second),
			wantErr: errNegativeTimeout,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := New("http://example.org", "testjob", test.opt).Error(); !errors.Is(err, test.wantErr) {
				t.Fatalf("Error() = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestRetryPolicyBackoff(t *testing.T) {
	policy := RetryPolicy{MaxRetries: 6, MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}
	want := []time.Duration{
		10 * time.Millisecond, // Attempt 1 retry.
		20 * time.Millisecond, // Attempt 2 retry.
		40 * time.Millisecond, // Attempt 3 retry.
		50 * time.Millisecond, // Capped.
		50 * time.Millisecond,
		50 * time.Millisecond,
	}
	for i, wantDelay := range want {
		if got := policy.backoff(i + 1); got != wantDelay {
			t.Errorf("backoff(%d) = %s, want %s", i+1, got, wantDelay)
		}
	}

	// A zero MinBackoff retries immediately.
	if got := (RetryPolicy{MaxRetries: 3}).backoff(1); got != 0 {
		t.Errorf("backoff without MinBackoff = %s, want 0", got)
	}
	if got := DefaultRetryPolicy(); got.MaxRetries != 3 {
		t.Errorf("DefaultRetryPolicy().MaxRetries = %d, want 3", got.MaxRetries)
	}
}

// TestPushRetryReusesIdenticalRequest verifies that a retry sends the same
// grouping key, body, headers, and credentials as the first attempt, even if
// the Pusher is reconfigured while the first attempt is in flight.
func TestPushRetryReusesIdenticalRequest(t *testing.T) {
	firstRequest := make(chan struct{})
	releaseFirst := make(chan struct{})
	doer := &fakeDoer{}
	doer.handle = func(_ *http.Request, attempt int) (*http.Response, error) {
		if attempt == 0 {
			close(firstRequest)
			<-releaseFirst
			return nil, errors.New("first attempt timed out")
		}
		return okResponse(), nil
	}
	pusher := New("http://example.org", "testjob", WithRetryPolicy(RetryPolicy{MaxRetries: 2})).
		Client(doer).
		Grouping("zone", "xy").
		BasicAuth("user", "secret").
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "test_counter", Help: "test help."}))

	done := make(chan error, 1)
	go func() { done <- pusher.Push() }()
	<-firstRequest

	// Mutating the Pusher during the first attempt must not leak into the retry.
	pusher.Grouping("zone", "changed")
	pusher.BasicAuth("other", "other")
	pusher.Header(http.Header{"X-Late": {"value"}})
	close(releaseFirst)

	if err := <-done; err != nil {
		t.Fatalf("Push() = %v, want nil", err)
	}
	if got := doer.requestCount(); got != 2 {
		t.Fatalf("got %d requests, want 2", got)
	}
	for i := range 2 {
		req := doer.requestAt(t, i)
		if want := "/metrics/job/testjob/zone/xy"; req.URL.Path != want {
			t.Errorf("request %d path = %q, want %q", i, req.URL.Path, want)
		}
		if got, want := req.Header.Get("Authorization"), "Basic dXNlcjpzZWNyZXQ="; got != want {
			t.Errorf("request %d Authorization = %q, want %q", i, got, want)
		}
		if got := req.Header.Get("X-Late"); got != "" {
			t.Errorf("request %d carried a header added after the push started: %q", i, got)
		}
		if !bytes.Equal(doer.bodyAt(t, i), doer.bodyAt(t, 0)) {
			t.Errorf("request %d body differs from the first attempt", i)
		}
	}
	if len(doer.bodyAt(t, 0)) == 0 {
		t.Error("request body is empty")
	}
}

// TestPushRetryClosesIdleConnections verifies that a retry drops connections
// from the failed attempt and sends the retry with "Connection: close" so a
// half-uploaded connection cannot be reused.
func TestPushRetryClosesIdleConnections(t *testing.T) {
	doer := &fakeDoer{handle: func(_ *http.Request, attempt int) (*http.Response, error) {
		if attempt == 0 {
			return nil, errors.New("timeout")
		}
		return okResponse(), nil
	}}
	err := New("http://example.org", "job", WithRetryPolicy(RetryPolicy{MaxRetries: 1})).
		Client(doer).
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "retry_counter", Help: "test help."})).
		Push()
	if err != nil {
		t.Fatalf("Push() = %v, want nil", err)
	}
	if got := doer.requestCount(); got != 2 {
		t.Fatalf("got %d requests, want 2", got)
	}
	if doer.closeCount() == 0 {
		t.Error("CloseIdleConnections was not called before the retry")
	}
	if !doer.requestAt(t, 1).Close {
		t.Error("retry request did not set Close, so it could reuse a connection from the failed attempt")
	}
}

// TestPushNoRetryKeepsSingleAttempt verifies that the zero-value policy keeps
// the historical single-attempt behavior.
func TestPushNoRetryKeepsSingleAttempt(t *testing.T) {
	doer := &fakeDoer{handle: func(*http.Request, int) (*http.Response, error) {
		return nil, errors.New("timeout")
	}}
	err := New("http://example.org", "job").
		Client(doer).
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "single_counter", Help: "test help."})).
		Push()
	if err == nil {
		t.Fatal("Push() = nil, want transport error")
	}
	if got := doer.requestCount(); got != 1 {
		t.Errorf("got %d requests, want 1", got)
	}
}

// TestPushRetryStopsOnCancellation verifies that cancellation is final, so a
// retry cannot outlive the caller's context.
func TestPushRetryStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doer := &fakeDoer{handle: func(*http.Request, int) (*http.Response, error) {
		// Cancel the context during the first attempt to model a caller that
		// gives up while the request is in flight.
		cancel()
		return nil, context.Canceled
	}}
	err := New("http://example.org", "job", WithRetryPolicy(RetryPolicy{MaxRetries: 5})).
		Client(doer).
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "cancel_counter", Help: "test help."})).
		PushContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("PushContext() = %v, want context.Canceled", err)
	}
	if got := doer.requestCount(); got != 1 {
		t.Errorf("got %d requests, want 1 (cancellation must not be retried)", got)
	}
}

// TestPushRetryRespectsContextDeadline verifies that the retry backoff stops
// when the context deadline expires and reports the deadline.
func TestPushRetryRespectsContextDeadline(t *testing.T) {
	doer := &fakeDoer{handle: func(*http.Request, int) (*http.Response, error) {
		return nil, errors.New("timeout")
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := New("http://example.org", "job", WithRetryPolicy(RetryPolicy{
		MaxRetries: 5,
		MinBackoff: time.Second,
	})).
		Client(doer).
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "deadline_counter", Help: "test help."})).
		PushContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PushContext() = %v, want context.DeadlineExceeded", err)
	}
	if got := doer.requestCount(); got != 1 {
		t.Errorf("got %d requests, want 1 (backoff must not exceed the deadline)", got)
	}
}

// TestPushTimeoutOption verifies that WithTimeout bounds a single exchange.
func TestPushTimeoutOption(t *testing.T) {
	doer := doerFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	err := New("http://example.org", "job", WithTimeout(20*time.Millisecond)).
		Client(doer).
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "timeout_counter", Help: "test help."})).
		Push()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Push() = %v, want context.DeadlineExceeded", err)
	}
}

// TestPushTimeoutSkipsRequestAfterLateGather verifies that a gather overrunning
// the timeout fails the operation without sending a request.
func TestPushTimeoutSkipsRequestAfterLateGather(t *testing.T) {
	doer := &fakeDoer{}
	slowGatherer := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		time.Sleep(100 * time.Millisecond)
		return nil, nil
	})
	err := New("http://example.org", "job", WithTimeout(20*time.Millisecond)).
		Client(doer).
		Gatherer(slowGatherer).
		Push()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Push() = %v, want context.DeadlineExceeded", err)
	}
	if got := doer.requestCount(); got != 0 {
		t.Errorf("got %d requests, want 0", got)
	}
}

// TestDeleteRetryIsIdempotent verifies that Delete retries the identical
// request without a body.
func TestDeleteRetryIsIdempotent(t *testing.T) {
	doer := &fakeDoer{handle: func(_ *http.Request, attempt int) (*http.Response, error) {
		if attempt == 0 {
			return nil, errors.New("timeout")
		}
		return statusResponse(http.StatusAccepted, ""), nil
	}}
	err := New("http://example.org", "testjob", WithRetryPolicy(RetryPolicy{MaxRetries: 2})).
		Client(doer).
		Grouping("zone", "xy").
		Delete()
	if err != nil {
		t.Fatalf("Delete() = %v, want nil", err)
	}
	if got := doer.requestCount(); got != 2 {
		t.Fatalf("got %d requests, want 2", got)
	}
	for i := range 2 {
		req := doer.requestAt(t, i)
		if got := req.Method; got != http.MethodDelete {
			t.Errorf("request %d method = %q, want DELETE", i, got)
		}
		if want := "/metrics/job/testjob/zone/xy"; req.URL.Path != want {
			t.Errorf("request %d path = %q, want %q", i, req.URL.Path, want)
		}
		if len(doer.bodyAt(t, i)) != 0 {
			t.Errorf("request %d has a body, want empty", i)
		}
	}
	if doer.closeCount() == 0 {
		t.Error("CloseIdleConnections was not called before the DELETE retry")
	}
}

// TestDeleteNoRetryOnClientError verifies that a non-retryable status code is
// returned immediately.
func TestDeleteNoRetryOnClientError(t *testing.T) {
	doer := &fakeDoer{handle: func(*http.Request, int) (*http.Response, error) {
		return statusResponse(http.StatusBadRequest, "bad"), nil
	}}
	err := New("http://example.org", "job", WithRetryPolicy(RetryPolicy{MaxRetries: 3})).
		Client(doer).
		Delete()
	if err == nil {
		t.Fatal("Delete() = nil, want status error")
	}
	if got := doer.requestCount(); got != 1 {
		t.Errorf("got %d requests, want 1 (4xx must not be retried)", got)
	}
}

// TestDeleteSuccessStatus verifies that only 202 means success for DELETE,
// matching the grouping semantics of the Pushgateway, also with retries
// enabled.
func TestDeleteSuccessStatus(t *testing.T) {
	for _, test := range []struct {
		name    string
		code    int
		wantErr bool
	}{
		{"ok", http.StatusOK, true},
		{"accepted", http.StatusAccepted, false},
		{"no content", http.StatusNoContent, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &fakeDoer{handle: func(*http.Request, int) (*http.Response, error) {
				return statusResponse(test.code, ""), nil
			}}
			err := New("http://example.org", "job", WithRetryPolicy(RetryPolicy{MaxRetries: 2})).
				Client(doer).
				Delete()
			if (err != nil) != test.wantErr {
				t.Fatalf("Delete() = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

// TestPushMethodSemantics verifies that PUT and POST keep their success
// statuses and use distinct HTTP methods.
func TestPushMethodSemantics(t *testing.T) {
	for _, test := range []struct {
		name   string
		method string
		code   int
	}{
		{"put ok", http.MethodPut, http.StatusOK},
		{"put accepted", http.MethodPut, http.StatusAccepted},
		{"post ok", http.MethodPost, http.StatusOK},
		{"post accepted", http.MethodPost, http.StatusAccepted},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &fakeDoer{handle: func(*http.Request, int) (*http.Response, error) {
				return statusResponse(test.code, ""), nil
			}}
			pusher := New("http://example.org", "job").
				Client(doer).
				Collector(prometheus.NewCounter(prometheus.CounterOpts{
					Name: "method_" + strings.ToLower(test.method) + "_counter",
					Help: "test help.",
				}))
			var err error
			if test.method == http.MethodPut {
				err = pusher.Push()
			} else {
				err = pusher.Add()
			}
			if err != nil {
				t.Fatalf("push = %v, want nil", err)
			}
			if got := doer.requestAt(t, 0).Method; got != test.method {
				t.Errorf("method = %q, want %q", got, test.method)
			}
		})
	}
}

// TestPushRetryableStatuses verifies which status codes are retried.
func TestPushRetryableStatuses(t *testing.T) {
	for _, test := range []struct {
		name         string
		code         int
		wantRequests int
		wantErr      bool
	}{
		{"ok", http.StatusOK, 1, false},
		{"accepted", http.StatusAccepted, 1, false},
		{"bad request", http.StatusBadRequest, 1, true},
		{"not found", http.StatusNotFound, 1, true},
		{"request timeout", http.StatusRequestTimeout, 2, false},
		{"too many requests", http.StatusTooManyRequests, 2, false},
		{"internal server error", http.StatusInternalServerError, 2, false},
		{"bad gateway", http.StatusBadGateway, 2, false},
		{"service unavailable", http.StatusServiceUnavailable, 2, false},
		{"gateway timeout", http.StatusGatewayTimeout, 2, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &fakeDoer{handle: func(_ *http.Request, attempt int) (*http.Response, error) {
				if attempt == 0 {
					return statusResponse(test.code, "response"), nil
				}
				return okResponse(), nil
			}}
			err := New("http://example.org", "job", WithRetryPolicy(RetryPolicy{MaxRetries: 1})).
				Client(doer).
				Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "status_counter", Help: "test help."})).
				Push()
			if got := doer.requestCount(); got != test.wantRequests {
				t.Fatalf("got %d requests, want %d", got, test.wantRequests)
			}
			if (err != nil) != test.wantErr {
				t.Fatalf("Push() = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

// TestPushRetryExhaustionReturnsLastError verifies that the last attempt's
// error is returned when every retry fails.
func TestPushRetryExhaustionReturnsLastError(t *testing.T) {
	var calls atomic.Int32
	doer := &fakeDoer{handle: func(*http.Request, int) (*http.Response, error) {
		n := calls.Add(1)
		return nil, fmt.Errorf("attempt %d failed", n)
	}}
	err := New("http://example.org", "job", WithRetryPolicy(RetryPolicy{MaxRetries: 2})).
		Client(doer).
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "exhaust_counter", Help: "test help."})).
		Push()
	if err == nil {
		t.Fatal("Push() = nil, want error")
	}
	if got, want := err.Error(), "attempt 3 failed"; got != want {
		t.Errorf("Push() error = %q, want %q", got, want)
	}
	if got := doer.requestCount(); got != 3 {
		t.Errorf("got %d requests, want 3 (one initial attempt plus two retries)", got)
	}
}

// TestPushErrorBodyIsBounded verifies that an oversized error body is truncated
// instead of being read into memory without bound.
func TestPushErrorBodyIsBounded(t *testing.T) {
	big := strings.Repeat("x", maxErrorBodyBytes*2)
	doer := &fakeDoer{handle: func(*http.Request, int) (*http.Response, error) {
		return statusResponse(http.StatusBadRequest, big), nil
	}}
	err := New("http://example.org", "job").
		Client(doer).
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "bounded_counter", Help: "test help."})).
		Push()
	if err == nil {
		t.Fatal("Push() = nil, want status error")
	}
	if got := len(err.Error()); got > maxErrorBodyBytes+256 {
		t.Errorf("error message is %d bytes, want at most about %d", got, maxErrorBodyBytes)
	}
}

// TestPusherConcurrentConfiguration verifies that configuration changes made
// concurrently with pushes are race free and do not affect in-flight requests.
func TestPusherConcurrentConfiguration(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	pusher := New(server.URL, "job").
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "concurrent_counter", Help: "test help."}))

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 5 {
				pusher.Grouping("worker", "w")
				pusher.Header(http.Header{"X-Test": {"value"}})
				pusher.BasicAuth("user", "secret")
				_ = pusher.Error()
			}
		})
	}
	for range 5 {
		wg.Go(func() {
			_ = pusher.Push()
		})
	}
	wg.Wait()
	if hits.Load() == 0 {
		t.Error("no push reached the server")
	}
}

// TestSnapshotIsIndependent verifies that mutating the Pusher after a snapshot
// does not change the snapshot.
func TestSnapshotIsIndependent(t *testing.T) {
	pusher := New("http://example.org", "job").
		Grouping("zone", "xy").
		Header(http.Header{"X-Test": {"one"}}).
		BasicAuth("user", "secret")

	snapshot, err := pusher.snapshot()
	if err != nil {
		t.Fatal(err)
	}

	pusher.Grouping("zone", "changed")
	pusher.Header(http.Header{"X-Test": {"two"}})
	pusher.BasicAuth("other", "other")

	if got := snapshot.grouping["zone"]; got != "xy" {
		t.Errorf("snapshot grouping zone = %q, want xy", got)
	}
	if got := snapshot.header.Get("X-Test"); got != "one" {
		t.Errorf("snapshot header X-Test = %q, want one", got)
	}
	if snapshot.username != "user" || snapshot.password != "secret" {
		t.Errorf("snapshot credentials = %q/%q, want user/secret", snapshot.username, snapshot.password)
	}
}

// TestGroupingURLIsDeterministic verifies that the same grouping key always
// produces the same URL, regardless of insertion order.
func TestGroupingURLIsDeterministic(t *testing.T) {
	first, err := New("http://example.org", "job").
		Grouping("b", "2").
		Grouping("a", "1").
		Grouping("c", "3").
		snapshot()
	if err != nil {
		t.Fatal(err)
	}
	second, err := New("http://example.org", "job").
		Grouping("c", "3").
		Grouping("a", "1").
		Grouping("b", "2").
		snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := first.fullURL(), "http://example.org/metrics/job/job/a/1/b/2/c/3"; got != want {
		t.Errorf("URL = %q, want %q", got, want)
	}
	if got, want := second.fullURL(), first.fullURL(); got != want {
		t.Errorf("URL depends on insertion order: %q != %q", got, want)
	}
}

// TestTransportErrorClosesIdleConnections verifies that a failed exchange
// closes pooled connections before returning.
func TestTransportErrorClosesIdleConnections(t *testing.T) {
	doer := &fakeDoer{handle: func(*http.Request, int) (*http.Response, error) {
		return nil, errors.New("connection reset")
	}}
	if err := New("http://example.org", "job").
		Client(doer).
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "closed_counter", Help: "test help."})).
		Push(); err == nil {
		t.Fatal("Push() = nil, want error")
	}
	if doer.closeCount() == 0 {
		t.Error("CloseIdleConnections was not called after a transport error")
	}
}

// TestPushRetryUsesNewConnectionOnTheWire verifies, against a real HTTP
// server, that a retried push opens a new TCP connection instead of reusing the
// connection of the failed attempt, and that both attempts carry an identical
// URL and body.
func TestPushRetryUsesNewConnectionOnTheWire(t *testing.T) {
	var (
		mu          sync.Mutex
		connections []string
		bodies      [][]byte
		paths       []string
		attempts    int
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		attempts++
		attempt := attempts
		connections = append(connections, r.RemoteAddr)
		bodies = append(bodies, body)
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if attempt == 1 {
			// Hijack and close the connection to model a connection that dies
			// in the middle of an exchange.
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("response writer does not support hijacking")
				return
			}
			conn, _, hijackErr := hj.Hijack()
			if hijackErr != nil {
				t.Errorf("hijack: %v", hijackErr)
				return
			}
			_ = conn.Close()
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	pusher := New(server.URL, "testjob", WithRetryPolicy(RetryPolicy{MaxRetries: 1})).
		Grouping("zone", "xy").
		Collector(prometheus.NewCounter(prometheus.CounterOpts{Name: "wire_counter", Help: "test help."}))
	if err := pusher.Push(); err != nil {
		t.Fatalf("Push() = %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("server saw %d requests, want 2", attempts)
	}
	for i := range 2 {
		if paths[i] != "/metrics/job/testjob/zone/xy" {
			t.Errorf("request %d path = %q, want %q", i, paths[i], "/metrics/job/testjob/zone/xy")
		}
		if !bytes.Equal(bodies[i], bodies[0]) {
			t.Errorf("request %d body differs from the first attempt", i)
		}
	}
	// The failed attempt closed its connection, so the retry cannot have
	// reused it. If it had, the server would report one connection for both.
	if connections[0] == connections[1] {
		t.Errorf("retry reused the failed connection %q", connections[0])
	}
}
