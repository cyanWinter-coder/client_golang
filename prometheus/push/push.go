// Copyright 2015 The Prometheus Authors
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

// Package push provides functions to push metrics to a Pushgateway. It uses a
// builder approach. Create a Pusher with New and then add the various options
// by using its methods, finally calling Add or Push, like this:
//
//	// Easy case:
//	push.New("http://example.org/metrics", "my_job").Gatherer(myRegistry).Push()
//
//	// Complex case:
//	push.New("http://example.org/metrics", "my_job").
//	    Collector(myCollector1).
//	    Collector(myCollector2).
//	    Grouping("zone", "xy").
//	    Client(&myHTTPClient).
//	    BasicAuth("top", "secret").
//	    Add()
//
// See the examples section for more detailed examples.
//
// See the documentation of the Pushgateway to understand the meaning of
// the grouping key and the differences between Push and Add:
// https://github.com/prometheus/pushgateway
package push

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	contentTypeHeader = "Content-Type"
	// base64Suffix is appended to a label name in the request URL path to
	// mark the following label value as base64 encoded.
	base64Suffix = "@base64"

	// maxErrorBodyBytes bounds how much of an error response body is kept for
	// the returned error message. Without a bound, a misbehaving gateway could
	// force every failed push to buffer an arbitrarily large body.
	maxErrorBodyBytes = 64 << 10
)

var (
	errJobEmpty           = errors.New("job name is empty")
	errNegativeTimeout    = errors.New("timeout must not be negative")
	errInvalidRetryPolicy = errors.New("invalid retry policy")
)

// HTTPDoer is an interface for the one method of http.Client that is used by Pusher
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// RetryPolicy controls how a Pusher retries failed HTTP exchanges. Its zero
// value disables retries and keeps the historical single-attempt behavior.
//
// Every attempt of an operation is built from one immutable snapshot that is
// captured when the operation starts: the grouping key, the encoded request
// body, the request headers, the credentials, and the HTTP client. Consequently
// a retry can never mix values configured after the operation started into a
// later attempt.
//
// A Pushgateway applies a push only after it has received the full request
// body, and every attempt carries the same body. Retrying is therefore safe
// for all supported methods: Push (PUT) replaces the same metric group, Add
// (POST) merges the same metric families, and Delete (DELETE) removes the same
// group.
type RetryPolicy struct {
	// MaxRetries is the number of attempts after the first one. A zero or
	// negative value disables retries.
	MaxRetries int
	// MinBackoff is the delay before the first retry. Every further retry
	// doubles the delay, up to MaxBackoff. A zero value retries immediately.
	MinBackoff time.Duration
	// MaxBackoff caps the delay between retries. A zero value leaves the
	// delay uncapped.
	MaxBackoff time.Duration
}

// DefaultRetryPolicy returns a conservative retry policy suitable for a
// Pushgateway. It retries up to three times with an exponential backoff
// between 100ms and 2s.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries: 3,
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
	}
}

// validate reports whether the policy can be applied.
func (p RetryPolicy) validate() error {
	if p.MaxRetries < 0 {
		return fmt.Errorf("%w: MaxRetries must not be negative", errInvalidRetryPolicy)
	}
	if p.MinBackoff < 0 {
		return fmt.Errorf("%w: MinBackoff must not be negative", errInvalidRetryPolicy)
	}
	if p.MaxBackoff < 0 {
		return fmt.Errorf("%w: MaxBackoff must not be negative", errInvalidRetryPolicy)
	}
	if p.MaxBackoff > 0 && p.MinBackoff > p.MaxBackoff {
		return fmt.Errorf("%w: MinBackoff must not exceed MaxBackoff", errInvalidRetryPolicy)
	}
	return nil
}

// backoff returns the delay before the retry with the provided one-based
// attempt number. Retrying immediately is reported as a zero duration.
func (p RetryPolicy) backoff(attempt int) time.Duration {
	if p.MinBackoff <= 0 {
		return 0
	}
	d := p.MinBackoff
	for i := 1; i < attempt; i++ {
		if p.MaxBackoff > 0 && d >= p.MaxBackoff {
			return p.MaxBackoff
		}
		d *= 2
		if d <= 0 {
			// Guard against overflow for very large attempt numbers.
			if p.MaxBackoff > 0 {
				return p.MaxBackoff
			}
			return time.Duration(1<<63 - 1)
		}
	}
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		return p.MaxBackoff
	}
	return d
}

// retryableStatus reports whether a response with the provided status code is
// worth retrying.
func retryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// PusherOption configures a Pusher at construction time. New applies every
// option once before the Pusher is returned. An option must not retain the
// Pusher or be reused after it has been passed to New.
type PusherOption func(*Pusher)

// WithRetryPolicy configures how the Pusher retries failed HTTP exchanges.
// Retries reuse the immutable snapshot captured at the start of the operation,
// so a retry never sends a different grouping key, body, header, or credential
// set than the first attempt. Failed exchanges close the connections they used,
// and every retry is sent on a fresh connection (with "Connection: close"), so
// an attempt that died mid-upload cannot hand its connection state to a retry.
func WithRetryPolicy(policy RetryPolicy) PusherOption {
	return func(p *Pusher) {
		if p.error != nil {
			return
		}
		if err := policy.validate(); err != nil {
			p.error = err
			return
		}
		p.retry = policy
	}
}

// WithTimeout bounds an operation with the provided timeout. A zero timeout
// disables the bound (the default). The timeout is attached to the request
// context, so it covers every HTTP attempt and the backoff delays between
// them. Gathering and encoding run under the same deadline: if they overrun it,
// the operation fails with a deadline error and no request is sent. Note that
// Gather itself cannot be cancelled, so a slow Gatherer still delays the
// failure until it returns. If the context passed to PushContext, AddContext,
// or DeleteContext has an earlier deadline, that deadline wins.
func WithTimeout(timeout time.Duration) PusherOption {
	return func(p *Pusher) {
		if p.error != nil {
			return
		}
		if timeout < 0 {
			p.error = fmt.Errorf("%w: %s", errNegativeTimeout, timeout)
			return
		}
		p.timeout = timeout
	}
}

// Pusher manages a push to the Pushgateway. Use New to create one, configure it
// with its methods, and finally use the Add or Push method to push.
//
// A Pusher is safe for concurrent use. Configuration methods and operations may
// be called from different goroutines: each operation captures an immutable
// snapshot of the configuration when it starts, so configuration changes made
// while an operation is in flight only affect later operations.
type Pusher struct {
	mu    sync.RWMutex
	error error

	url, job string
	grouping map[string]string

	gatherers  prometheus.Gatherers
	registerer prometheus.Registerer

	client             HTTPDoer
	header             http.Header
	useBasicAuth       bool
	username, password string

	expfmt expfmt.Format

	timeout time.Duration
	retry   RetryPolicy
}

// New creates a new Pusher to push to the provided URL with the provided job
// name (which must not be empty). You can use just host:port or ip:port as url,
// in which case “http://” is added automatically. Alternatively, include the
// schema in the URL. However, do not include the “/metrics/jobs/…” part.
//
// The optional options configure behavior that is not covered by the builder
// methods, such as retries and an operation timeout. They are applied once,
// before New returns.
func New(url, job string, opts ...PusherOption) *Pusher {
	var (
		reg = prometheus.NewRegistry()
		err error
	)
	if job == "" {
		err = errJobEmpty
	}
	if !strings.Contains(url, "://") {
		url = "http://" + url
	}
	url = strings.TrimSuffix(url, "/")

	p := &Pusher{
		error:      err,
		url:        url,
		job:        job,
		grouping:   map[string]string{},
		gatherers:  prometheus.Gatherers{reg},
		registerer: reg,
		client:     &http.Client{},
		expfmt:     expfmt.NewFormat(expfmt.TypeProtoDelim),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	return p
}

// Push collects/gathers all metrics from all Collectors and Gatherers added to
// this Pusher. Then, it pushes them to the Pushgateway configured while
// creating this Pusher, using the configured job name and any added grouping
// labels as grouping key. All previously pushed metrics with the same job and
// other grouping labels will be replaced with the metrics pushed by this
// call. (It uses HTTP method “PUT” to push to the Pushgateway.)
//
// The grouping key, the encoded body, the headers, the credentials, and the
// client are captured when Push starts. Changing the Pusher while the request
// is in flight does not affect it, and retries send exactly the same request.
//
// Push returns the first error encountered by any method call (including this
// one) in the lifetime of the Pusher.
func (p *Pusher) Push() error {
	return p.push(context.Background(), http.MethodPut)
}

// PushContext is like Push but includes a context.
//
// If the context expires before HTTP request is complete, an error is returned.
//
// As metrics are often pushed on completion of a job, and jobs may
// complete due to cancellation of their context, take care not to use
// the potentially canceled context as the argument to this function.
// You can reset a context's timeout with [context.WithoutCancel],
// e.g.
//
//	p.PushContext(context.WithTimeout(context.WithoutCancel(ctx), time.Second))
func (p *Pusher) PushContext(ctx context.Context) error {
	return p.push(ctx, http.MethodPut)
}

// Add works like push, but only previously pushed metrics with the same name
// (and the same job and other grouping labels) will be replaced. (It uses HTTP
// method “POST” to push to the Pushgateway.)
//
// Like Push, Add captures the grouping key, body, headers, credentials, and
// client when it starts, and a retry re-sends the identical merge request.
func (p *Pusher) Add() error {
	return p.push(context.Background(), http.MethodPost)
}

// AddContext is like Add but includes a context.
//
// If the context expires before HTTP request is complete, an error is returned.
func (p *Pusher) AddContext(ctx context.Context) error {
	return p.push(ctx, http.MethodPost)
}

// Gatherer adds a Gatherer to the Pusher, from which metrics will be gathered
// to push them to the Pushgateway. The gathered metrics must not contain a job
// label of their own.
//
// For convenience, this method returns a pointer to the Pusher itself.
func (p *Pusher) Gatherer(g prometheus.Gatherer) *Pusher {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gatherers = append(p.gatherers, g)
	return p
}

// Collector adds a Collector to the Pusher, from which metrics will be
// collected to push them to the Pushgateway. The collected metrics must not
// contain a job label of their own.
//
// For convenience, this method returns a pointer to the Pusher itself.
func (p *Pusher) Collector(c prometheus.Collector) *Pusher {
	p.mu.RLock()
	err := p.error
	registerer := p.registerer
	p.mu.RUnlock()
	if err != nil {
		// The first error of the Pusher wins, matching the historical
		// behavior of not registering Collectors after an error.
		return p
	}
	// Registration calls Describe on the Collector, which may run user code.
	// It therefore happens outside the Pusher lock.
	if err := registerer.Register(c); err != nil {
		p.mu.Lock()
		if p.error == nil {
			p.error = err
		}
		p.mu.Unlock()
	}
	return p
}

// Error returns the error that was encountered.
func (p *Pusher) Error() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.error
}

// Grouping adds a label pair to the grouping key of the Pusher, replacing any
// previously added label pair with the same label name. Note that setting any
// labels in the grouping key that are already contained in the metrics to push
// will lead to an error.
//
// The grouping key is captured when an operation starts, so calling Grouping
// while a push is in flight does not change the request already sent. The
// Pusher never retains the caller's map or slice.
//
// For convenience, this method returns a pointer to the Pusher itself.
func (p *Pusher) Grouping(name, value string) *Pusher {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.error == nil {
		if !model.UTF8Validation.IsValidLabelName(name) {
			p.error = fmt.Errorf("grouping label has invalid name: %s", name)
			return p
		}
		p.grouping[name] = value
	}
	return p
}

// Client sets a custom HTTP client for the Pusher. For convenience, this method
// returns a pointer to the Pusher itself.
// Pusher only needs one method of the custom HTTP client: Do(*http.Request).
// Thus, rather than requiring a fully fledged http.Client,
// the provided client only needs to implement the HTTPDoer interface.
// Since *http.Client naturally implements that interface, it can still be used normally.
//
// The client is captured when an operation starts, so replacing it while a
// push is in flight does not affect that push.
func (p *Pusher) Client(c HTTPDoer) *Pusher {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.client = c
	return p
}

// Header sets a custom HTTP header for the Pusher's client. The header is
// copied into each request and is not modified by the Pusher. For convenience,
// this method returns a pointer to the Pusher itself.
func (p *Pusher) Header(header http.Header) *Pusher {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.header = header
	return p
}

// BasicAuth configures the Pusher to use HTTP Basic Authentication with the
// provided username and password. For convenience, this method returns a
// pointer to the Pusher itself.
//
// The credentials are captured when an operation starts, so changing them
// while a push is in flight does not affect that push.
func (p *Pusher) BasicAuth(username, password string) *Pusher {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.useBasicAuth = true
	p.username = username
	p.password = password
	return p
}

// Format configures the Pusher to use an encoding format given by the
// provided expfmt.Format. The default format is expfmt.FmtProtoDelim and
// should be used with the standard Prometheus Pushgateway. Custom
// implementations may require different formats. For convenience, this
// method returns a pointer to the Pusher itself.
func (p *Pusher) Format(format expfmt.Format) *Pusher {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expfmt = format
	return p
}

// Delete sends a “DELETE” request to the Pushgateway configured while creating
// this Pusher, using the configured job name and any added grouping labels as
// grouping key. Any added Gatherers and Collectors added to this Pusher are
// ignored by this method.
//
// The grouping key, headers, credentials, and client are captured when Delete
// starts. Because DELETE is idempotent, a retry after a timeout or a retryable
// status repeats the same request against the same grouping key.
//
// Delete returns the first error encountered by any method call (including this
// one) in the lifetime of the Pusher.
func (p *Pusher) Delete() error {
	return p.delete(context.Background())
}

// DeleteContext is like Delete but includes a context.
//
// If the context expires before the HTTP request is complete, an error is
// returned.
func (p *Pusher) DeleteContext(ctx context.Context) error {
	return p.delete(ctx)
}

func (p *Pusher) delete(ctx context.Context) error {
	snapshot, err := p.snapshot()
	if err != nil {
		return err
	}
	ctx, cancel := snapshot.applyTimeout(ctx)
	defer cancel()
	return snapshot.do(ctx, http.MethodDelete, nil)
}

// push collects, encodes, and sends the metrics using the immutable snapshot
// taken at the start of the operation. The snapshot is taken before gathering
// so that the grouping-label check, the encoded body, the URL, and the headers
// all refer to the same configuration generation, even if the Pusher is
// reconfigured concurrently.
func (p *Pusher) push(ctx context.Context, method string) error {
	snapshot, err := p.snapshot()
	if err != nil {
		return err
	}
	ctx, cancel := snapshot.applyTimeout(ctx)
	defer cancel()
	mfs, err := snapshot.gatherers.Gather()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	body, err := encodeMetricFamilies(mfs, snapshot.grouping, snapshot.expfmt)
	if err != nil {
		return err
	}
	return snapshot.do(ctx, method, body)
}

// pushSnapshot is an immutable copy of the Pusher configuration taken at the
// start of an operation. All attempts of that operation, including retries,
// are built from this snapshot and never read the Pusher again.
type pushSnapshot struct {
	url, job string
	grouping map[string]string

	gatherers prometheus.Gatherers

	client             HTTPDoer
	header             http.Header
	useBasicAuth       bool
	username, password string

	expfmt expfmt.Format

	timeout time.Duration
	retry   RetryPolicy
}

// snapshot copies the current configuration of the Pusher. The caller owns the
// returned snapshot, including its grouping map and header, and may use it
// without holding the Pusher lock.
func (p *Pusher) snapshot() (*pushSnapshot, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.error != nil {
		return nil, p.error
	}
	snapshot := &pushSnapshot{
		url:          p.url,
		job:          p.job,
		grouping:     make(map[string]string, len(p.grouping)),
		gatherers:    slices.Clone(p.gatherers),
		client:       p.client,
		useBasicAuth: p.useBasicAuth,
		username:     p.username,
		password:     p.password,
		expfmt:       p.expfmt,
		timeout:      p.timeout,
		retry:        p.retry,
	}
	maps.Copy(snapshot.grouping, p.grouping)
	if p.header != nil {
		snapshot.header = p.header.Clone()
	}
	return snapshot, nil
}

// applyTimeout derives the context used for the whole operation. The returned
// cancel function is never nil so callers can always defer it.
func (s *pushSnapshot) applyTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.timeout)
}

// encodeMetricFamilies validates the gathered metric families against the
// grouping key and encodes them with the provided format. The returned body is
// owned by the caller and is reused unchanged for every attempt.
func encodeMetricFamilies(mfs []*dto.MetricFamily, grouping map[string]string, format expfmt.Format) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := expfmt.NewEncoder(buf, format)
	// Check for pre-existing grouping labels.
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "job" {
					return nil, fmt.Errorf("pushed metric %s (%s) already contains a job label", mf.GetName(), m)
				}
				if _, ok := grouping[l.GetName()]; ok {
					return nil, fmt.Errorf(
						"pushed metric %s (%s) already contains grouping label %s",
						mf.GetName(), m, l.GetName(),
					)
				}
			}
		}
		if err := enc.Encode(mf); err != nil {
			return nil, fmt.Errorf(
				"failed to encode metric family %s, error is %w",
				mf.GetName(), err,
			)
		}
	}
	if closer, ok := enc.(expfmt.Closer); ok {
		if err := closer.Close(); err != nil {
			return nil, fmt.Errorf("failed to finalize metrics, error is %w", err)
		}
	}
	return buf.Bytes(), nil
}

// do performs the operation, retrying according to the snapshot's policy. The
// request URL and body are computed once and reused for every attempt.
func (s *pushSnapshot) do(ctx context.Context, method string, body []byte) error {
	url := s.fullURL()
	maxRetries := s.retry.MaxRetries
	var lastErr error
	// attempt counts from zero and is compared with maxRetries instead of
	// precomputing maxRetries+1, so a very large MaxRetries cannot overflow.
	for attempt := 0; ; attempt++ {
		fresh := attempt > 0
		retryable, err := s.attempt(ctx, method, url, body, fresh)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryable || attempt >= maxRetries {
			return lastErr
		}
		// Do not reuse any connection that carried the failed attempt. The
		// transport errors already closed them, but a retryable status code
		// leaves an otherwise healthy connection in the pool, so close the idle
		// connections here as well. The retry itself is additionally sent with
		// "Connection: close", so no connection state from the failed attempt
		// can reach the retried request.
		closeIdleConnections(s.client)
		if delay := s.retry.backoff(attempt + 1); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return errors.Join(lastErr, ctx.Err())
			case <-timer.C:
			}
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(lastErr, err)
		}
	}
}

// attempt performs exactly one HTTP exchange. It reports whether a retry is
// advisable together with the error of the attempt. The returned errors keep
// the historical messages of the package.
//
// When fresh is true, the request is sent with "Connection: close" so that it
// cannot share a connection with a previous, failed attempt. The body is
// wrapped in a new reader on every attempt, so all attempts send exactly the
// same bytes.
func (s *pushSnapshot) attempt(ctx context.Context, method, url string, body []byte, fresh bool) (bool, error) {
	var reader io.Reader
	if body != nil {
		// A fresh reader per attempt makes the request replayable and keeps
		// every attempt byte-for-byte identical.
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return false, err
	}
	if fresh {
		req.Close = true
	}
	if s.header != nil {
		req.Header = s.header.Clone()
	}
	if s.useBasicAuth {
		req.SetBasicAuth(s.username, s.password)
	}
	if method != http.MethodDelete {
		// Preserve the historical behavior of always declaring the wire format
		// of a push, even when the gathered set is empty.
		req.Header.Set(contentTypeHeader, string(s.expfmt))
	}

	resp, doErr := s.client.Do(req)
	if doErr != nil {
		if resp != nil && resp.Body != nil {
			// Close the body without draining it. Draining an incomplete
			// response could return a half-consumed connection to the pool.
			_ = resp.Body.Close()
		}
		// The transport state is unknown after a transport error: the request
		// body may have been only partially written before the failure, so the
		// connection must not be handed to a later request.
		// CloseIdleConnections does not interrupt in-flight requests, but it
		// drops whatever the transport had already returned to the idle pool.
		// This also runs for cancellation and deadline expiry, so a connection
		// abandoned mid-upload is not kept around until a later request happens
		// to reuse it.
		closeIdleConnections(s.client)
		if ctx.Err() != nil {
			// Cancellation and deadline expiry are final; retrying would only
			// repeat the same failure.
			return false, doErr
		}
		return true, doErr
	}
	if resp.Body == nil {
		return false, fmt.Errorf("unexpected empty response body from %s", url)
	}

	if successStatus(method, resp.StatusCode) {
		// Close without reading the rest of the body, matching the historical
		// behavior. The server already received the complete request, so this
		// connection holds no half-uploaded request state; http.Transport
		// simply does not reuse a connection whose body was not drained.
		_ = resp.Body.Close()
		return false, nil
	}

	// Read a bounded amount for the error message. A response means the server
	// received the whole request, so the connection is not half-uploaded. If
	// the body already ended, the connection is clean and may return to the
	// pool on Close. If it did not, Close discards the connection.
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	_ = resp.Body.Close()
	return retryableStatus(resp.StatusCode), statusError(method, resp.StatusCode, bodyBytes, url)
}

// successStatus reports whether the status code is a successful answer for the
// provided HTTP method. Depending on version and configuration of the
// Pushgateway, StatusOK or StatusAccepted may be returned for pushes, while
// deletes are answered with StatusAccepted.
func successStatus(method string, code int) bool {
	if method == http.MethodDelete {
		return code == http.StatusAccepted
	}
	return code == http.StatusOK || code == http.StatusAccepted
}

// statusError builds the error for an unexpected status code, preserving the
// message format used by earlier versions of the package.
func statusError(method string, code int, body []byte, url string) error {
	if method == http.MethodDelete {
		return fmt.Errorf("unexpected status code %d while deleting %s: %s", code, url, body)
	}
	return fmt.Errorf("unexpected status code %d while pushing to %s: %s", code, url, body)
}

// closeIdleConnections closes the idle connections of the provided client if it
// supports that operation. Both *http.Client and *http.Transport do. Custom
// HTTPDoer implementations that do not support it are left untouched.
func closeIdleConnections(client HTTPDoer) {
	if closer, ok := client.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// fullURL assembles the URL used to push/delete metrics and returns it as a
// string. The job name and any grouping label values containing a '/' or equal
// to '.' or '..' will trigger a base64 encoding of the affected component and
// proper suffixing of the preceding component. Similarly, an empty grouping
// label value will be encoded as base64 just with a single `=` padding character
// (to avoid an empty path component). Other special characters use url.PathEscape for
// compatibility with older versions of the Pushgateway and better readability.
//
// Grouping label names are sorted so that the same grouping key always
// produces the same URL. This makes repeated and retried requests identical.
func (s *pushSnapshot) fullURL() string {
	urlComponents := []string{}
	if encodedJob, base64 := encodeComponent(s.job); base64 {
		urlComponents = append(urlComponents, "job"+base64Suffix, encodedJob)
	} else {
		urlComponents = append(urlComponents, "job", encodedJob)
	}
	labelNames := make([]string, 0, len(s.grouping))
	for labelName := range s.grouping {
		labelNames = append(labelNames, labelName)
	}
	slices.Sort(labelNames)
	for _, labelName := range labelNames {
		labelValue := s.grouping[labelName]
		if encodedLV, base64 := encodeComponent(labelValue); base64 {
			urlComponents = append(urlComponents, labelName+base64Suffix, encodedLV)
		} else {
			urlComponents = append(urlComponents, labelName, encodedLV)
		}
	}
	return fmt.Sprintf("%s/metrics/%s", s.url, strings.Join(urlComponents, "/"))
}

// encodeComponent encodes the provided string with base64.RawURLEncoding in
// case it contains '/' or equals '.' or '..', and as "=" in case it is empty.
// Otherwise, it uses url.PathEscape. It returns true when base64 encoding is used.
func encodeComponent(s string) (string, bool) {
	if s == "" {
		return "=", true
	}
	if strings.Contains(s, "/") || s == "." || s == ".." {
		return base64.RawURLEncoding.EncodeToString([]byte(s)), true
	}
	return url.PathEscape(s), false
}
