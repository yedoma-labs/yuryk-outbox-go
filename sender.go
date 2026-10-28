// Package outbox sends signed outbound webhooks with correct retry semantics.
//
// Each event is serialized once, and its ID, timestamp and signature are fixed before the first
// attempt. Every retry sends the same bytes and headers, so receivers can deduplicate by
// webhook-id. The package signs deliveries with the Standard Webhooks scheme and depends only on
// the standard library. It is the counterpart to receiving webhooks and does not store events or
// run queues.
package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Event is a webhook to deliver.
type Event struct {
	// Type is the event type, for example "invoice.paid". It is required.
	Type string
	// Data is serialized once with encoding/json.
	Data any
	// ID identifies the event across retries. If empty, a random UUIDv4 is generated once per
	// Send call.
	ID string
	// Timestamp is the event time. If zero, the sender's clock is used.
	Timestamp time.Time
}

// Result describes the outcome of Send. A failed delivery is reported here with OK set to false
// and a nil error, unless the failure is a configuration problem or a dead-letter callback error.
type Result struct {
	// OK is true if the receiver answered with a 2xx status.
	OK bool
	// Attempts is the number of HTTP requests made.
	Attempts int
	// Status is the status of the last response, or 0 if the last attempt got no response.
	Status int
}

// DeadLetter describes an event whose delivery failed for good.
type DeadLetter struct {
	// URL is the destination the event was sent to.
	URL string
	// Event is the delivered event with its ID and Timestamp resolved.
	Event Event
	// ID is the webhook-id that every attempt used.
	ID string
	// Attempts is the number of HTTP requests made.
	Attempts int
	// LastStatus is the status of the last response, or 0 if the last attempt got no response.
	LastStatus int
	// LastError is the transport error of the last attempt, or nil if it got a response.
	LastError error
}

// Sender delivers signed webhooks. It holds no per-delivery state, so one Sender can be used
// concurrently from several goroutines.
type Sender struct {
	key               []byte
	client            *http.Client
	maxAttempts       int
	baseBackoff       time.Duration
	maxBackoff        time.Duration
	random            func() float64
	clock             func() time.Time
	sleep             SleepFunc
	onDeadLetter      DeadLetterFunc
	allowInsecureHTTP bool
}

// envelope is the JSON body sent to receivers.
type envelope struct {
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
	Data      any    `json:"data"`
}

// NewSender creates a Sender for the given secret, which must have the format whsec_<base64>.
// It returns an error wrapping ErrMalformedSecret if the secret is malformed, and an *Error with
// CodeInvalidOptions if an option is invalid.
func NewSender(secret string, opts ...Option) (*Sender, error) {
	key, err := decodeSecret(secret)
	if err != nil {
		return nil, err
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt == nil {
			return nil, invalidOptions("option must not be nil")
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	// Redirects are never followed: a redirect would send the signed request to a location that
	// was not validated. The caller's client is copied, so it is not modified.
	client := *cfg.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Sender{
		key:               key,
		client:            &client,
		maxAttempts:       cfg.maxAttempts,
		baseBackoff:       cfg.baseBackoff,
		maxBackoff:        cfg.maxBackoff,
		random:            cfg.random,
		clock:             cfg.clock,
		sleep:             cfg.sleep,
		onDeadLetter:      cfg.onDeadLetter,
		allowInsecureHTTP: cfg.allowInsecureHTTP,
	}, nil
}

// Send delivers ev to url, retrying according to the retry rules of the package.
//
// It returns a Result with OK set to true on a 2xx response. A failed delivery returns OK false
// and a nil error after the dead-letter callback, if configured, has run. A configuration or
// input problem returns an *Error. If ctx is canceled, Send returns ctx.Err() and does not call
// the dead-letter callback.
func (s *Sender) Send(ctx context.Context, rawURL string, ev Event) (Result, error) {
	if err := s.checkURL(rawURL); err != nil {
		return Result{}, err
	}
	if ev.Type == "" {
		return Result{}, newError(CodeInvalidEvent, "event type must not be empty", nil)
	}
	if ev.ID == "" {
		id, err := newUUID()
		if err != nil {
			return Result{}, fmt.Errorf("outbox: generate event id: %w", err)
		}
		ev.ID = id
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = s.clock()
	}
	secs := ev.Timestamp.Unix()

	// Serialized and signed exactly once. The retries below reuse these bytes.
	body, err := encodeBody(ev.Type, secs, ev.Data)
	if err != nil {
		return Result{}, err
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	headers.Set("Webhook-Id", ev.ID)
	headers.Set("Webhook-Timestamp", strconv.FormatInt(secs, 10))
	headers.Set("Webhook-Signature", sign(s.key, ev.ID, secs, body))

	var res Result
	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		res.Attempts++
		status, retryAfter, hasRetryAfter, attemptErr := s.attempt(ctx, rawURL, headers, body)
		res.Status = status
		if attemptErr == nil && isSuccess(status) {
			res.OK = true
			return res, nil
		}
		if attemptErr != nil && ctx.Err() != nil {
			return res, ctx.Err()
		}

		retryable := attemptErr != nil || isRetryableStatus(status)
		if !retryable || res.Attempts >= s.maxAttempts {
			return s.deadLetter(ctx, rawURL, ev, res, attemptErr)
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return res, ctxErr
		}
		// Retry-After takes precedence over backoff, but is capped to the maximum delay.
		delay := backoffDelay(res.Attempts, s.baseBackoff, s.maxBackoff, s.random)
		if hasRetryAfter {
			delay = min(retryAfter, s.maxBackoff)
		}
		if err := s.sleep(ctx, delay); err != nil {
			return res, err
		}
	}
}

// attempt performs one HTTP request. It returns the status code and the parsed Retry-After
// delay, or a transport error if no response was received. The response body is never read.
func (s *Sender) attempt(ctx context.Context, target string, headers http.Header, body []byte) (int, time.Duration, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return 0, 0, false, newError(CodeInvalidURL, "destination URL could not be used", nil)
	}
	req.Header = headers.Clone()

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, 0, false, err
	}
	// The receiver's body is discarded without reading it. Closing releases the connection.
	defer func() { _ = resp.Body.Close() }()

	retryAfter, ok := parseRetryAfter(resp.Header.Get("Retry-After"), s.clock())
	return resp.StatusCode, retryAfter, ok, nil
}

// deadLetter calls the dead-letter callback once, if one is configured.
func (s *Sender) deadLetter(ctx context.Context, target string, ev Event, res Result, lastErr error) (Result, error) {
	if s.onDeadLetter == nil {
		return res, nil
	}
	letter := DeadLetter{
		URL:        target,
		Event:      ev,
		ID:         ev.ID,
		Attempts:   res.Attempts,
		LastStatus: res.Status,
		LastError:  lastErr,
	}
	if err := s.onDeadLetter(ctx, letter); err != nil {
		return res, newError(CodeDeadLetterCallback,
			fmt.Sprintf("dead-letter callback failed after %d attempt(s)", res.Attempts), err)
	}
	return res, nil
}

// checkURL validates the destination. The URL is not echoed in error messages, since it may
// carry tokens.
func (s *Sender) checkURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return newError(CodeInvalidURL, "destination URL must be an absolute URL", nil)
	}
	if u.User != nil {
		return newError(CodeInvalidURL, "destination URL must not contain credentials", nil)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if s.allowInsecureHTTP {
			return nil
		}
		return newError(CodeInsecureURL,
			"destination URL must use https (WithAllowInsecureHTTP allows http for local development)", nil)
	default:
		return newError(CodeInvalidURL, "destination URL must use https", nil)
	}
}

// encodeBody serializes the Standard Webhooks payload for one event.
func encodeBody(eventType string, secs int64, data any) ([]byte, error) {
	body, err := json.Marshal(envelope{
		Type:      eventType,
		Timestamp: time.Unix(secs, 0).UTC().Format(time.RFC3339),
		Data:      data,
	})
	if err != nil {
		return nil, newError(CodeInvalidEvent, "event data cannot be serialized to JSON", err)
	}
	return body, nil
}
