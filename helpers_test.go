package outbox

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// testKey is the raw HMAC key. testSecret is its Standard Webhooks encoding.
const testKey = "0123456789abcdef-test-key"

var testSecret = "whsec_" + base64.StdEncoding.EncodeToString([]byte(testKey))

// testNow is the fixed clock used by the tests (2023-11-14T22:13:20Z).
var testNow = time.Unix(1700000000, 0).UTC()

// verifyStandardWebhook implements the receiver side of the Standard Webhooks scheme on its own,
// without the package's signing code. It checks the signature headers against the body.
func verifyStandardWebhook(secret string, header http.Header, body []byte) error {
	raw, ok := strings.CutPrefix(secret, "whsec_")
	if !ok {
		return errors.New("verifier: secret has no whsec_ prefix")
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return fmt.Errorf("verifier: secret is not base64: %w", err)
	}
	id := header.Get("webhook-id")
	ts := header.Get("webhook-timestamp")
	if id == "" || ts == "" {
		return errors.New("verifier: webhook-id or webhook-timestamp missing")
	}
	if _, err := strconv.ParseInt(ts, 10, 64); err != nil {
		return fmt.Errorf("verifier: webhook-timestamp is not an integer: %w", err)
	}
	var env struct {
		Type      string `json:"type"`
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("verifier: body is not JSON: %w", err)
	}
	if env.Type == "" {
		return errors.New("verifier: body has no type")
	}

	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(id + "." + ts + "." + string(body)))
	want := mac.Sum(nil)

	for _, part := range strings.Fields(header.Get("webhook-signature")) {
		version, sig, found := strings.Cut(part, ",")
		if !found || version != "v1" {
			continue
		}
		got, err := base64.StdEncoding.DecodeString(sig)
		if err == nil && hmac.Equal(got, want) {
			return nil
		}
	}
	return errors.New("verifier: no v1 signature matches the body")
}

// roundTripFunc lets a test stand in for the network. No request leaves the process.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// capturedRequest is what the fake transport saw.
type capturedRequest struct {
	url    string
	header http.Header
	body   []byte
}

// scriptedResponse is one scripted answer. The last entry repeats once the script is exhausted.
type scriptedResponse struct {
	status int
	header http.Header
	err    error
}

// scripted is a fake transport that answers from a script and records every request.
type scripted struct {
	mu        sync.Mutex
	responses []scriptedResponse
	requests  []capturedRequest
}

func (s *scripted) RoundTrip(r *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, capturedRequest{url: r.URL.String(), header: r.Header.Clone(), body: body})
	idx := min(len(s.requests)-1, len(s.responses)-1)
	sr := s.responses[idx]
	if sr.err != nil {
		return nil, sr.err
	}
	return newResponse(sr.status, sr.header), nil
}

func (s *scripted) client() *http.Client {
	return &http.Client{Transport: s}
}

func (s *scripted) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *scripted) captured() []capturedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]capturedRequest(nil), s.requests...)
}

func newResponse(status int, header http.Header) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     header,
		Body:       io.NopCloser(strings.NewReader("receiver body that is never read")),
	}
}

// trackedBody records how the sender touched the response body.
type trackedBody struct {
	reads  int
	closes int
}

func (b *trackedBody) Read(p []byte) (int, error) {
	b.reads++
	return 0, io.EOF
}

func (b *trackedBody) Close() error {
	b.closes++
	return nil
}

// sleepRecorder replaces the real sleep and records the requested delays.
type sleepRecorder struct {
	delays []time.Duration
}

func (r *sleepRecorder) sleep(ctx context.Context, d time.Duration) error {
	r.delays = append(r.delays, d)
	return ctx.Err()
}

// testOptions returns deterministic options: fixed clock, jitter factor 0.5, recorded sleep.
// Options passed by the caller are applied after these and override them.
func testOptions(rec *sleepRecorder, extra ...Option) []Option {
	opts := []Option{
		WithClock(func() time.Time { return testNow }),
		WithRandom(func() float64 { return 0.5 }),
		WithSleep(rec.sleep),
	}
	return append(opts, extra...)
}

// newTestSender creates a Sender with the given client and deterministic options.
func newTestSender(t *testing.T, client *http.Client, rec *sleepRecorder, extra ...Option) *Sender {
	t.Helper()
	opts := append([]Option{WithHTTPClient(client)}, testOptions(rec, extra...)...)
	s, err := NewSender(testSecret, opts...)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	return s
}

// makeHeader builds an http.Header from alternating key and value strings.
func makeHeader(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

// testEvent is a valid event with a fixed ID and timestamp.
func testEvent() Event {
	return Event{
		Type:      "invoice.paid",
		Data:      map[string]any{"amount": 4200},
		ID:        "msg_1",
		Timestamp: testNow,
	}
}

const testURL = "https://hooks.example.test/in"
