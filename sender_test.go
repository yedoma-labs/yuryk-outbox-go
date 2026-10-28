package outbox

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// letterLog collects dead letters delivered to the callback.
type letterLog struct {
	letters []DeadLetter
}

func (l *letterLog) record(_ context.Context, letter DeadLetter) error {
	l.letters = append(l.letters, letter)
	return nil
}

// sendOK returns a transport that always answers 200.
func sendOK() *scripted {
	return &scripted{responses: []scriptedResponse{{status: http.StatusOK}}}
}

func TestSendURLValidation(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		insecure bool
		wantErr  error // nil means the request is sent and succeeds
	}{
		{name: "https accepted", url: "https://hooks.example.test/in"},
		{name: "http rejected by default", url: "http://hooks.example.test/in", wantErr: ErrInsecureURL},
		{name: "http accepted with opt-in", url: "http://hooks.example.test/in", insecure: true},
		{name: "ftp rejected", url: "ftp://hooks.example.test/in", wantErr: ErrInvalidURL},
		{name: "relative rejected", url: "/hooks/in", wantErr: ErrInvalidURL},
		{name: "empty rejected", url: "", wantErr: ErrInvalidURL},
		{name: "unparsable rejected", url: "https://[::1", wantErr: ErrInvalidURL},
		{name: "missing host rejected", url: "https:///in", wantErr: ErrInvalidURL},
		{name: "credentials rejected", url: "https://user:secret-pass@hooks.example.test/in", wantErr: ErrInvalidURL},
		{name: "empty userinfo rejected", url: "https://@hooks.example.test/in", wantErr: ErrInvalidURL},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := sendOK()
			var opts []Option
			if tc.insecure {
				opts = append(opts, WithAllowInsecureHTTP())
			}
			s := newTestSender(t, transport.client(), &sleepRecorder{}, opts...)

			res, err := s.Send(context.Background(), tc.url, testEvent())
			if tc.wantErr == nil {
				if err != nil || !res.OK {
					t.Fatalf("Send = (%+v, %v), want OK", res, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Send error = %v, want %v", err, tc.wantErr)
			}
			if transport.count() != 0 {
				t.Fatalf("a request was sent for an invalid URL")
			}
			if tc.url != "" && strings.Contains(err.Error(), tc.url) {
				t.Fatalf("error message echoes the URL: %q", err.Error())
			}
		})
	}
}

func TestSendEventValidation(t *testing.T) {
	tests := []struct {
		name    string
		ev      Event
		wantErr error
	}{
		{name: "empty type", ev: Event{Data: map[string]any{"a": 1}}, wantErr: ErrInvalidEvent},
		{name: "func data not serializable", ev: Event{Type: "x", Data: func() {}}, wantErr: ErrInvalidEvent},
		{name: "channel data not serializable", ev: Event{Type: "x", Data: make(chan int)}, wantErr: ErrInvalidEvent},
		{name: "nil data allowed", ev: Event{Type: "x"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := sendOK()
			s := newTestSender(t, transport.client(), &sleepRecorder{})
			res, err := s.Send(context.Background(), testURL, tc.ev)
			if tc.wantErr == nil {
				if err != nil || !res.OK {
					t.Fatalf("Send = (%+v, %v), want OK", res, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Send error = %v, want %v", err, tc.wantErr)
			}
			if transport.count() != 0 {
				t.Fatal("a request was sent for an invalid event")
			}
		})
	}
}

func TestSendStatusClasses(t *testing.T) {
	const maxAttempts = 3
	redirect := makeHeader("Location", "https://elsewhere.test/in")
	tests := []struct {
		name         string
		status       int
		header       http.Header
		wantOK       bool
		wantAttempts int
	}{
		{name: "200 succeeds", status: 200, wantOK: true, wantAttempts: 1},
		{name: "201 succeeds", status: 201, wantOK: true, wantAttempts: 1},
		{name: "202 succeeds", status: 202, wantOK: true, wantAttempts: 1},
		{name: "204 succeeds", status: 204, wantOK: true, wantAttempts: 1},
		{name: "301 not followed", status: 301, header: redirect, wantAttempts: 1},
		{name: "302 not followed", status: 302, header: redirect, wantAttempts: 1},
		{name: "307 not followed", status: 307, header: redirect, wantAttempts: 1},
		{name: "308 not followed", status: 308, header: redirect, wantAttempts: 1},
		{name: "304 is final", status: 304, wantAttempts: 1},
		{name: "400 fails fast", status: 400, wantAttempts: 1},
		{name: "401 fails fast", status: 401, wantAttempts: 1},
		{name: "403 fails fast", status: 403, wantAttempts: 1},
		{name: "404 fails fast", status: 404, wantAttempts: 1},
		{name: "409 fails fast", status: 409, wantAttempts: 1},
		{name: "410 fails fast", status: 410, wantAttempts: 1},
		{name: "418 fails fast", status: 418, wantAttempts: 1},
		{name: "422 fails fast", status: 422, wantAttempts: 1},
		{name: "408 is retried", status: 408, wantAttempts: maxAttempts},
		{name: "425 is retried", status: 425, wantAttempts: maxAttempts},
		{name: "429 is retried", status: 429, wantAttempts: maxAttempts},
		{name: "500 is retried", status: 500, wantAttempts: maxAttempts},
		{name: "502 is retried", status: 502, wantAttempts: maxAttempts},
		{name: "503 is retried", status: 503, wantAttempts: maxAttempts},
		{name: "504 is retried", status: 504, wantAttempts: maxAttempts},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := &scripted{responses: []scriptedResponse{{status: tc.status, header: tc.header}}}
			var log letterLog
			s := newTestSender(t, transport.client(), &sleepRecorder{},
				WithMaxAttempts(maxAttempts), WithDeadLetter(log.record))

			res, err := s.Send(context.Background(), testURL, testEvent())
			if err != nil {
				t.Fatalf("Send error = %v, want nil", err)
			}
			if res.OK != tc.wantOK {
				t.Fatalf("OK = %v, want %v", res.OK, tc.wantOK)
			}
			if res.Attempts != tc.wantAttempts || transport.count() != tc.wantAttempts {
				t.Fatalf("attempts = %d (requests %d), want %d", res.Attempts, transport.count(), tc.wantAttempts)
			}
			if res.Status != tc.status {
				t.Fatalf("Status = %d, want %d", res.Status, tc.status)
			}
			wantLetters := 1
			if tc.wantOK {
				wantLetters = 0
			}
			if len(log.letters) != wantLetters {
				t.Fatalf("dead letters = %d, want %d", len(log.letters), wantLetters)
			}
		})
	}
}

func TestRedirectIsNotFollowed(t *testing.T) {
	transport := &scripted{responses: []scriptedResponse{
		{status: 302, header: makeHeader("Location", "https://elsewhere.test/in")},
		{status: 200},
	}}
	var log letterLog
	s := newTestSender(t, transport.client(), &sleepRecorder{}, WithDeadLetter(log.record))

	res, err := s.Send(context.Background(), testURL, testEvent())
	if err != nil {
		t.Fatalf("Send error = %v", err)
	}
	if res.OK || res.Status != 302 || res.Attempts != 1 {
		t.Fatalf("result = %+v, want a final 302 failure after one attempt", res)
	}
	reqs := transport.captured()
	if len(reqs) != 1 || reqs[0].url != testURL {
		t.Fatalf("requests = %+v, want exactly one request to %s", reqs, testURL)
	}
	if len(log.letters) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(log.letters))
	}
}

func TestRetriesReuseIdenticalBytes(t *testing.T) {
	transport := &scripted{responses: []scriptedResponse{
		{status: 500}, {status: 503}, {status: 429}, {status: 200},
	}}
	rec := &sleepRecorder{}
	s := newTestSender(t, transport.client(), rec, WithMaxAttempts(5))
	ev := Event{Type: "invoice.paid", Data: map[string]any{"amount": 4200, "items": []string{"a", "b"}}}

	res, err := s.Send(context.Background(), testURL, ev)
	if err != nil || !res.OK || res.Attempts != 4 {
		t.Fatalf("Send = (%+v, %v), want OK after 4 attempts", res, err)
	}
	reqs := transport.captured()
	if len(reqs) != 4 {
		t.Fatalf("requests = %d, want 4", len(reqs))
	}
	first := reqs[0]
	for i, r := range reqs {
		if !bytes.Equal(r.body, first.body) {
			t.Errorf("attempt %d body differs from attempt 1", i+1)
		}
		for _, name := range []string{"Webhook-Id", "Webhook-Timestamp", "Webhook-Signature", "Content-Type"} {
			if r.header.Get(name) != first.header.Get(name) {
				t.Errorf("attempt %d header %s = %q, want %q", i+1, name, r.header.Get(name), first.header.Get(name))
			}
		}
		if err := verifyStandardWebhook(testSecret, r.header, r.body); err != nil {
			t.Errorf("attempt %d does not verify: %v", i+1, err)
		}
	}
	if first.header.Get("Webhook-Id") == "" {
		t.Fatal("generated webhook-id is empty")
	}
	if len(rec.delays) != 3 {
		t.Fatalf("sleeps = %d, want 3", len(rec.delays))
	}
}

func TestRetryAfterIsHonored(t *testing.T) {
	tests := []struct {
		name     string
		header   http.Header
		maxDelay time.Duration
		want     time.Duration
	}{
		{name: "seconds", header: makeHeader("Retry-After", "7"), maxDelay: 5 * time.Minute, want: 7 * time.Second},
		{name: "zero seconds retries at once", header: makeHeader("Retry-After", "0"), maxDelay: 5 * time.Minute, want: 0},
		{name: "seconds capped to max", header: makeHeader("Retry-After", "3600"), maxDelay: 5 * time.Minute, want: 5 * time.Minute},
		{name: "http date", header: makeHeader("Retry-After", "Tue, 14 Nov 2023 22:13:50 GMT"), maxDelay: 5 * time.Minute, want: 30 * time.Second},
		{name: "http date capped to max", header: makeHeader("Retry-After", "Tue, 14 Nov 2023 23:13:20 GMT"), maxDelay: 5 * time.Minute, want: 5 * time.Minute},
		{name: "http date in the past", header: makeHeader("Retry-After", "Tue, 14 Nov 2023 22:00:00 GMT"), maxDelay: 5 * time.Minute, want: 0},
		{name: "invalid value falls back to backoff", header: makeHeader("Retry-After", "soon"), maxDelay: 5 * time.Minute, want: 500 * time.Millisecond},
		{name: "missing header falls back to backoff", maxDelay: 5 * time.Minute, want: 500 * time.Millisecond},
		{name: "seconds cap wins over base", header: makeHeader("Retry-After", "10"), maxDelay: 2 * time.Second, want: 2 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := &scripted{responses: []scriptedResponse{
				{status: 429, header: tc.header},
				{status: 200},
			}}
			rec := &sleepRecorder{}
			s := newTestSender(t, transport.client(), rec,
				WithMaxAttempts(3), WithBackoff(time.Second, tc.maxDelay))

			res, err := s.Send(context.Background(), testURL, testEvent())
			if err != nil || !res.OK || res.Attempts != 2 {
				t.Fatalf("Send = (%+v, %v), want OK after 2 attempts", res, err)
			}
			if len(rec.delays) != 1 || rec.delays[0] != tc.want {
				t.Fatalf("delays = %v, want [%v]", rec.delays, tc.want)
			}
		})
	}
}

func TestDeadLetterCalledOnce(t *testing.T) {
	tests := []struct {
		name         string
		responses    []scriptedResponse
		maxAttempts  int
		wantAttempts int
		wantStatus   int
		wantLastErr  bool
	}{
		{
			name:         "retries exhausted on 503",
			responses:    []scriptedResponse{{status: 503}},
			maxAttempts:  4,
			wantAttempts: 4,
			wantStatus:   503,
		},
		{
			name:         "permanent 404",
			responses:    []scriptedResponse{{status: 404}},
			maxAttempts:  4,
			wantAttempts: 1,
			wantStatus:   404,
		},
		{
			name:         "network errors exhausted",
			responses:    []scriptedResponse{{err: errors.New("connection reset by peer")}},
			maxAttempts:  3,
			wantAttempts: 3,
			wantStatus:   0,
			wantLastErr:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := &scripted{responses: tc.responses}
			var log letterLog
			s := newTestSender(t, transport.client(), &sleepRecorder{},
				WithMaxAttempts(tc.maxAttempts), WithDeadLetter(log.record))

			res, err := s.Send(context.Background(), testURL, testEvent())
			if err != nil {
				t.Fatalf("Send error = %v, want nil", err)
			}
			if res.OK || res.Attempts != tc.wantAttempts || res.Status != tc.wantStatus {
				t.Fatalf("result = %+v, want failure after %d attempts with status %d", res, tc.wantAttempts, tc.wantStatus)
			}
			if len(log.letters) != 1 {
				t.Fatalf("dead letter calls = %d, want exactly 1", len(log.letters))
			}
			l := log.letters[0]
			if l.URL != testURL || l.ID != "msg_1" || l.Attempts != tc.wantAttempts || l.LastStatus != tc.wantStatus {
				t.Fatalf("dead letter = %+v", l)
			}
			if l.Event.ID != "msg_1" || !l.Event.Timestamp.Equal(testNow) || l.Event.Type != "invoice.paid" {
				t.Fatalf("dead letter event not resolved: %+v", l.Event)
			}
			if (l.LastError != nil) != tc.wantLastErr {
				t.Fatalf("LastError = %v, want set=%v", l.LastError, tc.wantLastErr)
			}
		})
	}
}

func TestNoDeadLetterOnSuccess(t *testing.T) {
	var log letterLog
	s := newTestSender(t, sendOK().client(), &sleepRecorder{}, WithDeadLetter(log.record))
	if _, err := s.Send(context.Background(), testURL, testEvent()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(log.letters) != 0 {
		t.Fatalf("dead letter called %d times on success", len(log.letters))
	}
}

func TestNetworkErrorThenSuccess(t *testing.T) {
	transport := &scripted{responses: []scriptedResponse{
		{err: errors.New("connection refused")},
		{status: 200},
	}}
	res, err := newTestSender(t, transport.client(), &sleepRecorder{}).Send(context.Background(), testURL, testEvent())
	if err != nil || !res.OK || res.Attempts != 2 {
		t.Fatalf("Send = (%+v, %v), want OK after 2 attempts", res, err)
	}
}

func TestDeadLetterCallbackErrorIsReported(t *testing.T) {
	cbErr := errors.New("database is down")
	transport := &scripted{responses: []scriptedResponse{{status: 400}}}
	var calls int
	s := newTestSender(t, transport.client(), &sleepRecorder{}, WithDeadLetter(func(context.Context, DeadLetter) error {
		calls++
		return cbErr
	}))

	res, err := s.Send(context.Background(), testURL, testEvent())
	if !errors.Is(err, ErrDeadLetterCallback) {
		t.Fatalf("error = %v, want ErrDeadLetterCallback", err)
	}
	if !errors.Is(err, cbErr) {
		t.Fatalf("error does not wrap the callback error: %v", err)
	}
	var oerr *Error
	if !errors.As(err, &oerr) || oerr.Code != CodeDeadLetterCallback {
		t.Fatalf("error code = %v, want %s", err, CodeDeadLetterCallback)
	}
	if res.OK || res.Attempts != 1 || res.Status != 400 {
		t.Fatalf("result = %+v, want failure after 1 attempt", res)
	}
	if calls != 1 {
		t.Fatalf("callback calls = %d, want 1", calls)
	}
}

func TestSendContextCanceled(t *testing.T) {
	t.Run("already canceled", func(t *testing.T) {
		transport := sendOK()
		var log letterLog
		s := newTestSender(t, transport.client(), &sleepRecorder{}, WithDeadLetter(log.record))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		res, err := s.Send(ctx, testURL, testEvent())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if res.Attempts != 0 || transport.count() != 0 {
			t.Fatalf("a canceled Send made %d requests", transport.count())
		}
		if len(log.letters) != 0 {
			t.Fatal("dead letter called on cancellation")
		}
	})

	t.Run("canceled during attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var log letterLog
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			_ = r.Body.Close()
			cancel()
			return newResponse(http.StatusInternalServerError, nil), nil
		})}
		s := newTestSender(t, client, &sleepRecorder{}, WithDeadLetter(log.record))

		_, err := s.Send(ctx, testURL, testEvent())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if len(log.letters) != 0 {
			t.Fatal("dead letter called on cancellation")
		}
	})

	t.Run("canceled during backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			_ = r.Body.Close()
			return newResponse(http.StatusServiceUnavailable, nil), nil
		})}
		var log letterLog
		s := newTestSender(t, client, &sleepRecorder{},
			WithBackoff(time.Hour, time.Hour), WithSleep(sleepContext), WithDeadLetter(log.record))
		time.AfterFunc(20*time.Millisecond, cancel)

		start := time.Now()
		res, err := s.Send(ctx, testURL, testEvent())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if res.Attempts != 1 {
			t.Fatalf("attempts = %d, want 1", res.Attempts)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("Send waited %v after cancellation", elapsed)
		}
		if len(log.letters) != 0 {
			t.Fatal("dead letter called on cancellation")
		}
	})
}

func TestResponseBodyIsDiscardedUnread(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusInternalServerError} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			body := &trackedBody{}
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				_ = r.Body.Close()
				return &http.Response{StatusCode: status, Header: http.Header{}, Body: body}, nil
			})}
			s := newTestSender(t, client, &sleepRecorder{}, WithMaxAttempts(1))

			if _, err := s.Send(context.Background(), testURL, testEvent()); err != nil {
				t.Fatalf("Send: %v", err)
			}
			if body.reads != 0 {
				t.Fatalf("response body was read %d times, want 0", body.reads)
			}
			if body.closes != 1 {
				t.Fatalf("response body closed %d times, want 1", body.closes)
			}
		})
	}
}

// countingData counts how often it is serialized.
type countingData struct {
	calls *int
}

func (d countingData) MarshalJSON() ([]byte, error) {
	*d.calls++
	return []byte(`{"n":1}`), nil
}

func TestDataSerializedOnce(t *testing.T) {
	calls := 0
	transport := &scripted{responses: []scriptedResponse{{status: 500}, {status: 500}, {status: 200}}}
	s := newTestSender(t, transport.client(), &sleepRecorder{}, WithMaxAttempts(3))

	res, err := s.Send(context.Background(), testURL, Event{Type: "x", Data: countingData{calls: &calls}})
	if err != nil || !res.OK || res.Attempts != 3 {
		t.Fatalf("Send = (%+v, %v), want OK after 3 attempts", res, err)
	}
	if calls != 1 {
		t.Fatalf("data serialized %d times, want 1", calls)
	}
}

func TestEventIDAndTimestampResolution(t *testing.T) {
	uuidV4 := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	tests := []struct {
		name    string
		id      string
		ts      time.Time
		wantID  string // empty means a generated UUIDv4
		wantSec int64
	}{
		{name: "explicit id and timestamp", id: "evt_42", ts: testNow.Add(123 * time.Second), wantID: "evt_42", wantSec: 1700000123},
		{name: "sub-second part is truncated", id: "evt_43", ts: testNow.Add(123*time.Second + 900*time.Millisecond), wantID: "evt_43", wantSec: 1700000123},
		{name: "zero timestamp uses clock", id: "evt_44", wantID: "evt_44", wantSec: 1700000000},
		{name: "generated id", wantSec: 1700000000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := sendOK()
			s := newTestSender(t, transport.client(), &sleepRecorder{})
			ev := Event{Type: "x", ID: tc.id, Timestamp: tc.ts}

			if _, err := s.Send(context.Background(), testURL, ev); err != nil {
				t.Fatalf("Send: %v", err)
			}
			req := transport.captured()[0]
			if got := req.header.Get("Webhook-Timestamp"); got != strconv.FormatInt(tc.wantSec, 10) {
				t.Fatalf("webhook-timestamp = %q, want %d", got, tc.wantSec)
			}
			id := req.header.Get("Webhook-Id")
			if tc.wantID == "" {
				if !uuidV4.MatchString(id) {
					t.Fatalf("generated webhook-id %q is not a UUIDv4", id)
				}
			} else if id != tc.wantID {
				t.Fatalf("webhook-id = %q, want %q", id, tc.wantID)
			}

			var env struct {
				Timestamp string `json:"timestamp"`
			}
			if err := json.Unmarshal(req.body, &env); err != nil {
				t.Fatalf("body is not JSON: %v", err)
			}
			want := time.Unix(tc.wantSec, 0).UTC().Format(time.RFC3339)
			if env.Timestamp != want {
				t.Fatalf("body timestamp = %q, want %q", env.Timestamp, want)
			}
		})
	}
}

func TestGeneratedIDsAreUniquePerSend(t *testing.T) {
	transport := sendOK()
	s := newTestSender(t, transport.client(), &sleepRecorder{})
	for range 2 {
		if _, err := s.Send(context.Background(), testURL, Event{Type: "x"}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	reqs := transport.captured()
	if reqs[0].header.Get("Webhook-Id") == reqs[1].header.Get("Webhook-Id") {
		t.Fatal("two sends without an ID share the same webhook-id")
	}
}

func TestSenderIsSafeForConcurrentUse(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		_ = r.Body.Close()
		return newResponse(http.StatusOK, nil), nil
	})}
	s := newTestSender(t, client, &sleepRecorder{})

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.Send(context.Background(), testURL, Event{Type: "x"})
			if err != nil || !res.OK {
				t.Errorf("concurrent Send = (%+v, %v), want OK", res, err)
			}
		}()
	}
	wg.Wait()
}

func TestSecretNeverAppearsInErrors(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte(testKey))
	leak := func(s string) bool {
		return strings.Contains(s, testSecret) || strings.Contains(s, encoded) || strings.Contains(s, testKey)
	}

	t.Run("invalid URL", func(t *testing.T) {
		s := newTestSender(t, sendOK().client(), &sleepRecorder{})
		_, err := s.Send(context.Background(), "http://hooks.example.test", testEvent())
		if err == nil || leak(err.Error()) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("malformed secret", func(t *testing.T) {
		_, err := NewSender("whsec_" + encoded + "!")
		if err == nil || leak(err.Error()) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("dead-letter callback error", func(t *testing.T) {
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			_ = r.Body.Close()
			return nil, errors.New("transport failed")
		})}
		s := newTestSender(t, client, &sleepRecorder{}, WithMaxAttempts(1),
			WithDeadLetter(func(_ context.Context, letter DeadLetter) error {
				if leak(fmt.Sprintf("%+v", letter)) {
					t.Error("dead letter contains key material")
				}
				return fmt.Errorf("store failed for %s", letter.ID)
			}))
		_, err := s.Send(context.Background(), testURL, testEvent())
		if err == nil || leak(err.Error()) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("network error", func(t *testing.T) {
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			_ = r.Body.Close()
			return nil, errors.New("connection refused")
		})}
		var log letterLog
		s := newTestSender(t, client, &sleepRecorder{}, WithMaxAttempts(1), WithDeadLetter(log.record))
		if _, err := s.Send(context.Background(), testURL, testEvent()); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if len(log.letters) != 1 || leak(fmt.Sprintf("%v", log.letters[0].LastError)) {
			t.Fatalf("dead letter error leaks key material")
		}
	})
}

func TestEventDataIsSentAsJSON(t *testing.T) {
	transport := sendOK()
	s := newTestSender(t, transport.client(), &sleepRecorder{})
	ev := Event{Type: "order.created", Data: map[string]any{"id": "o_1", "total": 12.5}, ID: "msg_9", Timestamp: testNow}
	if _, err := s.Send(context.Background(), testURL, ev); err != nil {
		t.Fatalf("Send: %v", err)
	}
	var got struct {
		Type      string         `json:"type"`
		Timestamp string         `json:"timestamp"`
		Data      map[string]any `json:"data"`
	}
	if err := json.Unmarshal(transport.captured()[0].body, &got); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if got.Type != "order.created" || got.Timestamp != "2023-11-14T22:13:20Z" || got.Data["id"] != "o_1" {
		t.Fatalf("body = %+v", got)
	}
	if got.Data["total"] != 12.5 {
		t.Fatalf("data.total = %v, want 12.5", got.Data["total"])
	}
}
