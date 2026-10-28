package outbox

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestEndToEndOverTLS sends over a real loopback TLS connection. The receiver fails twice, then
// accepts. Every request it received must verify with the independent verifier.
func TestEndToEndOverTLS(t *testing.T) {
	var mu sync.Mutex
	var received []http.Header
	var bodies [][]byte

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		received = append(received, r.Header.Clone())
		bodies = append(bodies, body)
		if len(received) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	client := *srv.Client()
	s := newTestSender(t, &client, &sleepRecorder{}, WithMaxAttempts(5))
	res, err := s.Send(context.Background(), srv.URL, testEvent())
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !res.OK || res.Attempts != 3 || res.Status != http.StatusAccepted {
		t.Fatalf("result = %+v, want OK on attempt 3", res)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("receiver saw %d requests, want 3", len(received))
	}
	for i := range received {
		if err := verifyStandardWebhook(testSecret, received[i], bodies[i]); err != nil {
			t.Errorf("request %d does not verify: %v", i+1, err)
		}
		if string(bodies[i]) != string(bodies[0]) {
			t.Errorf("request %d body differs from request 1", i+1)
		}
	}
}

// TestTimeoutIsRetried uses a receiver that never answers in time. Each attempt must time out,
// the sender must retry, and the dead letter must carry the timeout error.
func TestTimeoutIsRetried(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer srv.Close()
	defer close(release)

	client := *srv.Client()
	client.Timeout = 50 * time.Millisecond
	var log letterLog
	s := newTestSender(t, &client, &sleepRecorder{}, WithMaxAttempts(2), WithDeadLetter(log.record))

	res, err := s.Send(context.Background(), srv.URL, testEvent())
	if err != nil {
		t.Fatalf("Send error = %v, want nil", err)
	}
	if res.OK || res.Attempts != 2 || res.Status != 0 {
		t.Fatalf("result = %+v, want failure after 2 attempts without response", res)
	}
	if len(log.letters) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(log.letters))
	}
	var timeoutErr interface{ Timeout() bool }
	if !errors.As(log.letters[0].LastError, &timeoutErr) || !timeoutErr.Timeout() {
		t.Fatalf("LastError = %v, want a timeout", log.letters[0].LastError)
	}
}
