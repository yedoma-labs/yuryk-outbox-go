package outbox

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNewSenderSecret(t *testing.T) {
	valid16 := "whsec_" + base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	valid32 := "whsec_" + base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))

	tests := []struct {
		name    string
		secret  string
		wantErr bool
	}{
		{name: "valid 16 byte key", secret: valid16},
		{name: "valid 32 byte key", secret: valid32},
		{name: "empty", secret: "", wantErr: true},
		{name: "prefix only", secret: "whsec_", wantErr: true},
		{name: "no prefix", secret: "MDEyMzQ1Njc4OWFiY2RlZg==", wantErr: true},
		{name: "wrong prefix case", secret: "WHSEC_MDEyMzQ1Njc4OWFiY2RlZg==", wantErr: true},
		{name: "not base64", secret: "whsec_!!!notbase64!!!", wantErr: true},
		{name: "missing padding", secret: "whsec_MDEyMzQ1Njc4OWFiY2RlZg", wantErr: true},
		{name: "non-canonical trailing bits", secret: "whsec_AB==", wantErr: true},
		{name: "embedded newline", secret: "whsec_AAE=\n", wantErr: true},
		{name: "embedded space", secret: "whsec_AA E=", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSender(tc.secret)
			if tc.wantErr {
				if !errors.Is(err, ErrMalformedSecret) {
					t.Fatalf("NewSender error = %v, want ErrMalformedSecret", err)
				}
				// The message names the expected format, so only the payload after the prefix counts.
				payload := strings.TrimPrefix(tc.secret, "whsec_")
				if payload != "" && strings.Contains(err.Error(), payload) {
					t.Fatalf("error message contains the secret payload: %q", err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("NewSender: unexpected error %v", err)
			}
		})
	}
}

func TestNewSenderOptionValidation(t *testing.T) {
	tests := []struct {
		name    string
		opt     Option
		wantErr bool
	}{
		{name: "max attempts zero", opt: WithMaxAttempts(0), wantErr: true},
		{name: "max attempts negative", opt: WithMaxAttempts(-1), wantErr: true},
		{name: "max attempts one", opt: WithMaxAttempts(1)},
		{name: "negative base", opt: WithBackoff(-time.Second, time.Minute), wantErr: true},
		{name: "negative max", opt: WithBackoff(time.Second, -time.Minute), wantErr: true},
		{name: "zero backoff", opt: WithBackoff(0, 0)},
		{name: "nil http client", opt: WithHTTPClient(nil), wantErr: true},
		{name: "nil random", opt: WithRandom(nil), wantErr: true},
		{name: "nil clock", opt: WithClock(nil), wantErr: true},
		{name: "nil sleep", opt: WithSleep(nil), wantErr: true},
		{name: "nil dead letter", opt: WithDeadLetter(nil), wantErr: true},
		{name: "nil option", opt: nil, wantErr: true},
		{name: "insecure opt-in", opt: WithAllowInsecureHTTP()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSender(testSecret, tc.opt)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidOptions) {
					t.Fatalf("NewSender error = %v, want ErrInvalidOptions", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewSender: unexpected error %v", err)
			}
		})
	}
}

func TestNewSenderDefaults(t *testing.T) {
	s, err := NewSender(testSecret)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	if s.client.Timeout != 10*time.Second {
		t.Errorf("default timeout = %v, want 10s", s.client.Timeout)
	}
	if s.maxAttempts != 5 {
		t.Errorf("default maxAttempts = %d, want 5", s.maxAttempts)
	}
	if s.baseBackoff != time.Second {
		t.Errorf("default base backoff = %v, want 1s", s.baseBackoff)
	}
	if s.maxBackoff != 5*time.Minute {
		t.Errorf("default max backoff = %v, want 5m", s.maxBackoff)
	}
	if s.allowInsecureHTTP {
		t.Error("insecure HTTP must be off by default")
	}
	if s.client.CheckRedirect == nil {
		t.Fatal("CheckRedirect must be set so that redirects are never followed")
	}
	if err := s.client.CheckRedirect(nil, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Errorf("CheckRedirect = %v, want http.ErrUseLastResponse", err)
	}
}

func TestNewSenderKeepsCallerClient(t *testing.T) {
	caller := &http.Client{Timeout: 3 * time.Second}
	s, err := NewSender(testSecret, WithHTTPClient(caller))
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	if caller.CheckRedirect != nil {
		t.Error("the caller's client was modified")
	}
	if s.client == caller {
		t.Error("the sender must use a copy of the caller's client")
	}
	if s.client.Timeout != 3*time.Second {
		t.Errorf("copied timeout = %v, want 3s", s.client.Timeout)
	}
}

func TestSenderOptionsApplied(t *testing.T) {
	s, err := NewSender(testSecret,
		WithMaxAttempts(7),
		WithBackoff(2*time.Second, time.Minute),
		WithAllowInsecureHTTP(),
	)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	if s.maxAttempts != 7 || s.baseBackoff != 2*time.Second || s.maxBackoff != time.Minute {
		t.Errorf("options not applied: attempts=%d base=%v max=%v", s.maxAttempts, s.baseBackoff, s.maxBackoff)
	}
	if !s.allowInsecureHTTP {
		t.Error("WithAllowInsecureHTTP not applied")
	}
}
