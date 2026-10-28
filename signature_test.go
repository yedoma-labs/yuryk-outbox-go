package outbox

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"strconv"
	"testing"
)

// referenceBody is the exact payload of the reference vector.
const referenceBody = `{"type":"invoice.paid","timestamp":"2023-11-14T22:13:20Z","data":{"amount":4200}}`

// referenceSignature was computed with OpenSSL (HMAC-SHA256 over "msg_1.1700000000.<body>" with
// the key in testKey), independently of this package.
const referenceSignature = "v1,TtH3IaZmyXR7wMxyASMEwN4a8w0Hun01roS/wXTyeZs="

func TestSignReferenceVector(t *testing.T) {
	tests := []struct {
		name      string
		id        string
		timestamp int64
		body      string
		want      string
	}{
		{
			name:      "fixed vector",
			id:        "msg_1",
			timestamp: 1700000000,
			body:      referenceBody,
			want:      referenceSignature,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Expected value computed here with crypto/hmac, on top of the fixed vector.
			mac := hmac.New(sha256.New, []byte(testKey))
			_, _ = mac.Write([]byte(tc.id + "." + strconv.FormatInt(tc.timestamp, 10) + "." + tc.body))
			computed := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
			if computed != tc.want {
				t.Fatalf("test helper computed %q, want fixed vector %q", computed, tc.want)
			}

			got := sign([]byte(testKey), tc.id, tc.timestamp, []byte(tc.body))
			if got != tc.want {
				t.Fatalf("sign() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSendSignatureMatchesReferenceVector(t *testing.T) {
	transport := &scripted{responses: []scriptedResponse{{status: http.StatusOK}}}
	rec := &sleepRecorder{}
	s := newTestSender(t, transport.client(), rec)

	res, err := s.Send(context.Background(), testURL, testEvent())
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !res.OK {
		t.Fatalf("Send result = %+v, want OK", res)
	}

	reqs := transport.captured()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d, want 1", len(reqs))
	}
	req := reqs[0]
	if string(req.body) != referenceBody {
		t.Errorf("body = %s\nwant %s", req.body, referenceBody)
	}
	checks := map[string]string{
		"Webhook-Id":        "msg_1",
		"Webhook-Timestamp": "1700000000",
		"Webhook-Signature": referenceSignature,
		"Content-Type":      "application/json",
	}
	for name, want := range checks {
		if got := req.header.Get(name); got != want {
			t.Errorf("header %s = %q, want %q", name, got, want)
		}
	}
	if err := verifyStandardWebhook(testSecret, req.header, req.body); err != nil {
		t.Fatalf("independent verifier rejected the request: %v", err)
	}
}

func TestVerifierRejectsTampering(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(h http.Header, body *[]byte)
	}{
		{name: "body changed", mutate: func(_ http.Header, body *[]byte) { *body = []byte(`{"type":"x","timestamp":"","data":{}}`) }},
		{name: "id changed", mutate: func(h http.Header, _ *[]byte) { h.Set("Webhook-Id", "msg_2") }},
		{name: "timestamp changed", mutate: func(h http.Header, _ *[]byte) { h.Set("Webhook-Timestamp", "1700000001") }},
		{name: "signature missing", mutate: func(h http.Header, _ *[]byte) { h.Del("Webhook-Signature") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := makeHeader("Webhook-Id", "msg_1", "Webhook-Timestamp", "1700000000", "Webhook-Signature", referenceSignature)
			body := []byte(referenceBody)
			if err := verifyStandardWebhook(testSecret, h, body); err != nil {
				t.Fatalf("baseline must verify: %v", err)
			}
			tc.mutate(h, &body)
			if err := verifyStandardWebhook(testSecret, h, body); err == nil {
				t.Fatal("verifier accepted a tampered request")
			}
		})
	}
}
