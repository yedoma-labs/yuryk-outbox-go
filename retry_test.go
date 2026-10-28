package outbox

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIsRetryableStatus(t *testing.T) {
	tests := []struct {
		status int
		want   bool
	}{
		{200, false}, {201, false}, {204, false},
		{301, false}, {302, false}, {304, false}, {307, false}, {308, false},
		{400, false}, {401, false}, {403, false}, {404, false}, {409, false}, {410, false}, {418, false}, {422, false},
		{408, true}, {425, true}, {429, true},
		{500, true}, {501, true}, {502, true}, {503, true}, {504, true}, {599, true},
		{600, false}, {100, false},
	}
	for _, tc := range tests {
		if got := isRetryableStatus(tc.status); got != tc.want {
			t.Errorf("isRetryableStatus(%d) = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestIsSuccess(t *testing.T) {
	tests := []struct {
		status int
		want   bool
	}{
		{199, false}, {200, true}, {202, true}, {204, true}, {299, true}, {300, false}, {500, false},
	}
	for _, tc := range tests {
		if got := isSuccess(tc.status); got != tc.want {
			t.Errorf("isSuccess(%d) = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2015, time.October, 21, 7, 27, 0, 0, time.UTC)
	tests := []struct {
		name      string
		value     string
		want      time.Duration
		wantFound bool
	}{
		{name: "empty", value: "", want: 0, wantFound: false},
		{name: "blank", value: "   ", want: 0, wantFound: false},
		{name: "zero seconds", value: "0", want: 0, wantFound: true},
		{name: "seconds", value: "120", want: 120 * time.Second, wantFound: true},
		{name: "seconds with spaces", value: " 5 ", want: 5 * time.Second, wantFound: true},
		{name: "negative seconds rejected", value: "-1", want: 0, wantFound: false},
		{name: "fraction rejected", value: "1.5", want: 0, wantFound: false},
		{name: "garbage rejected", value: "soon", want: 0, wantFound: false},
		{name: "huge seconds saturate", value: "99999999999999999999999", want: 1<<63 - 1, wantFound: true},
		{name: "http date in future", value: "Wed, 21 Oct 2015 07:28:00 GMT", want: time.Minute, wantFound: true},
		{name: "http date in past", value: "Wed, 21 Oct 2015 07:26:00 GMT", want: 0, wantFound: true},
		{name: "http date same instant", value: "Wed, 21 Oct 2015 07:27:00 GMT", want: 0, wantFound: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, found := parseRetryAfter(tc.value, now)
			if found != tc.wantFound || got != tc.want {
				t.Fatalf("parseRetryAfter(%q) = (%v, %v), want (%v, %v)", tc.value, got, found, tc.want, tc.wantFound)
			}
		})
	}
}

func TestBackoffDelayJitter(t *testing.T) {
	const second = time.Second
	tests := []struct {
		name    string
		attempt int
		base    time.Duration
		maxD    time.Duration
		random  float64
		want    time.Duration
	}{
		{name: "first retry, random 0", attempt: 1, base: second, maxD: 5 * second, random: 0, want: 0},
		{name: "first retry, random 0.5", attempt: 1, base: second, maxD: 5 * second, random: 0.5, want: 500 * time.Millisecond},
		{name: "second retry doubles", attempt: 2, base: second, maxD: 5 * second, random: 0.5, want: second},
		{name: "third retry", attempt: 3, base: second, maxD: 5 * second, random: 0.5, want: 2 * second},
		{name: "fourth retry capped by max", attempt: 4, base: second, maxD: 5 * second, random: 0.5, want: 2500 * time.Millisecond},
		{name: "late retry stays at cap", attempt: 10, base: second, maxD: 5 * second, random: 0.5, want: 2500 * time.Millisecond},
		{name: "huge attempt does not overflow", attempt: 200, base: second, maxD: 5 * time.Minute, random: 0.5, want: 150 * time.Second},
		{name: "zero base", attempt: 3, base: 0, maxD: 5 * second, random: 0.9, want: 0},
		{name: "base above max is capped", attempt: 1, base: 10 * second, maxD: 5 * second, random: 1, want: 5 * second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := backoffDelay(tc.attempt, tc.base, tc.maxD, func() float64 { return tc.random })
			if got != tc.want {
				t.Fatalf("backoffDelay = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBackoffDelayStaysWithinBounds(t *testing.T) {
	const base, maxD = time.Second, 5 * time.Second
	for attempt := 1; attempt <= 8; attempt++ {
		ceiling := min(base*time.Duration(1<<(attempt-1)), maxD)
		for _, r := range []float64{0, 0.25, 0.5, 0.75, 0.999999} {
			got := backoffDelay(attempt, base, maxD, func() float64 { return r })
			if got < 0 || got > ceiling {
				t.Errorf("attempt %d random %v: delay %v outside [0, %v]", attempt, r, got, ceiling)
			}
		}
	}
}

func TestSleepContextHonorsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := sleepContext(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleepContext error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("sleepContext did not return promptly: %v", elapsed)
	}
	if err := sleepContext(context.Background(), 0); err != nil {
		t.Fatalf("sleepContext(0) on a live context = %v, want nil", err)
	}
}
