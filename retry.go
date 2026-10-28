package outbox

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxRetryAfterSeconds is the largest number of seconds that fits into a time.Duration.
const maxRetryAfterSeconds = math.MaxInt64 / int64(time.Second)

// isRetryableStatus reports whether a response status is retried: 408, 425, 429 and all 5xx.
func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	}
	return status >= 500 && status <= 599
}

// isSuccess reports whether a response status is 2xx.
func isSuccess(status int) bool {
	return status >= 200 && status <= 299
}

// parseRetryAfter parses a Retry-After header value, either delay-seconds or an HTTP-date. It
// returns the delay and true, or false if the value is empty or invalid. Values too large for a
// time.Duration are returned as the largest possible duration; the caller caps them anyway.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if isDigits(value) {
		secs, err := strconv.ParseInt(value, 10, 64)
		if err != nil || secs > maxRetryAfterSeconds {
			return math.MaxInt64, true
		}
		return time.Duration(secs) * time.Second, true
	}
	at, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	return max(at.Sub(now), 0), true
}

// backoffDelay returns the delay before the retry that follows the given failed attempt (1-based).
// It uses full jitter: random() * min(maxDelay, baseDelay * 2^(attempt-1)).
func backoffDelay(attempt int, baseDelay, maxDelay time.Duration, random func() float64) time.Duration {
	ceiling := baseDelay
	for i := 1; i < attempt && ceiling < maxDelay; i++ {
		// Doubling would overflow or exceed the cap: stop at the cap.
		if ceiling > maxDelay/2 {
			ceiling = maxDelay
			break
		}
		ceiling *= 2
	}
	ceiling = min(ceiling, maxDelay)
	delay := time.Duration(random() * float64(ceiling))
	return min(max(delay, 0), ceiling)
}

func isDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}
