package outbox

import (
	"context"
	"math/rand/v2"
	"net/http"
	"time"
)

const (
	defaultTimeout     = 10 * time.Second
	defaultMaxAttempts = 5
	defaultBaseBackoff = time.Second
	defaultMaxBackoff  = 5 * time.Minute
)

// SleepFunc waits for d between two attempts. It returns early with an error if ctx is done.
type SleepFunc func(ctx context.Context, d time.Duration) error

// DeadLetterFunc is called once when delivery has failed for good. If it returns an error,
// Send returns it wrapped in an *Error with CodeDeadLetterCallback.
type DeadLetterFunc func(ctx context.Context, letter DeadLetter) error

// Option configures a Sender. NewSender returns an *Error with CodeInvalidOptions for invalid
// values.
type Option func(*config) error

type config struct {
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

func defaultConfig() config {
	return config{
		client:      &http.Client{Timeout: defaultTimeout},
		maxAttempts: defaultMaxAttempts,
		baseBackoff: defaultBaseBackoff,
		maxBackoff:  defaultMaxBackoff,
		// Jitter does not need cryptographic randomness.
		random: rand.Float64, //nolint:gosec // G404: jitter is not security-sensitive
		clock:  time.Now,
		sleep:  sleepContext,
	}
}

// WithHTTPClient sets the HTTP client used for deliveries. Its Timeout bounds each attempt, so a
// zero Timeout means no per-attempt limit. A copy of the client is used; CheckRedirect is
// replaced so that redirects are never followed. The default client has a 10 second timeout.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) error {
		if client == nil {
			return invalidOptions("WithHTTPClient: client must not be nil")
		}
		c.client = client
		return nil
	}
}

// WithMaxAttempts sets the total number of attempts, including the first one. The default is 5.
func WithMaxAttempts(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return invalidOptions("WithMaxAttempts: must be at least 1")
		}
		c.maxAttempts = n
		return nil
	}
}

// WithBackoff sets the base delay of the exponential backoff and the cap for any single delay,
// including delays taken from Retry-After. The defaults are 1 second and 5 minutes.
func WithBackoff(base, maxDelay time.Duration) Option {
	return func(c *config) error {
		if base < 0 || maxDelay < 0 {
			return invalidOptions("WithBackoff: base and max must not be negative")
		}
		c.baseBackoff = base
		c.maxBackoff = maxDelay
		return nil
	}
}

// WithRandom sets the source for jitter. It must return values in [0, 1). The default is
// math/rand/v2.Float64.
func WithRandom(random func() float64) Option {
	return func(c *config) error {
		if random == nil {
			return invalidOptions("WithRandom: function must not be nil")
		}
		c.random = random
		return nil
	}
}

// WithClock sets the clock used for default timestamps and for HTTP-date Retry-After values.
// The default is time.Now.
func WithClock(clock func() time.Time) Option {
	return func(c *config) error {
		if clock == nil {
			return invalidOptions("WithClock: clock must not be nil")
		}
		c.clock = clock
		return nil
	}
}

// WithSleep sets the function that waits between attempts. The default waits with a timer and
// returns early when the context is done.
func WithSleep(sleep SleepFunc) Option {
	return func(c *config) error {
		if sleep == nil {
			return invalidOptions("WithSleep: function must not be nil")
		}
		c.sleep = sleep
		return nil
	}
}

// WithDeadLetter sets the callback that receives an event whose delivery failed for good. The
// caller is responsible for persisting it; if the callback does not persist it, it is lost.
func WithDeadLetter(onDeadLetter DeadLetterFunc) Option {
	return func(c *config) error {
		if onDeadLetter == nil {
			return invalidOptions("WithDeadLetter: callback must not be nil")
		}
		c.onDeadLetter = onDeadLetter
		return nil
	}
}

// WithAllowInsecureHTTP allows http:// destinations. Use it for local development only.
// Without it, only https:// destinations are accepted.
func WithAllowInsecureHTTP() Option {
	return func(c *config) error {
		c.allowInsecureHTTP = true
		return nil
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
