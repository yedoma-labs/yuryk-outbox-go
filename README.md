# yuryk-outbox-go
<picture>
  <source media="(max-width: 640px)" srcset="https://raw.githubusercontent.com/yedoma-labs/assets/main/resized/banner-resized-mobile.png">
  <img src="https://raw.githubusercontent.com/yedoma-labs/assets/main/resized/banner-resized.png" alt="Project Header">
</picture>

A small Go library for sending outbound webhooks with correct retry semantics. It has stable event
IDs across retries, `Retry-After` support, per-attempt timeouts, and
[Standard Webhooks](https://www.standardwebhooks.com/) signatures. It depends only on the
standard library and requires Go 1.23 or newer.

It is the counterpart to [`tuyar-webhook-go`](https://github.com/yedoma-labs/tuyar-webhook-go),
which verifies incoming webhooks. The TypeScript version is
`@yedoma-labs/yuryk-outbox`.

## The problem

Hand-rolled webhook senders tend to get the same things wrong:

- **Duplicates on retry.** A new event ID per attempt makes receivers treat each retry as a new event.
- **Signatures recomputed per attempt.** The timestamp changes, so the signature changes, and the body may be serialized differently each time.
- **`Retry-After` ignored.** Receivers that ask for a pause get hammered anyway.
- **No timeout.** A hanging receiver blocks the sender indefinitely.
- **Retries on permanent errors.** A `400` or `404` will not succeed on the fifth try.
- **Response bodies read for nothing.** The sender waits for a body whose content it never uses.

`yuryk-outbox-go` handles these. It is a sender only: it does not store events, run queues, or
provide a dashboard.

## Quickstart

```go
package main

import (
	"context"
	"log"
	"os"

	outbox "github.com/yedoma-labs/yuryk-outbox-go"
)

func main() {
	sender, err := outbox.NewSender(os.Getenv("WEBHOOK_SECRET"), // whsec_<base64>
		outbox.WithDeadLetter(func(ctx context.Context, l outbox.DeadLetter) error {
			// Persist the failed delivery. If this returns an error, Send reports it.
			return saveFailedWebhook(ctx, l.URL, l.ID, l.Attempts, l.LastStatus)
		}),
	)
	if err != nil {
		log.Fatal(err) // ErrMalformedSecret or ErrInvalidOptions
	}

	res, err := sender.Send(context.Background(), "https://customer.example.com/hooks", outbox.Event{
		Type: "invoice.paid",
		ID:   "evt_01J9Z3",
		Data: map[string]any{"invoiceId": "inv_123", "amount": 4200},
	})
	if err != nil {
		log.Fatal(err) // configuration, input, or dead-letter callback error
	}
	if !res.OK {
		// Already dead-lettered. res.Attempts and res.Status describe the failure.
	}
}
```

The package name is `outbox`. The import above uses an explicit alias because the module path
ends in `-go`.

The receiver gets:

```
POST /hooks
Content-Type: application/json
Webhook-Id: evt_01J9Z3
Webhook-Timestamp: 1800000000
Webhook-Signature: v1,<base64 HMAC-SHA256>

{"type":"invoice.paid","timestamp":"2027-01-15T08:00:00Z","data":{"invoiceId":"inv_123","amount":4200}}
```

If `Event.ID` is empty, a random UUIDv4 is generated once per `Send` call and reused on every retry.

## Retry behavior

| Response or failure                                          | Action                       |
| ------------------------------------------------------------ | ---------------------------- |
| `2xx`                                                        | Success, stop.               |
| Network error, timeout                                       | Retry.                       |
| `408`, `425`, `429`, `5xx`                                   | Retry.                       |
| Other `4xx`                                                  | Fail immediately, no retry.  |
| `1xx`, `3xx` (redirects are not followed)                    | Fail immediately, no retry.  |

Delays between attempts:

- If the response has `Retry-After` (seconds or HTTP-date), that delay is used, capped to the maximum backoff.
- Otherwise full jitter: `random * min(max, base * 2^(attempt-1))`.
- No sleep after the final attempt.

After the last failed attempt, the dead-letter callback is called once, and `Send` returns
`Result{OK: false}`. This also applies to non-retryable failures. A canceled context ends `Send`
with `ctx.Err()` and does not call the callback.

## Options

| Option                       | Default               | Description                                                              |
| ---------------------------- | --------------------- | ------------------------------------------------------------------------ |
| `WithHTTPClient(c)`          | client with 10s timeout | Client for deliveries. Its `Timeout` bounds each attempt. Redirects are never followed. |
| `WithMaxAttempts(n)`         | `5`                   | Total attempts, including the first.                                     |
| `WithBackoff(base, max)`     | `1s`, `5m`            | Base delay for exponential backoff, and the cap for any single delay, including `Retry-After`. |
| `WithRandom(fn)`             | `math/rand/v2`        | Source for jitter, returns values in `[0, 1)`.                           |
| `WithClock(fn)`              | `time.Now`            | Clock for default timestamps and HTTP-date `Retry-After` values.         |
| `WithSleep(fn)`              | timer, context-aware  | Waiting function between attempts.                                       |
| `WithDeadLetter(fn)`         | none                  | Called once when delivery has failed for good.                           |
| `WithAllowInsecureHTTP()`    | off                   | Allow `http://` destinations. For local development only.                |

## Signing and timestamps

- The signature is HMAC-SHA256 over `<webhook-id>.<webhook-timestamp>.<body>`, keyed with the base64-decoded secret.
- The body is serialized once. The ID, timestamp, body, and signature are fixed before the first attempt and sent unchanged on every retry.
- `Webhook-Timestamp` is the event time in whole seconds. It does not move with retries.

**Receiver tolerance.** Standard Webhooks receivers commonly accept timestamps within 5 minutes.
With the defaults, the worst case without `Retry-After` is about 65 seconds: five attempts of 10
seconds each, plus at most 1, 2, 4, and 8 seconds of backoff. A large `Retry-After` can extend the
window, but each delay is capped by `WithBackoff`. If your receivers enforce a tight tolerance,
lower the maximum backoff or the attempt count.

## Idempotency on the receiver

Delivery is **at-least-once**. A receiver can get the same event again if its response was lost,
or if the sender timed out after the receiver had already processed the request. Deduplicate by
the `Webhook-Id` header and treat a repeated ID as a no-op.

## Dead letters

`DeadLetter` contains `URL`, `Event`, `ID`, `Attempts`, `LastStatus`, and `LastError`. Persisting
failed deliveries is the caller's job. If the callback does not persist them, they are lost. An
error returned by the callback is reported by `Send` as an `*Error` with code
`DEAD_LETTER_CALLBACK_FAILED`, which matches `errors.Is(err, outbox.ErrDeadLetterCallback)`, and
the callback's own error is reachable through `errors.Is` as well.

## Errors

Configuration and input problems return an `*Error` with a `Code`. Use `errors.Is` with the sentinel values:

| Sentinel               | Code                          | When                                                                 |
| ---------------------- | ----------------------------- | -------------------------------------------------------------------- |
| `ErrMalformedSecret`   | `MALFORMED_SECRET`            | The secret is not `whsec_<base64>`. Returned by `NewSender`.         |
| `ErrInvalidURL`        | `INVALID_URL`                 | Not absolute, contains credentials, or uses a scheme other than `https`. |
| `ErrInsecureURL`       | `INSECURE_URL`                | `http://` without `WithAllowInsecureHTTP`.                           |
| `ErrInvalidEvent`      | `INVALID_EVENT`               | Empty `Type`, or `Data` that cannot be serialized to JSON.           |
| `ErrInvalidOptions`    | `INVALID_OPTIONS`             | An option has an invalid or nil value.                               |
| `ErrDeadLetterCallback`| `DEAD_LETTER_CALLBACK_FAILED` | The dead-letter callback returned an error. The cause is wrapped.    |

Delivery failures are not errors. They are reported as `Result{OK: false}`. Error messages never
contain the secret, and the destination URL is not echoed in them.

## Comparison

- **[svix](https://www.svix.com/)** is a full webhook platform, hosted or self-hosted, with endpoint management, dashboards, and a message store. Choose it if you want to outsource webhook delivery as a product.
- **`yuryk-outbox-go`** is a small library for your own process. It sends, signs, and retries, and hands failures to your code. Choose it if you want correct semantics without running another service.

## Development

```sh
gofmt -l .
go vet ./...
go test -race ./...
go build ./...
```

## License

MIT
