package outbox

// ErrorCode identifies the kind of an *Error. The values match the error codes of the
// TypeScript package @yedoma-labs/yuryk-outbox.
type ErrorCode string

// Error codes carried by *Error.
const (
	CodeMalformedSecret    ErrorCode = "MALFORMED_SECRET"
	CodeInvalidURL         ErrorCode = "INVALID_URL"
	CodeInsecureURL        ErrorCode = "INSECURE_URL"
	CodeInvalidEvent       ErrorCode = "INVALID_EVENT"
	CodeInvalidOptions     ErrorCode = "INVALID_OPTIONS"
	CodeDeadLetterCallback ErrorCode = "DEAD_LETTER_CALLBACK_FAILED"
)

// Sentinel errors for use with errors.Is. An *Error matches a sentinel when both carry the same
// Code, so errors.Is(err, ErrInvalidURL) also matches errors with a more specific message.
var (
	// ErrMalformedSecret means the secret is not in the format whsec_<base64>.
	ErrMalformedSecret = &Error{Code: CodeMalformedSecret, msg: "outbox: malformed secret, expected whsec_<base64>"}

	// ErrInvalidURL means the destination URL is not absolute, contains credentials, or uses a
	// scheme other than https (or http with WithAllowInsecureHTTP).
	ErrInvalidURL = &Error{Code: CodeInvalidURL, msg: "outbox: invalid destination URL"}

	// ErrInsecureURL means the destination uses http while WithAllowInsecureHTTP is not set.
	ErrInsecureURL = &Error{Code: CodeInsecureURL, msg: "outbox: destination URL must use https"}

	// ErrInvalidEvent means the event has no type or its data cannot be serialized to JSON.
	ErrInvalidEvent = &Error{Code: CodeInvalidEvent, msg: "outbox: invalid event"}

	// ErrInvalidOptions means an option was given an invalid or nil value.
	ErrInvalidOptions = &Error{Code: CodeInvalidOptions, msg: "outbox: invalid option"}

	// ErrDeadLetterCallback means the WithDeadLetter callback returned an error. The callback's
	// error is available through errors.Unwrap and errors.Is.
	ErrDeadLetterCallback = &Error{Code: CodeDeadLetterCallback, msg: "outbox: dead-letter callback failed"}
)

// Error is returned for configuration and input problems, and for failures of the dead-letter
// callback. Delivery failures are reported through Result.OK instead. Error messages never
// contain the signing secret, and the destination URL is not echoed in them.
type Error struct {
	// Code identifies the kind of error.
	Code ErrorCode

	msg   string
	cause error
}

// Error returns the message of the error.
func (e *Error) Error() string {
	return e.msg
}

// Unwrap returns the underlying error, if any.
func (e *Error) Unwrap() error {
	return e.cause
}

// Is reports whether target is an *Error with the same Code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code != "" && t.Code == e.Code
}

func newError(code ErrorCode, msg string, cause error) *Error {
	return &Error{Code: code, msg: "outbox: " + msg, cause: cause}
}

func invalidOptions(msg string) *Error {
	return newError(CodeInvalidOptions, msg, nil)
}
