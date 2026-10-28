package outbox

import (
	"encoding/base64"
	"strings"
)

const secretPrefix = "whsec_"

// decodeSecret decodes a Standard Webhooks secret (whsec_<base64>) into the raw HMAC key. The
// error never contains the secret.
func decodeSecret(secret string) ([]byte, error) {
	encoded, ok := strings.CutPrefix(secret, secretPrefix)
	if !ok || encoded == "" {
		return nil, newError(CodeMalformedSecret, "secret must have the format whsec_<base64>", nil)
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(key) == 0 {
		return nil, newError(CodeMalformedSecret, "secret must have the format whsec_<base64>", nil)
	}
	// DecodeString silently accepts line breaks and non-zero trailing bits. Re-encoding and
	// comparing rejects every input that is not the canonical form.
	if base64.StdEncoding.EncodeToString(key) != encoded {
		return nil, newError(CodeMalformedSecret, "secret must have the format whsec_<base64>", nil)
	}
	return key, nil
}
