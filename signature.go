package outbox

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
)

// sign computes the Standard Webhooks signature "v1,<base64 HMAC-SHA256>" over
// "<id>.<timestamp>.<body>". The timestamp is in whole seconds.
func sign(key []byte, id string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(id + "." + strconv.FormatInt(timestamp, 10) + "."))
	_, _ = mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
