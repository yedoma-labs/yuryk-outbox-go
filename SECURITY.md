# Security Policy

## Supported Versions

Only the latest released version receives security fixes.

## Reporting a Vulnerability

Please do not open a public issue for security problems. Report them privately through
GitHub's Private Vulnerability Reporting:

1. Open the repository on GitHub.
2. Go to the **Security** tab.
3. Click **Report a vulnerability** and describe the issue.

Include the affected version, steps to reproduce, and the expected and actual behavior. You will
get an acknowledgement, and the report stays private until a fix is released.

## Scope

In scope:

- Leaks of the signing secret or of the HMAC key through error messages, dead letters, or logs
  produced by this package.
- Signatures that do not match the body, or a body, ID, or timestamp that changes between retries.
- Requests sent to destinations that validation should have rejected, such as `http:` without the
  opt-in, or URLs with credentials.
- Redirects being followed, which would send a signed request to an unvalidated location.
- Response bodies being read when the package does not need them.

Out of scope:

- Choosing which destinations are allowed. The caller decides the URL, so allow-listing and
  protection against server-side request forgery belong to the caller.
- Persisting dead letters. The caller owns that storage.
- Receivers that do not deduplicate by `webhook-id`. Delivery is at-least-once by design.
