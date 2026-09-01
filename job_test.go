package aquifer

import "testing"

func validRequest(rawURL string) JobRequest {
	return JobRequest{
		UserID:        "user-1",
		IdempotentKey: "key-1",
		URL:           rawURL,
		Method:        "POST",
		WebhookURL:    "https://example.com/callback",
	}
}

func TestDomainAllowlistUnsetMeansUnrestricted(t *testing.T) {
	req := validRequest("https://anything.example/whatever")
	if msg := req.Validate(); msg != "" {
		t.Fatalf("expected no allowlist configured to permit any domain, got: %q", msg)
	}
}

func TestDomainAllowlistExactMatch(t *testing.T) {
	t.Setenv("AQUIFER_ALLOWED_URL_DOMAINS", "api.openai.com,example.com")

	req := validRequest("https://api.openai.com/v1/chat/completions")
	if msg := req.Validate(); msg != "" {
		t.Fatalf("expected exact-match domain to be allowed, got: %q", msg)
	}
}

func TestDomainAllowlistSubdomainMatch(t *testing.T) {
	t.Setenv("AQUIFER_ALLOWED_URL_DOMAINS", "example.com")

	req := validRequest("https://api.example.com/whatever")
	if msg := req.Validate(); msg != "" {
		t.Fatalf("expected subdomain of an allowlisted domain to be allowed, got: %q", msg)
	}
}

func TestDomainAllowlistRejectsUnlistedDomain(t *testing.T) {
	t.Setenv("AQUIFER_ALLOWED_URL_DOMAINS", "example.com")

	req := validRequest("https://evil.attacker.example/exfiltrate")
	msg := req.Validate()
	if msg == "" {
		t.Fatalf("expected a domain outside the allowlist to be rejected")
	}
	if msg != "url domain is not in the configured allowlist (AQUIFER_ALLOWED_URL_DOMAINS)" {
		t.Fatalf("unexpected rejection message: %q", msg)
	}
}

func TestDomainAllowlistDoesNotFalsePositiveOnSuffixWithoutDot(t *testing.T) {
	// "notexample.com" must NOT be treated as a subdomain of "example.com" --
	// confirms the check is "." + allowed, not a raw strings.HasSuffix on the
	// allowed string itself.
	t.Setenv("AQUIFER_ALLOWED_URL_DOMAINS", "example.com")

	req := validRequest("https://notexample.com/whatever")
	if msg := req.Validate(); msg == "" {
		t.Fatalf("expected notexample.com to be rejected as a distinct domain from example.com")
	}
}

func TestDomainAllowlistIgnoresPoolRoutedJobs(t *testing.T) {
	t.Setenv("AQUIFER_ALLOWED_URL_DOMAINS", "example.com")

	req := JobRequest{
		UserID:        "user-1",
		IdempotentKey: "key-1",
		PoolID:        "pool-1",
		Method:        "POST",
		WebhookURL:    "https://example.com/callback",
	}
	if msg := req.Validate(); msg != "" {
		t.Fatalf("expected a pool-routed job (no caller-supplied URL) to skip the allowlist check, got: %q", msg)
	}
}
