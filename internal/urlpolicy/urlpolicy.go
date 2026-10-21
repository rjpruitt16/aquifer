// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

// Package urlpolicy decides which upstream hosts Aquifer may call.
package urlpolicy

import (
	"net/url"
	"os"
	"strings"
)

// Allowed reports whether rawURL's host is in AQUIFER_ALLOWED_URL_DOMAINS
// (comma-separated; a domain also allows its subdomains). An empty list
// allows every host.
func Allowed(rawURL string) bool {
	allowlist := os.Getenv("AQUIFER_ALLOWED_URL_DOMAINS")
	if allowlist == "" {
		return true
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := u.Hostname()

	for _, allowed := range strings.Split(allowlist, ",") {
		allowed = strings.TrimSpace(allowed)
		if allowed == "" {
			continue
		}
		if host == allowed || strings.HasSuffix(host, "."+allowed) {
			return true
		}
	}
	return false
}
