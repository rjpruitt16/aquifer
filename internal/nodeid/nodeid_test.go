// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package nodeid

import (
	"errors"
	"testing"
)

func TestNodeIDPrefersExplicitThenClusterThenFlyThenHostname(t *testing.T) {
	env := map[string]string{}
	getenv := func(k string) string { return env[k] }
	host := func() (string, error) { return "box-1", nil }
	if got := Resolve(getenv, host); got != "box-1" {
		t.Fatalf("got %q, want the hostname", got)
	}
	env["FLY_MACHINE_ID"] = "fly-1"
	if got := Resolve(getenv, host); got != "fly-1" {
		t.Fatalf("got %q", got)
	}
	env["AQUIFER_CLUSTER_SELF_ID"] = "a"
	if got := Resolve(getenv, host); got != "a" {
		t.Fatalf("got %q", got)
	}
	env["AQUIFER_NODE_ID"] = "explicit"
	if got := Resolve(getenv, host); got != "explicit" {
		t.Fatalf("got %q", got)
	}
	if got := Resolve(func(string) string { return "" }, func() (string, error) { return "", errors.New("no") }); got != "unknown" {
		t.Fatalf("got %q", got)
	}
}
