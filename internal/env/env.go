// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

// Package env reads Aquifer's AQUIFER_* settings. An invalid value is
// logged and the default is used, so a typo never stops a node starting.
package env

import (
	"log"
	"os"
	"strconv"
	"strings"
)

// Bool reads key as 1/true/yes or 0/false/no.
func Bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes":
		return true
	case "0", "false", "no":
		return false
	default:
		log.Printf("config: invalid %s=%q, using default %v", key, v, def)
		return def
	}
}

// Int64 reads key as a base-10 integer.
func Int64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Printf("config: invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}
