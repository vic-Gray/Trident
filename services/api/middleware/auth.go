// Package middleware contains HTTP middleware for the Trident API.
package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
)

// hashKey returns the HMAC-SHA256 hex digest of key with API_KEY_SALT.
// The salt is read on each call so tests can set the env var at any time.
func hashKey(key string) string {
	salt := []byte(os.Getenv("API_KEY_SALT"))
	mac := hmac.New(sha256.New, salt)
	mac.Write([]byte(key))
	return hex.EncodeToString(mac.Sum(nil))
}

// ParseKeyHashes parses a comma-separated list of HMAC-SHA256 hex digests
// (as stored in API_KEY_HASHES) into a set for O(1) lookup.
func ParseKeyHashes(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, h := range strings.Split(raw, ",") {
		h = strings.TrimSpace(h)
		if h != "" {
			out[h] = struct{}{}
		}
	}
	return out
}

// Auth returns middleware that validates the X-API-Key header on all /v1/*
// and /ws requests. GET /v1/health is always allowed through.
//
// validHashes is the set of accepted HMAC-SHA256 hex digests of valid keys.
// In production, load these from the database / config.
func Auth(validHashes map[string]struct{}, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for the health endpoint.
		if r.Method == http.MethodGet && r.URL.Path == "/v1/health" {
			next.ServeHTTP(w, r)
			return
		}

		key := r.Header.Get("X-API-Key")
		if key == "" {
			http.Error(w, `{"error":"missing X-API-Key header"}`, http.StatusUnauthorized)
			return
		}

		if _, ok := validHashes[hashKey(key)]; !ok {
			http.Error(w, `{"error":"invalid API key"}`, http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
