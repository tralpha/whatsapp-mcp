package main

import (
	"crypto/subtle"
	"net"
	"net/http"
	"os"
	"strings"
)

// publicGuard lets requests through when they come over Railway's private
// network (Host *.railway.internal) or localhost, which is how the backend
// proxy calls the bridge. Anything else arrived through a public domain and
// must carry X-Bridge-Token = BRIDGE_PUBLIC_TOKEN; with no token configured,
// public access is refused. Before this, the public domains exposed /api/send
// and /api/status to anyone.
func publicGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPrivateHost(r.Host) {
			next.ServeHTTP(w, r)
			return
		}
		want := os.Getenv("BRIDGE_PUBLIC_TOKEN")
		got := r.Header.Get("X-Bridge-Token")
		if want == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isPrivateHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.ToLower(host)
	return strings.HasSuffix(host, ".railway.internal") || host == "localhost" || host == "127.0.0.1" || host == "::1"
}
