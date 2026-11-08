package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPublicGuard(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h := publicGuard(ok)
	t.Setenv("BRIDGE_PUBLIC_TOKEN", "s3cret")
	cases := []struct {
		host, token string
		want        int
	}{
		{"whatsapp-bridge-hk.railway.internal:8080", "", 200},
		{"localhost:8080", "", 200},
		{"whatsapp-bridge-hk-production.up.railway.app", "", 401},
		{"whatsapp-bridge-hk-production.up.railway.app", "wrong", 401},
		{"whatsapp-bridge-hk-production.up.railway.app", "s3cret", 200},
		{"evil.railway.internal.example.com", "", 401},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/api/send", nil)
		r.Host = c.host
		if c.token != "" {
			r.Header.Set("X-Bridge-Token", c.token)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, r)
		if rr.Code != c.want {
			t.Errorf("%s token=%q: got %d want %d", c.host, c.token, rr.Code, c.want)
		}
	}
}
