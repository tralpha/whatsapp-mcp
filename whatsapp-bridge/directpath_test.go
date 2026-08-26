package main

import "testing"

// whatsmeow appends "&hash=..." to the direct path, so the query string that
// carries WhatsApp's CDN auth (oh/oe/_nc_sid) has to survive extraction.
// Dropping it made every media download 403.
func TestExtractDirectPathKeepsQuery(t *testing.T) {
	const url = "https://mmg.whatsapp.net/v/t62.7117-24/787017628_153_n.enc?ccb=11-4&oh=01_Q5Aa5Q&oe=6AB51CD6&_nc_sid=5e03e0&mms3=true"
	got := extractDirectPathFromURL(url)
	want := "/v/t62.7117-24/787017628_153_n.enc?ccb=11-4&oh=01_Q5Aa5Q&oe=6AB51CD6&_nc_sid=5e03e0&mms3=true"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestExtractDirectPathPassesThroughUnrecognised(t *testing.T) {
	if got := extractDirectPathFromURL("not-a-url"); got != "not-a-url" {
		t.Fatalf("got %q", got)
	}
}
