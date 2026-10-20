package main

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestResolveMentions(t *testing.T) {
	dm := types.JID{User: "237655432556", Server: types.DefaultUserServer}
	text, jids := resolveMentions(nil, dm, "hi @23777710612 @1@lid", []string{"+23777710612", "1@lid", "bad@@"})
	if text != "hi @23777710612 @1@lid" {
		t.Fatalf("text rewritten in a DM: %q", text)
	}
	want := []string{"23777710612@s.whatsapp.net", "1@lid"}
	if len(jids) != len(want) || jids[0] != want[0] || jids[1] != want[1] {
		t.Fatalf("jids = %v, want %v", jids, want)
	}
}
