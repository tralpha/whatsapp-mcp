package main

import (
	"encoding/json"
	"strings"
	"testing"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/proto"
)

func TestExtractQuote(t *testing.T) {
	reply := &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{
		Text: proto.String("yes that one"),
		ContextInfo: &waProto.ContextInfo{
			StanzaID:      proto.String("ABC123"),
			Participant:   proto.String("237659099178@s.whatsapp.net"),
			QuotedMessage: &waProto.Message{Conversation: proto.String("Check-in at 2 PM?")},
		},
	}}
	q := extractQuote(reply)
	if q != (quote{ID: "ABC123", Sender: "237659099178", Text: "Check-in at 2 PM?"}) {
		t.Fatalf("text reply: got %+v", q)
	}

	// Voice note replying to a captioned photo.
	voice := &waProto.Message{AudioMessage: &waProto.AudioMessage{ContextInfo: &waProto.ContextInfo{
		StanzaID:      proto.String("IMG1"),
		QuotedMessage: &waProto.Message{ImageMessage: &waProto.ImageMessage{Caption: proto.String("the suite")}},
	}}}
	if q := extractQuote(voice); q.ID != "IMG1" || q.Text != "the suite" {
		t.Fatalf("voice reply: got %+v", q)
	}

	if q := extractQuote(&waProto.Message{Conversation: proto.String("plain")}); q.ID != "" {
		t.Fatalf("plain message should carry no quote: %+v", q)
	}

	// Embedded quote must reach the webhook JSON at top level.
	b, _ := json.Marshal(WebhookPayload{quote: quote{ID: "ABC123", Text: "hi"}})
	if !strings.Contains(string(b), `"quoted_message_id":"ABC123"`) {
		t.Fatalf("webhook json missing quote: %s", b)
	}
	if b, _ := json.Marshal(WebhookPayload{}); strings.Contains(string(b), "quoted") {
		t.Fatalf("non-reply webhook should omit quote fields: %s", b)
	}
}
