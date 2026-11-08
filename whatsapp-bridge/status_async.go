package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
)

// A status goes to every recipient's devices (~1850 people on hk), which takes
// minutes; Railway dropped the HTTP connection before SendMessage returned, so
// the caller never learned the message id and could not delete the status.
// The id is now chosen up front and returned at once (202); the send runs in
// the background and /api/status/result reports the outcome.

type statusOutcome struct {
	ID     string `json:"message_id"`
	State  string `json:"state"` // pending | sent | failed
	Error  string `json:"error,omitempty"`
	Detail string `json:"detail,omitempty"`
	At     string `json:"at"`
}

var (
	statusMu       sync.Mutex
	statusOutcomes = map[string]statusOutcome{}
)

func setStatusOutcome(o statusOutcome) {
	o.At = time.Now().UTC().Format(time.RFC3339)
	statusMu.Lock()
	statusOutcomes[o.ID] = o
	statusMu.Unlock()
}

func statusResult(id string) statusOutcome {
	statusMu.Lock()
	defer statusMu.Unlock()
	if o, ok := statusOutcomes[id]; ok {
		return o
	}
	return statusOutcome{ID: id, State: "unknown"}
}

func sendStatusAsync(w http.ResponseWriter, client *whatsmeow.Client, msg *waProto.Message, detail string) {
	id := client.GenerateMessageID()
	setStatusOutcome(statusOutcome{ID: id, State: "pending", Detail: detail})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
		defer cancel()
		_, err := client.SendMessage(ctx, types.StatusBroadcastJID, msg, whatsmeow.SendRequestExtra{ID: id})
		if err != nil {
			setStatusOutcome(statusOutcome{ID: id, State: "failed", Error: err.Error(), Detail: detail})
			fmt.Printf("[status] %s failed: %v\n", id, err)
			return
		}
		setStatusOutcome(statusOutcome{ID: id, State: "sent", Detail: detail})
		fmt.Printf("[status] %s sent (%s)\n", id, detail)
	}()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "message_id": id, "state": "pending",
		"message": "status accepted (" + detail + "); poll /api/status/result?id=" + id})
}
