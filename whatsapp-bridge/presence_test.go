package main

import "testing"

// One waiter under two keys must survive events on both keys (no double
// close panic) and be fully unregistered afterwards.
func TestPresenceWaiterTwoKeys(t *testing.T) {
	keys := []string{"237600000000@s.whatsapp.net", "123@lid"}
	ch := make(chan struct{}, 1)
	presenceMu.Lock()
	for _, k := range keys {
		presenceWaiters[k] = append(presenceWaiters[k], ch)
	}
	presenceMu.Unlock()
	notifyPresence(keys[0])
	notifyPresence(keys[1])
	<-ch
	removePresenceWaiter(keys, ch)
	for _, k := range keys {
		if len(presenceWaiters[k]) != 0 {
			t.Fatalf("waiter left under %s", k)
		}
	}
}
