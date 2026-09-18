package main

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

func TestMediaRetryCoordinatorJoinsConcurrentRequests(t *testing.T) {
	c := newMediaRetryCoordinator()
	id := types.MessageID("message-1")
	first, leader := c.begin(id, []byte("key"))
	if !leader {
		t.Fatal("first request must lead")
	}
	second, leader := c.begin(id, []byte("other"))
	if leader || first != second {
		t.Fatal("second request must join the pending retry")
	}
}

func TestMediaRetryCoordinatorWaitHonorsCancellation(t *testing.T) {
	c := newMediaRetryCoordinator()
	id := types.MessageID("message-2")
	p, _ := c.begin(id, []byte("key"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.wait(ctx, id, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if _, ok := c.pending[id]; ok {
		t.Fatal("cancelled retry was not removed")
	}
}

func TestShouldRetryMediaDownload(t *testing.T) {
	for _, err := range []error{
		whatsmeow.ErrMediaDownloadFailedWith403,
		whatsmeow.ErrMediaDownloadFailedWith404,
		whatsmeow.ErrMediaDownloadFailedWith410,
	} {
		if !shouldRetryMediaDownload(err) {
			t.Fatalf("expected retry for %v", err)
		}
	}
	if shouldRetryMediaDownload(errors.New("network down")) {
		t.Fatal("unexpected retry for generic error")
	}
}
