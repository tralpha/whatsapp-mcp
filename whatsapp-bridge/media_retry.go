package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type mediaRetryResult struct {
	directPath string
	err        error
}

type pendingMediaRetry struct {
	mediaKey []byte
	done     chan struct{}
	result   mediaRetryResult
}

// mediaRetryCoordinator joins concurrent requests for the same message and
// routes whatsmeow's asynchronous MediaRetry event back to the HTTP request.
type mediaRetryCoordinator struct {
	mu      sync.Mutex
	pending map[types.MessageID]*pendingMediaRetry
}

func newMediaRetryCoordinator() *mediaRetryCoordinator {
	return &mediaRetryCoordinator{pending: make(map[types.MessageID]*pendingMediaRetry)}
}

func (c *mediaRetryCoordinator) begin(id types.MessageID, mediaKey []byte) (*pendingMediaRetry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.pending[id]; ok {
		return existing, false
	}
	p := &pendingMediaRetry{mediaKey: append([]byte(nil), mediaKey...), done: make(chan struct{})}
	c.pending[id] = p
	return p, true
}

func (c *mediaRetryCoordinator) wait(ctx context.Context, id types.MessageID, p *pendingMediaRetry) (string, error) {
	select {
	case <-p.done:
		return p.result.directPath, p.result.err
	case <-ctx.Done():
		c.mu.Lock()
		if c.pending[id] == p {
			delete(c.pending, id)
		}
		c.mu.Unlock()
		return "", ctx.Err()
	}
}

func (c *mediaRetryCoordinator) deliver(evt *events.MediaRetry) bool {
	c.mu.Lock()
	p, ok := c.pending[evt.MessageID]
	if !ok {
		c.mu.Unlock()
		return false
	}
	delete(c.pending, evt.MessageID)
	c.mu.Unlock()

	notification, err := whatsmeow.DecryptMediaRetryNotification(evt, p.mediaKey)
	if err == nil {
		directPath := notification.GetDirectPath()
		if directPath == "" {
			err = errors.New("media retry returned an empty direct path")
		} else {
			p.result.directPath = directPath
		}
	}
	if err != nil {
		p.result.err = fmt.Errorf("media retry failed: %w", err)
	}
	close(p.done)
	return true
}

func shouldRetryMediaDownload(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

// persistLateMediaRetry saves a re-upload reply that arrived after the HTTP
// request stopped waiting. Phones often answer minutes or hours later (when they
// wake), so without this the reply is lost and every fetch times out again.
func persistLateMediaRetry(store *MessageStore, evt *events.MediaRetry) error {
	chatJID := evt.ChatID.String()
	_, _, _, mediaKey, _, _, _, err := store.GetMediaInfo(string(evt.MessageID), chatJID)
	if err != nil {
		return fmt.Errorf("load media key: %w", err)
	}
	notification, err := whatsmeow.DecryptMediaRetryNotification(evt, mediaKey)
	if err != nil {
		return fmt.Errorf("decrypt: %w", err)
	}
	directPath := notification.GetDirectPath()
	if directPath == "" {
		return errors.New("empty direct path")
	}
	return store.UpdateMediaURL(string(evt.MessageID), chatJID, directPath)
}
