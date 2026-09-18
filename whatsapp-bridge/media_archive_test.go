package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeArchive struct {
	data   []byte
	getErr error
	putErr error
	putKey string
}

func (archive *fakeArchive) Get(context.Context, string) ([]byte, error) {
	return archive.data, archive.getErr
}

func (archive *fakeArchive) Put(_ context.Context, key, _ string, _ []byte) (string, error) {
	archive.putKey = key
	return "etag", archive.putErr
}

type fakeArchiveStore struct {
	key       string
	savedKey  string
	savedETag string
	savedErr  error
}

func (store *fakeArchiveStore) GetArchiveKey(string, string) (string, error) { return store.key, nil }
func (store *fakeArchiveStore) StoreArchiveSuccess(_, _, key, etag string) error {
	store.savedKey, store.savedETag = key, etag
	return nil
}
func (store *fakeArchiveStore) StoreArchiveError(_, _ string, err error) error {
	store.savedErr = err
	return nil
}

func TestArchiveObjectKeyIsDeterministicAndPrivate(t *testing.T) {
	key := archiveObjectKey("hk", "144907951948023@lid", "ABC123", "photo.jpg")
	if key != archiveObjectKey("hk", "144907951948023@lid", "ABC123", "photo.jpg") {
		t.Fatal("object key is not deterministic")
	}
	if strings.Contains(key, "144907951948023") {
		t.Fatal("object key leaks the chat identifier")
	}
	if !strings.HasSuffix(key, "/ABC123/photo.jpg") {
		t.Fatalf("unexpected object key: %s", key)
	}
}

func TestArchiveConfigurationCanBeDisabled(t *testing.T) {
	for _, name := range []string{"R2_ACCOUNT_ID", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"} {
		t.Setenv(name, "")
	}
	archive, err := newMediaArchiveFromEnv(context.Background())
	if err != nil || archive != nil {
		t.Fatalf("expected disabled archive, got archive=%v err=%v", archive, err)
	}
}

func TestArchiveConfigurationRejectsPartialValues(t *testing.T) {
	t.Setenv("R2_ACCOUNT_ID", "account")
	t.Setenv("R2_BUCKET", "")
	t.Setenv("R2_ACCESS_KEY_ID", "")
	t.Setenv("R2_SECRET_ACCESS_KEY", "")
	if _, err := newMediaArchiveFromEnv(context.Background()); err == nil {
		t.Fatal("expected partial configuration error")
	}
}

func TestLoadArchivedMediaReturnsStoredBytes(t *testing.T) {
	archive := &fakeArchive{data: []byte("archived")}
	store := &fakeArchiveStore{key: "hk/chat/message/photo.jpg"}
	data, ok := loadArchivedMedia(context.Background(), archive, store, "message", "chat")
	if !ok || string(data) != "archived" {
		t.Fatalf("expected archive hit, got ok=%v data=%q", ok, data)
	}
}

func TestLoadArchivedMediaFallsBackAndRecordsFailure(t *testing.T) {
	wantErr := errors.New("R2 unavailable")
	archive := &fakeArchive{getErr: wantErr}
	store := &fakeArchiveStore{key: "hk/chat/message/photo.jpg"}
	if _, ok := loadArchivedMedia(context.Background(), archive, store, "message", "chat"); ok {
		t.Fatal("expected archive miss")
	}
	if !errors.Is(store.savedErr, wantErr) {
		t.Fatalf("expected recorded error, got %v", store.savedErr)
	}
}

func TestStoreRecoveredMediaPersistsObjectMetadata(t *testing.T) {
	archive := &fakeArchive{}
	store := &fakeArchiveStore{}
	storeRecoveredMedia(context.Background(), archive, store, "hk", "message", "chat@lid", "photo.jpg", "image/jpeg", []byte("photo"))
	if archive.putKey == "" || store.savedKey != archive.putKey || store.savedETag != "etag" {
		t.Fatalf("archive metadata not persisted: put=%q key=%q etag=%q", archive.putKey, store.savedKey, store.savedETag)
	}
}
