# WhatsApp Media Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover expired WhatsApp attachments through the primary phone so the five requested HK media items can be downloaded without opening WhatsApp.

**Architecture:** Add a concurrency-safe retry coordinator beside the existing media downloader. `/api/media` performs its normal download, requests a phone re-upload only for WhatsApp 404/410 expiry errors, consumes the matching `events.MediaRetry`, persists the refreshed direct path, and downloads again.

**Tech Stack:** Go 1.26, whatsmeow, PostgreSQL, `net/http`, Go tests.

---

### Task 1: Retry coordinator

**Files:**
- Create: `whatsapp-bridge/media_retry.go`
- Create: `whatsapp-bridge/media_retry_test.go`

- [ ] **Step 1: Write failing coordinator tests**

Test registration, duplicate callers sharing a pending request, successful event delivery, cleanup, and timeout using short test contexts.

- [ ] **Step 2: Run tests and verify failure**

Run: `go test ./...`
Expected: compile failure because `mediaRetryCoordinator` is undefined.

- [ ] **Step 3: Implement the coordinator**

Define a mutex-protected map keyed by `types.MessageID`. A pending entry stores the media key, result channel, and waiter count. Provide methods to begin/join a request, deliver a decrypted retry notification, and remove completed or timed-out entries.

- [ ] **Step 4: Run tests**

Run: `go test ./...`
Expected: PASS.

### Task 2: Recover expired downloads

**Files:**
- Modify: `whatsapp-bridge/main.go`
- Modify: `whatsapp-bridge/media_retry_test.go`

- [ ] **Step 1: Write failing recovery tests**

Cover message-info reconstruction from stored `chat_jid`, sender, and `is_from_me`; refreshed direct-path persistence; and the decision to retry only on `whatsmeow.ErrMediaDownloadFailedWith404` or `whatsmeow.ErrMediaDownloadFailedWith410`.

- [ ] **Step 2: Run tests and verify failure**

Run: `go test ./...`
Expected: failure for missing recovery helpers.

- [ ] **Step 3: Implement message metadata and recovery**

Extend the existing media row query to include sender and direction. Parse the JIDs with `types.ParseJID`, call `client.SendMediaRetryReceipt`, wait for the coordinator result with a bounded context, store the new direct path, and rerun `client.Download`.

- [ ] **Step 4: Route retry events**

Instantiate one coordinator in `main`, pass it to the REST server, and add an `*events.MediaRetry` case to the existing event switch.

- [ ] **Step 5: Run formatting, tests, and build**

Run: `gofmt -w whatsapp-bridge/main.go whatsapp-bridge/media_retry.go whatsapp-bridge/media_retry_test.go && go test ./... && go build ./...`
Expected: all commands succeed.

### Task 3: Deploy and recover the requested set

**Files:**
- Modify: `docs/superpowers/plans/2026-09-18-whatsapp-media-retry-plan.md` only to check completed tasks.

- [ ] **Step 1: Commit and push**

Commit the tested recovery implementation and push the feature branch to GitHub.

- [ ] **Step 2: Deploy HK bridge**

Deploy the tested commit to `whatsapp-bridge-hk`, wait for a healthy connected instance, and verify `/api/health` through Railway logs or the service endpoint.

- [ ] **Step 3: Recover exact media IDs**

Call the authenticated admin media endpoint for:

- `3A13C48C9737FCAEFF85`
- `3A922F9A06859BCFA6B7`
- `3A5BAC5B2DC2B14B97D8`
- `3A4D5449C2BF10227E8F`
- `3A1E33E20EE0F1AD79CF`

Validate each file with `file`, SHA-256, and image/video decoding.

- [ ] **Step 4: Present the set**

Copy the recovered files into `/Users/ralphtigoumo/vita_resort/output/whatsapp/larissa-review/`, show all images and a video preview, then place the exact proposed Larissa bubbles below them. Do not send anything until Ralph approves.
