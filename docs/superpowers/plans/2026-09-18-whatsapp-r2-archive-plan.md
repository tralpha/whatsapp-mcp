# WhatsApp R2 Archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist successfully retrieved WhatsApp media in a private Cloudflare R2 bucket while keeping the existing PostgreSQL database for messages and archive metadata.

**Architecture:** A small `MediaArchive` interface isolates S3-compatible R2 access from WhatsApp retrieval. `/api/media` checks R2 first, falls back to CDN and phone retry, uploads recovered bytes, and records the private object key on the existing message row. Missing configuration disables archival without changing current behavior.

**Tech Stack:** Go 1.26, AWS SDK for Go v2 S3 client, Cloudflare R2, PostgreSQL, whatsmeow.

---

### Task 1: Archive storage adapter

**Files:**
- Create: `whatsapp-bridge/media_archive.go`
- Create: `whatsapp-bridge/media_archive_test.go`
- Modify: `whatsapp-bridge/go.mod`
- Modify: `whatsapp-bridge/go.sum`

- [ ] Test deterministic private object keys, complete/partial environment configuration, and not-found classification.
- [ ] Implement an interface with `Get` and `Put`, plus an R2 adapter using a private bucket and static scoped credentials.
- [ ] Run `go test ./...`.

### Task 2: Existing-database metadata

**Files:**
- Modify: `whatsapp-bridge/main.go`

- [ ] Add idempotent `archive_key`, `archive_etag`, `archived_at`, and `archive_error` columns to `messages`.
- [ ] Add narrow read/update methods for archive metadata; do not store media bytes in PostgreSQL.
- [ ] Run `go test ./...`.

### Task 3: Archive-first retrieval

**Files:**
- Modify: `whatsapp-bridge/main.go`
- Modify: `whatsapp-bridge/media_archive_test.go`

- [ ] Initialize R2 from environment and pass it into the REST server.
- [ ] Read R2 first when `archive_key` exists.
- [ ] Preserve CDN and phone retry as fallback.
- [ ] Upload successfully recovered bytes and persist the object key; return the media even if archival fails.
- [ ] Run `gofmt`, `go test ./...`, and `go build ./...`.

### Task 4: Configure and verify production

**Files:**
- Modify: `README.md`

- [ ] Document `R2_ACCOUNT_ID`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, and `R2_SECRET_ACCESS_KEY`.
- [ ] Create one private bucket and scoped credentials.
- [ ] Add configuration to HK, CM, and Vita bridge services.
- [ ] Deploy the same commit to all three bridges.
- [ ] Retrieve a disposable HK media item twice, verify its archive metadata, and confirm the second retrieval succeeds from R2.
