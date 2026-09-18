# Durable WhatsApp Media Design

## Goal

Keep the existing PostgreSQL database as the source of message metadata while making media retrievable without opening WhatsApp. Media remains lazy: the bridge archives a file only when a caller first requests it. If WhatsApp's CDN copy has expired, the bridge asks the primary device to re-upload it before reporting failure.

The behavior applies consistently to the `hk`, `cm`, and `vita` bridge services.

## Storage model

Cloudflare R2 stores decrypted media bytes. PostgreSQL stores only archive metadata on the existing `messages` row:

- `archive_key`: private R2 object key, unique per account, chat, and message
- `archive_etag`: integrity and diagnostic value returned by object storage
- `archived_at`: successful archive timestamp
- `archive_error`: last background or on-demand archival error

No second database is introduced. Existing WhatsApp URL, encryption key, hashes, and length remain available for live download and retry.

R2 uses its S3-compatible API and a private bucket. Each bridge receives the same endpoint, bucket, access key, and secret through Railway environment variables. Object keys begin with the bridge account so the three services cannot collide.

## Retrieval flow

`GET /api/media` follows this order:

1. Read the message row and its archive metadata.
2. If `archive_key` exists, fetch the object from R2 and stream it to the caller.
3. Otherwise, attempt the current WhatsApp CDN download.
4. If WhatsApp reports an expired or missing CDN object, send a media retry receipt to the primary phone and wait for the matching retry notification for a bounded period.
5. On a successful notification, update the stored WhatsApp media path and retry the download.
6. Upload successfully decrypted bytes to R2, persist the archive metadata, and return the bytes.

The first successful retrieval therefore makes that item durable. Later requests do not depend on WhatsApp or the primary phone.

## Retry coordination

The bridge keeps an in-memory map of pending media retry requests keyed by message ID. Each entry contains the stored media key and a one-result channel. The existing WhatsApp event handler routes `events.MediaRetry` into this coordinator.

Only one retry may be active for a message. Concurrent callers share its result. The request has a short timeout so `/api/media` cannot block indefinitely. A notification that says the media is unavailable on the phone becomes a clear terminal error; it does not delete existing metadata.

Successful retry data supplies a fresh direct path. The bridge persists a reconstructed current URL or a separate direct-path field before downloading so subsequent attempts can reuse it if archival upload transiently fails.

## Failure behavior

- R2 unavailable with a live WhatsApp copy: return the media to the caller and record `archive_error`; a later request retries archival.
- R2 object missing despite `archive_key`: clear or bypass the stale archive reference and try WhatsApp recovery.
- WhatsApp CDN expired and phone cannot re-upload: return a specific `media unavailable on linked devices` error.
- Retry timeout: return a retriable gateway error.
- Invalid or incomplete metadata: preserve the current explicit error.

R2 failure must never prevent delivery of bytes already downloaded from WhatsApp.

## Security and privacy

The R2 bucket remains private. Credentials exist only in Railway variables. Object keys use message identifiers and hashed chat identifiers rather than contact names or message text. The existing authenticated backend proxy remains the only public download route.

## Testing

Unit tests cover:

- archive-first retrieval
- live WhatsApp download followed by archive upload
- R2 upload failure with successful response delivery
- expired CDN followed by successful media retry
- retry timeout and phone-unavailable responses
- concurrent requests sharing one retry
- deterministic, collision-safe object keys
- migrations remaining idempotent

The bridge test suite and build must pass before deployment. After deployment, upload a disposable image to the HK self-chat, retrieve it through the admin proxy, confirm the archive metadata, and retrieve it again after bypassing the WhatsApp URL to prove the R2 path. Repeat a lightweight smoke test on CM and Vita because all three services use the same image.

## Deployment

Create one private R2 bucket and scoped object read/write credentials. Add the R2 variables to all three bridge services, deploy the same commit, and verify each service health endpoint. The feature remains compatible with rows created before the migration: old media has a null `archive_key` and enters the WhatsApp download or media-retry flow on first access.
