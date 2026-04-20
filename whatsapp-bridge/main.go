// Package main is a WhatsApp bridge using whatsmeow.
// Patched for Vita Resort: Postgres-backed (no SQLite/volume), webhook on incoming, env-based config.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/mdp/qrterminal"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// envOrDefault returns the env var or fallback.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// requireEnv aborts if env var missing.
func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "FATAL: missing required env var %s\n", key)
		os.Exit(2)
	}
	return v
}

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Database handler for storing message history
type MessageStore struct {
	db *sql.DB
}

// NewMessageStore connects to Postgres and ensures the chats/messages tables exist.
// All tables live inside the schema configured via search_path on the DSN.
func NewMessageStore(dsn string) (*MessageStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMPTZ
		);
		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMPTZ,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BYTEA,
			file_sha256 BYTEA,
			file_enc_sha256 BYTEA,
			file_length BIGINT,
			PRIMARY KEY (id, chat_jid)
		);
		CREATE TABLE IF NOT EXISTS calls (
			call_id TEXT PRIMARY KEY,
			from_jid TEXT NOT NULL,
			call_creator TEXT,
			is_group BOOLEAN DEFAULT FALSE,
			media TEXT,
			offered_at TIMESTAMPTZ,
			accepted_at TIMESTAMPTZ,
			terminated_at TIMESTAMPTZ,
			rejected_at TIMESTAMPTZ,
			terminate_reason TEXT,
			platform TEXT,
			version TEXT
		);
		CREATE INDEX IF NOT EXISTS calls_offered_at_idx ON calls (offered_at DESC);
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create tables: %w", err)
	}
	return &MessageStore{db: db}, nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Store a chat in the database (Postgres upsert).
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		`INSERT INTO chats (jid, name, last_message_time) VALUES ($1, $2, $3)
		 ON CONFLICT (jid) DO UPDATE SET name = EXCLUDED.name, last_message_time = EXCLUDED.last_message_time`,
		jid, name, lastMessageTime,
	)
	return err
}

// Store a message in the database (Postgres upsert).
func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	if content == "" && mediaType == "" {
		return nil
	}
	_, err := store.db.Exec(
		`INSERT INTO messages
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (id, chat_jid) DO UPDATE SET
			sender = EXCLUDED.sender,
			content = EXCLUDED.content,
			timestamp = EXCLUDED.timestamp,
			is_from_me = EXCLUDED.is_from_me,
			media_type = EXCLUDED.media_type,
			filename = EXCLUDED.filename,
			url = EXCLUDED.url,
			media_key = EXCLUDED.media_key,
			file_sha256 = EXCLUDED.file_sha256,
			file_enc_sha256 = EXCLUDED.file_enc_sha256,
			file_length = EXCLUDED.file_length`,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	)
	return err
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		`SELECT sender, content, timestamp, is_from_me, media_type, filename
		 FROM messages WHERE chat_jid = $1 ORDER BY timestamp DESC LIMIT $2`,
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}
	return messages, nil
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query(`SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}
	return chats, nil
}

// RecordCallOffer upserts the initial CALL row when an incoming WhatsApp call arrives.
// Both CallOffer (1:1) and CallOfferNotice (group/offline) land here.
func (store *MessageStore) RecordCallOffer(callID, fromJID, callCreator, media, platform, version string, isGroup bool, ts time.Time) error {
	if callID == "" {
		return nil
	}
	_, err := store.db.Exec(
		`INSERT INTO calls (call_id, from_jid, call_creator, is_group, media, offered_at, platform, version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		 ON CONFLICT (call_id) DO UPDATE SET
		   from_jid = EXCLUDED.from_jid,
		   call_creator = COALESCE(NULLIF(EXCLUDED.call_creator,''), calls.call_creator),
		   is_group = EXCLUDED.is_group,
		   media = COALESCE(NULLIF(EXCLUDED.media,''), calls.media),
		   offered_at = COALESCE(calls.offered_at, EXCLUDED.offered_at),
		   platform = COALESCE(NULLIF(EXCLUDED.platform,''), calls.platform),
		   version = COALESCE(NULLIF(EXCLUDED.version,''), calls.version)`,
		callID, fromJID, callCreator, isGroup, media, ts, platform, version,
	)
	return err
}

// RecordCallAccept stamps the moment the call was picked up.
func (store *MessageStore) RecordCallAccept(callID string, ts time.Time) error {
	if callID == "" {
		return nil
	}
	_, err := store.db.Exec(
		`UPDATE calls SET accepted_at = $2 WHERE call_id = $1 AND accepted_at IS NULL`,
		callID, ts,
	)
	return err
}

// RecordCallReject stamps the moment the callee rejected the call.
func (store *MessageStore) RecordCallReject(callID string, ts time.Time) error {
	if callID == "" {
		return nil
	}
	_, err := store.db.Exec(
		`UPDATE calls SET rejected_at = $2 WHERE call_id = $1 AND rejected_at IS NULL`,
		callID, ts,
	)
	return err
}

// RecordCallTerminate stamps the moment the call ended + its reason.
// With accepted_at NULL, terminate means the call was missed (never picked up).
func (store *MessageStore) RecordCallTerminate(callID, reason string, ts time.Time) error {
	if callID == "" {
		return nil
	}
	_, err := store.db.Exec(
		`UPDATE calls SET terminated_at = $2, terminate_reason = $3
		 WHERE call_id = $1 AND terminated_at IS NULL`,
		callID, ts, reason,
	)
	return err
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}
	return ""
}

// SendMessageResponse represents the response for the send message API
type SendMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SendMessageRequest represents the request body for the send message API
type SendMessageRequest struct {
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
	MediaPath string `json:"media_path,omitempty"`
}

// WebhookPayload is what we POST to WEBHOOK_URL on every incoming message.
type WebhookPayload struct {
	Account     string    `json:"account"`
	MessageID   string    `json:"message_id"`
	ChatJID     string    `json:"chat_jid"`
	Sender      string    `json:"sender"`
	SenderName  string    `json:"sender_name,omitempty"`
	IsFromMe    bool      `json:"is_from_me"`
	IsGroup     bool      `json:"is_group"`
	Content     string    `json:"content"`
	MediaType   string    `json:"media_type,omitempty"`
	Filename    string    `json:"filename,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

// Function to send a WhatsApp message
func sendWhatsAppMessage(client *whatsmeow.Client, recipient string, message string, mediaPath string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}

	var recipientJID types.JID
	var err error
	isJID := strings.Contains(recipient, "@")
	if isJID {
		recipientJID, err = types.ParseJID(recipient)
		if err != nil {
			return false, fmt.Sprintf("Error parsing JID: %v", err)
		}
	} else {
		recipientJID = types.JID{User: recipient, Server: "s.whatsapp.net"}
	}

	msg := &waProto.Message{}
	if mediaPath != "" {
		mediaData, err := os.ReadFile(mediaPath)
		if err != nil {
			return false, fmt.Sprintf("Error reading media file: %v", err)
		}
		fileExt := strings.ToLower(mediaPath[strings.LastIndex(mediaPath, ".")+1:])
		var mediaType whatsmeow.MediaType
		var mimeType string
		switch fileExt {
		case "jpg", "jpeg":
			mediaType, mimeType = whatsmeow.MediaImage, "image/jpeg"
		case "png":
			mediaType, mimeType = whatsmeow.MediaImage, "image/png"
		case "gif":
			mediaType, mimeType = whatsmeow.MediaImage, "image/gif"
		case "webp":
			mediaType, mimeType = whatsmeow.MediaImage, "image/webp"
		case "ogg":
			mediaType, mimeType = whatsmeow.MediaAudio, "audio/ogg; codecs=opus"
		case "mp4":
			mediaType, mimeType = whatsmeow.MediaVideo, "video/mp4"
		case "avi":
			mediaType, mimeType = whatsmeow.MediaVideo, "video/avi"
		case "mov":
			mediaType, mimeType = whatsmeow.MediaVideo, "video/quicktime"
		default:
			mediaType, mimeType = whatsmeow.MediaDocument, "application/octet-stream"
		}
		resp, err := client.Upload(context.Background(), mediaData, mediaType)
		if err != nil {
			return false, fmt.Sprintf("Error uploading media: %v", err)
		}
		switch mediaType {
		case whatsmeow.MediaImage:
			msg.ImageMessage = &waProto.ImageMessage{
				Caption: proto.String(message), Mimetype: proto.String(mimeType),
				URL: &resp.URL, DirectPath: &resp.DirectPath, MediaKey: resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256, FileSHA256: resp.FileSHA256, FileLength: &resp.FileLength,
			}
		case whatsmeow.MediaAudio:
			var seconds uint32 = 30
			var waveform []byte
			if strings.Contains(mimeType, "ogg") {
				analyzedSeconds, analyzedWaveform, err := analyzeOggOpus(mediaData)
				if err == nil {
					seconds, waveform = analyzedSeconds, analyzedWaveform
				} else {
					return false, fmt.Sprintf("Failed to analyze Ogg Opus file: %v", err)
				}
			}
			msg.AudioMessage = &waProto.AudioMessage{
				Mimetype: proto.String(mimeType), URL: &resp.URL, DirectPath: &resp.DirectPath,
				MediaKey: resp.MediaKey, FileEncSHA256: resp.FileEncSHA256, FileSHA256: resp.FileSHA256,
				FileLength: &resp.FileLength, Seconds: proto.Uint32(seconds), PTT: proto.Bool(true), Waveform: waveform,
			}
		case whatsmeow.MediaVideo:
			msg.VideoMessage = &waProto.VideoMessage{
				Caption: proto.String(message), Mimetype: proto.String(mimeType),
				URL: &resp.URL, DirectPath: &resp.DirectPath, MediaKey: resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256, FileSHA256: resp.FileSHA256, FileLength: &resp.FileLength,
			}
		case whatsmeow.MediaDocument:
			msg.DocumentMessage = &waProto.DocumentMessage{
				Title: proto.String(mediaPath[strings.LastIndex(mediaPath, "/")+1:]),
				Caption: proto.String(message), Mimetype: proto.String(mimeType),
				URL: &resp.URL, DirectPath: &resp.DirectPath, MediaKey: resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256, FileSHA256: resp.FileSHA256, FileLength: &resp.FileLength,
			}
		}
	} else {
		msg.Conversation = proto.String(message)
	}

	_, err = client.SendMessage(context.Background(), recipientJID, msg)
	if err != nil {
		return false, fmt.Sprintf("Error sending message: %v", err)
	}
	return true, fmt.Sprintf("Message sent to %s", recipient)
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + time.Now().Format("20060102_150405") + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + time.Now().Format("20060102_150405") + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + time.Now().Format("20060102_150405") + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + time.Now().Format("20060102_150405")
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}
	return "", "", "", nil, nil, nil, 0
}

// fireWebhook posts a payload to WEBHOOK_URL (best-effort, non-blocking).
func fireWebhook(payload WebhookPayload, webhookURL, webhookSecret string) {
	if webhookURL == "" {
		return
	}
	go func() {
		body, err := json.Marshal(payload)
		if err != nil {
			fmt.Printf("[webhook] marshal error: %v\n", err)
			return
		}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, webhookURL, bytes.NewReader(body))
		if err != nil {
			fmt.Printf("[webhook] new request: %v\n", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if webhookSecret != "" {
			req.Header.Set("X-Webhook-Secret", webhookSecret)
		}
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("[webhook] POST %s: %v\n", webhookURL, err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			fmt.Printf("[webhook] POST %s: status %d\n", webhookURL, resp.StatusCode)
		}
	}()
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger,
	account, webhookURL, webhookSecret string) {
	chatJID := msg.Info.Chat.String()
	sender := msg.Info.Sender.User
	name := GetChatName(client, messageStore, msg.Info.Chat, chatJID, nil, sender, logger)

	if err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp); err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	content := extractTextContent(msg.Message)
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	if content == "" && mediaType == "" {
		return
	}

	if err := messageStore.StoreMessage(
		msg.Info.ID, chatJID, sender, content, msg.Info.Timestamp, msg.Info.IsFromMe,
		mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	); err != nil {
		logger.Warnf("Failed to store message: %v", err)
	}

	timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
	direction := "←"
	if msg.Info.IsFromMe {
		direction = "→"
	}
	if mediaType != "" {
		fmt.Printf("[%s] %s %s: [%s: %s] %s\n", timestamp, direction, sender, mediaType, filename, content)
	} else if content != "" {
		fmt.Printf("[%s] %s %s: %s\n", timestamp, direction, sender, content)
	}

	// Fire webhook for inbound messages only (don't echo our own sends).
	if !msg.Info.IsFromMe {
		fireWebhook(WebhookPayload{
			Account:    account,
			MessageID:  msg.Info.ID,
			ChatJID:    chatJID,
			Sender:     sender,
			SenderName: name,
			IsFromMe:   false,
			IsGroup:    msg.Info.Chat.Server == "g.us",
			Content:    content,
			MediaType:  mediaType,
			Filename:   filename,
			Timestamp:  msg.Info.Timestamp,
		}, webhookURL, webhookSecret)
	}
}

// DownloadMediaRequest represents the request body for the download media API
type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

// DownloadMediaResponse represents the response for the download media API
type DownloadMediaResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		`UPDATE messages SET url = $1, media_key = $2, file_sha256 = $3, file_enc_sha256 = $4, file_length = $5
		 WHERE id = $6 AND chat_jid = $7`,
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Get media info from the database
func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	err := store.db.QueryRow(
		`SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length
		 FROM messages WHERE id = $1 AND chat_jid = $2`,
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)
	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}

// MediaDownloader implements the whatsmeow.DownloadableMessage interface
type MediaDownloader struct {
	URL           string
	DirectPath    string
	MediaKey      []byte
	FileLength    uint64
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     whatsmeow.MediaType
}

func (d *MediaDownloader) GetDirectPath() string             { return d.DirectPath }
func (d *MediaDownloader) GetURL() string                    { return d.URL }
func (d *MediaDownloader) GetMediaKey() []byte               { return d.MediaKey }
func (d *MediaDownloader) GetFileLength() uint64             { return d.FileLength }
func (d *MediaDownloader) GetFileSHA256() []byte             { return d.FileSHA256 }
func (d *MediaDownloader) GetFileEncSHA256() []byte          { return d.FileEncSHA256 }
func (d *MediaDownloader) GetMediaType() whatsmeow.MediaType { return d.MediaType }

// downloadMedia downloads media for a message and writes it to /tmp.
// (Container is ephemeral; downloads are short-lived. For persistent media, change to S3 or similar.)
func downloadMedia(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (bool, string, string, string, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	var err error

	chatDir := fmt.Sprintf("/tmp/whatsapp-media/%s", strings.ReplaceAll(chatJID, ":", "_"))
	localPath := ""

	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err = messageStore.GetMediaInfo(messageID, chatJID)
	if err != nil {
		err = messageStore.db.QueryRow(
			`SELECT media_type, filename FROM messages WHERE id = $1 AND chat_jid = $2`,
			messageID, chatJID,
		).Scan(&mediaType, &filename)
		if err != nil {
			return false, "", "", "", fmt.Errorf("failed to find message: %v", err)
		}
	}
	if mediaType == "" {
		return false, "", "", "", fmt.Errorf("not a media message")
	}
	if err := os.MkdirAll(chatDir, 0o755); err != nil {
		return false, "", "", "", fmt.Errorf("failed to create chat directory: %v", err)
	}
	localPath = fmt.Sprintf("%s/%s", chatDir, filename)
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to get absolute path: %v", err)
	}
	if _, err := os.Stat(localPath); err == nil {
		return true, mediaType, filename, absPath, nil
	}
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return false, "", "", "", fmt.Errorf("incomplete media information for download")
	}
	directPath := extractDirectPathFromURL(url)
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}
	downloader := &MediaDownloader{
		URL: url, DirectPath: directPath, MediaKey: mediaKey,
		FileLength: fileLength, FileSHA256: fileSHA256, FileEncSHA256: fileEncSHA256,
		MediaType: waMediaType,
	}
	mediaData, err := client.Download(context.Background(), downloader)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}
	if err := os.WriteFile(localPath, mediaData, 0o644); err != nil {
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}
	return true, mediaType, filename, absPath, nil
}

// fetchMediaBytes fetches + decrypts media for a message and returns it as raw bytes.
// Streams cleanly through the HTTP handler — no filesystem intermediate.
func fetchMediaBytes(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (mediaType, filename string, data []byte, err error) {
	mt, fn, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, metaErr := messageStore.GetMediaInfo(messageID, chatJID)
	if metaErr != nil {
		return "", "", nil, fmt.Errorf("message not found: %w", metaErr)
	}
	if mt == "" {
		return "", "", nil, fmt.Errorf("message is not a media message")
	}
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return "", "", nil, fmt.Errorf("incomplete media metadata; cannot decrypt")
	}
	var wt whatsmeow.MediaType
	switch mt {
	case "image":
		wt = whatsmeow.MediaImage
	case "video":
		wt = whatsmeow.MediaVideo
	case "audio":
		wt = whatsmeow.MediaAudio
	case "document":
		wt = whatsmeow.MediaDocument
	default:
		return "", "", nil, fmt.Errorf("unsupported media type: %s", mt)
	}
	downloader := &MediaDownloader{
		URL: url, DirectPath: extractDirectPathFromURL(url), MediaKey: mediaKey,
		FileLength: fileLength, FileSHA256: fileSHA256, FileEncSHA256: fileEncSHA256,
		MediaType: wt,
	}
	bytes, dlErr := client.Download(context.Background(), downloader)
	if dlErr != nil {
		return "", "", nil, fmt.Errorf("decrypt/download failed: %w", dlErr)
	}
	return mt, fn, bytes, nil
}

// mimeForMediaType maps whatsmeow media types to reasonable Content-Type defaults.
// Filename extension wins when available — this is just a fallback.
func mimeForMediaType(mediaType, filename string) string {
	lower := strings.ToLower(filename)
	switch {
	case strings.HasSuffix(lower, ".pdf"):
		return "application/pdf"
	case strings.HasSuffix(lower, ".jpg"), strings.HasSuffix(lower, ".jpeg"):
		return "image/jpeg"
	case strings.HasSuffix(lower, ".png"):
		return "image/png"
	case strings.HasSuffix(lower, ".mp4"):
		return "video/mp4"
	case strings.HasSuffix(lower, ".ogg"):
		return "audio/ogg"
	}
	switch mediaType {
	case "image":
		return "image/jpeg"
	case "video":
		return "video/mp4"
	case "audio":
		return "audio/ogg"
	default:
		return "application/octet-stream"
	}
}

// respondStatusResult writes a uniform JSON response for /api/status.
func respondStatusResult(w http.ResponseWriter, err error, okMsg string) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "message": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "message": okMsg})
}

// Extract direct path from a WhatsApp media URL
func extractDirectPathFromURL(url string) string {
	parts := strings.SplitN(url, ".net/", 2)
	if len(parts) < 2 {
		return url
	}
	pathPart := parts[1]
	pathPart = strings.SplitN(pathPart, "?", 2)[0]
	return "/" + pathPart
}

// startRESTServer exposes /api/send, /api/download, /api/health.
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, port int) {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, _ *http.Request) {
		status := map[string]any{
			"status":    "ok",
			"connected": client.IsConnected(),
			"logged_in": client.IsLoggedIn(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})

	mux.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}
		if req.Message == "" && req.MediaPath == "" {
			http.Error(w, "Message or media path is required", http.StatusBadRequest)
			return
		}
		success, message := sendWhatsAppMessage(client, req.Recipient, req.Message, req.MediaPath)
		w.Header().Set("Content-Type", "application/json")
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(SendMessageResponse{Success: success, Message: message})
	})

	// /api/media?message_id=...&chat_jid=...
	// Streams decrypted media bytes back. Replaces /api/download for callers
	// that want the file content inline (e.g. backend admin proxy).
	mux.HandleFunc("/api/media", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		msgID := r.URL.Query().Get("message_id")
		chatJID := r.URL.Query().Get("chat_jid")
		if msgID == "" || chatJID == "" {
			http.Error(w, "message_id and chat_jid required", http.StatusBadRequest)
			return
		}
		mediaType, filename, data, err := fetchMediaBytes(client, messageStore, msgID, chatJID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", mimeForMediaType(mediaType, filename))
		w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, filename))
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	})

	// /api/status — post to the user's WhatsApp status (24h stories).
	// Body:
	//   Text-only:      {"message": "text"}
	//   Image status:   {"media_type": "image", "media_base64": "...", "message": "caption (optional)"}
	//   Video status:   {"media_type": "video", "media_base64": "...", "message": "caption (optional)"}
	//
	// Body size limit: 20 MB (post-base64). WhatsApp limits status videos to ~30s / ~16 MB.
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Message     string `json:"message"`
			MediaType   string `json:"media_type"`   // "image" | "video" | ""
			MediaBase64 string `json:"media_base64"` // raw bytes, base64-encoded
			MimeType    string `json:"mime_type"`    // optional override (e.g. "video/mp4")
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 25<<20)).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON (or body > 25MB)", http.StatusBadRequest)
			return
		}
		if req.MediaBase64 == "" && strings.TrimSpace(req.Message) == "" {
			http.Error(w, "message or media_base64 required", http.StatusBadRequest)
			return
		}
		if !client.IsConnected() {
			http.Error(w, "Not connected to WhatsApp", http.StatusServiceUnavailable)
			return
		}

		// Text-only path
		if req.MediaBase64 == "" {
			msg := &waProto.Message{Conversation: proto.String(req.Message)}
			_, err := client.SendMessage(context.Background(), types.StatusBroadcastJID, msg)
			respondStatusResult(w, err, "status posted (text)")
			return
		}

		// Media path — decode, upload to WA CDN, wrap in Image/Video message.
		mediaBytes, err := base64.StdEncoding.DecodeString(req.MediaBase64)
		if err != nil {
			http.Error(w, "media_base64 is not valid base64", http.StatusBadRequest)
			return
		}
		mediaType := strings.ToLower(strings.TrimSpace(req.MediaType))
		if mediaType != "image" && mediaType != "video" {
			http.Error(w, `media_type must be "image" or "video"`, http.StatusBadRequest)
			return
		}
		mime := strings.TrimSpace(req.MimeType)
		if mime == "" {
			if mediaType == "image" {
				mime = "image/jpeg"
			} else {
				mime = "video/mp4"
			}
		}
		var waType whatsmeow.MediaType
		if mediaType == "image" {
			waType = whatsmeow.MediaImage
		} else {
			waType = whatsmeow.MediaVideo
		}
		up, err := client.Upload(context.Background(), mediaBytes, waType)
		if err != nil {
			respondStatusResult(w, fmt.Errorf("upload: %w", err), "")
			return
		}
		msg := &waProto.Message{}
		switch mediaType {
		case "image":
			msg.ImageMessage = &waProto.ImageMessage{
				Caption: proto.String(req.Message), Mimetype: proto.String(mime),
				URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
				FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: &up.FileLength,
			}
		case "video":
			msg.VideoMessage = &waProto.VideoMessage{
				Caption: proto.String(req.Message), Mimetype: proto.String(mime),
				URL: &up.URL, DirectPath: &up.DirectPath, MediaKey: up.MediaKey,
				FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: &up.FileLength,
			}
		}
		_, err = client.SendMessage(context.Background(), types.StatusBroadcastJID, msg)
		respondStatusResult(w, err, fmt.Sprintf("status posted (%s, %d bytes)", mediaType, len(mediaBytes)))
	})

	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req DownloadMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}
		success, mediaType, filename, path, err := downloadMedia(client, messageStore, req.MessageID, req.ChatJID)
		w.Header().Set("Content-Type", "application/json")
		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(DownloadMediaResponse{Success: false, Message: fmt.Sprintf("Failed to download media: %s", errMsg)})
			return
		}
		_ = json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success: true, Message: fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename, Path: path,
		})
	})

	serverAddr := fmt.Sprintf(":%d", port)
	fmt.Printf("REST API server listening on %s\n", serverAddr)
	go func() {
		if err := http.ListenAndServe(serverAddr, mux); err != nil {
			fmt.Printf("REST API server error: %v\n", err)
		}
	}()
}

func main() {
	logger := waLog.Stdout("Client", "INFO", true)
	dbLog := waLog.Stdout("Database", "INFO", true)

	// Required: Postgres DSN with search_path scoping the schema for THIS bridge.
	// Example: postgres://user:pass@host:5432/db?search_path=whatsapp_vita&sslmode=disable
	dsn := requireEnv("DATABASE_URL")

	// Account label (e.g. "vita", "cm", "hk") — included in webhook payloads so the receiver knows which bridge sent it.
	account := envOrDefault("ACCOUNT", "default")

	// Optional webhook for inbound messages.
	webhookURL := os.Getenv("WEBHOOK_URL")
	webhookSecret := os.Getenv("WEBHOOK_SECRET")

	// Optional: if set, pair via 8-char pairing code instead of QR.
	// Set to the phone number in international format (digits only, no +), e.g. "237659099178".
	// More reliable than QR because it's immune to terminal/log unicode corruption.
	pairPhone := sanitizePhone(os.Getenv("PAIR_PHONE"))

	port, err := strconv.Atoi(envOrDefault("PORT", "8080"))
	if err != nil {
		logger.Errorf("invalid PORT: %v", err)
		return
	}

	logger.Infof("Starting WhatsApp bridge: account=%s port=%d webhook=%v", account, port, webhookURL != "")

	// whatsmeow session/state lives in the same Postgres schema (whatsmeow_* tables).
	container, err := sqlstore.New(context.Background(), "postgres", dsn, dbLog)
	if err != nil {
		logger.Errorf("Failed to connect whatsmeow store: %v", err)
		return
	}

	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			return
		}
	}

	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		return
	}

	messageStore, err := NewMessageStore(dsn)
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		return
	}
	defer messageStore.Close()

	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			handleMessage(client, messageStore, v, logger, account, webhookURL, webhookSecret)
		case *events.HistorySync:
			handleHistorySync(client, messageStore, v, logger)
		case *events.Connected:
			logger.Infof("✓ Connected to WhatsApp")
		case *events.PairSuccess:
			logger.Infof("✓ Paired successfully (device: %s)", v.ID.String())
		case *events.LoggedOut:
			logger.Warnf("Device logged out (reason: %v). Restart bridge or wait for auto-reconnect.", v.Reason)
		case *events.CallOffer:
			// 1:1 incoming call. Media type not in BasicCallMeta; leave blank (filled
			// by CallOfferNotice when that also fires for same call_id).
			if err := messageStore.RecordCallOffer(
				v.CallID, v.From.String(), v.CallCreator.String(),
				"", v.RemotePlatform, v.RemoteVersion, false, v.Timestamp,
			); err != nil {
				logger.Warnf("RecordCallOffer: %v", err)
			}
			fmt.Printf("[%s] ☎  call offer from %s (id=%s)\n", v.Timestamp.Format("2006-01-02 15:04:05"), v.From.User, v.CallID)
		case *events.CallOfferNotice:
			// Fires for group calls or when the device was offline at the time
			// of a 1:1 offer. Carries Media ("audio"|"video") and Type ("group").
			isGroup := v.Type == "group"
			if err := messageStore.RecordCallOffer(
				v.CallID, v.From.String(), v.CallCreator.String(),
				v.Media, "", "", isGroup, v.Timestamp,
			); err != nil {
				logger.Warnf("RecordCallOffer (notice): %v", err)
			}
			fmt.Printf("[%s] ☎  call notice (%s%s) from %s (id=%s)\n",
				v.Timestamp.Format("2006-01-02 15:04:05"), v.Media, map[bool]string{true: ", group", false: ""}[isGroup], v.From.User, v.CallID)
		case *events.CallAccept:
			if err := messageStore.RecordCallAccept(v.CallID, v.Timestamp); err != nil {
				logger.Warnf("RecordCallAccept: %v", err)
			}
		case *events.CallReject:
			if err := messageStore.RecordCallReject(v.CallID, v.Timestamp); err != nil {
				logger.Warnf("RecordCallReject: %v", err)
			}
		case *events.CallTerminate:
			if err := messageStore.RecordCallTerminate(v.CallID, v.Reason, v.Timestamp); err != nil {
				logger.Warnf("RecordCallTerminate: %v", err)
			}
			fmt.Printf("[%s] ☎  call terminated (id=%s reason=%s)\n", v.Timestamp.Format("2006-01-02 15:04:05"), v.CallID, v.Reason)
		}
	})

	// Start REST server up-front so /api/health responds during pairing.
	startRESTServer(client, messageStore, port)

	// whatsmeow has a strict rule: GetQRChannel must be called BEFORE Connect.
	// So we split three code paths:
	//   1. Already paired (Store.ID != nil) → Connect only.
	//   2. Unpaired + PAIR_PHONE set          → Connect then pairingCodeLoop.
	//   3. Unpaired + no PAIR_PHONE            → qrPairingLoop owns Connect lifecycle.
	if client.Store.ID != nil {
		logger.Infof("Existing session found (device: %s) — reconnecting", client.Store.ID.String())
		if err := client.Connect(); err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}
	} else if pairPhone != "" {
		if err := client.Connect(); err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}
		go pairingCodeLoop(client, pairPhone, logger)
	} else {
		go qrPairingLoop(client, logger)
	}

	fmt.Println("Bridge is running. Ctrl+C to exit.")
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)
	<-exitChan

	fmt.Println("Disconnecting...")
	client.Disconnect()
}

// sanitizePhone keeps only digits (strips +, spaces, dashes).
func sanitizePhone(raw string) string {
	var out []rune
	for _, r := range raw {
		if r >= '0' && r <= '9' {
			out = append(out, r)
		}
	}
	return string(out)
}

// pairingCodeLoop requests a fresh pairing code until paired.
//
// Backoff policy matters: WhatsApp rate-limits PairPhone aggressively (429
// rate-overlimit after ~3 consecutive calls). On any error, we use
// exponential backoff capped at 15 minutes. On success we sleep 55s (code
// rotates every ~60s).
func pairingCodeLoop(client *whatsmeow.Client, phone string, logger waLog.Logger) {
	errorBackoff := 60 * time.Second
	const maxBackoff = 15 * time.Minute
	for {
		if client.IsLoggedIn() {
			return
		}
		code, err := client.PairPhone(context.Background(), phone, true, whatsmeow.PairClientChrome, "Vita Resort Bridge")
		if err != nil {
			logger.Warnf("PairPhone failed: %v — retrying in %v", err, errorBackoff)
			time.Sleep(errorBackoff)
			errorBackoff *= 2
			if errorBackoff > maxBackoff {
				errorBackoff = maxBackoff
			}
			continue
		}
		errorBackoff = 60 * time.Second // reset on success
		fmt.Println("")
		fmt.Println("╔══════════════════════════════════════════════════════╗")
		fmt.Printf("║  PAIRING CODE: %-36s  ║\n", code)
		fmt.Println("╠══════════════════════════════════════════════════════╣")
		fmt.Println("║  On your phone:                                      ║")
		fmt.Println("║    WhatsApp → Settings → Linked Devices              ║")
		fmt.Println("║      → Link a Device                                 ║")
		fmt.Println("║      → Link with phone number instead                ║")
		fmt.Println("║      → enter the code above                          ║")
		fmt.Println("╚══════════════════════════════════════════════════════╝")
		fmt.Println("")
		// Code lasts ~60s. Sleep 55s then regenerate if still unpaired.
		time.Sleep(55 * time.Second)
	}
}

// qrPairingLoop owns the full connect/pair lifecycle for QR-based pairing.
//
// whatsmeow requires GetQRChannel to be called BEFORE Connect, and a channel
// only produces codes for ~3 minutes total before closing. To offer unlimited
// scan attempts, we repeat: (GetQRChannel → Connect → drain → Disconnect).
//
// Exits when IsLoggedIn is true (paired successfully). The main goroutine
// stays blocked on the signal channel, so the container lives indefinitely.
func qrPairingLoop(client *whatsmeow.Client, logger waLog.Logger) {
	for {
		if client.IsLoggedIn() {
			return
		}
		qrChan, err := client.GetQRChannel(context.Background())
		if err != nil {
			logger.Warnf("GetQRChannel failed: %v — retrying in 15s", err)
			time.Sleep(15 * time.Second)
			continue
		}
		if err := client.Connect(); err != nil {
			logger.Warnf("Connect failed: %v — retrying in 15s", err)
			time.Sleep(15 * time.Second)
			continue
		}
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				fmt.Println("")
				fmt.Println("=========== SCAN THIS QR CODE WITH YOUR WHATSAPP APP ===========")
				// Half-block rendering: ~half the height/width of Generate().
				// More compact but requires a terminal that renders ▀▄█ at 1:1 cell width.
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				fmt.Println("================================================================")
				fmt.Println("If the QR above looks corrupted in your terminal, paste the")
				fmt.Println("text below into https://qrcode.show or any QR generator:")
				fmt.Println("")
				fmt.Println(evt.Code)
				fmt.Println("")
			case "success":
				return
			}
		}
		// Channel closed (codes exhausted). Disconnect before re-requesting a
		// fresh channel; whatsmeow requires the client to be in the right
		// state for GetQRChannel to work again.
		client.Disconnect()
		time.Sleep(5 * time.Second)
	}
}

// GetChatName determines the appropriate name for a chat based on JID and other info
func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	var existingName string
	err := messageStore.db.QueryRow(`SELECT name FROM chats WHERE jid = $1`, chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		return existingName
	}

	var name string
	if jid.Server == "g.us" {
		if conversation != nil {
			var displayName, convName *string
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}
		if name == "" {
			groupInfo, err := client.GetGroupInfo(context.Background(), jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}
	} else {
		contact, err := client.Store.Contacts.GetContact(context.Background(), jid)
		if err == nil && contact.FullName != "" {
			name = contact.FullName
		} else if sender != "" {
			name = sender
		} else {
			name = jid.User
		}
	}
	return name
}

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	fmt.Printf("Received history sync event with %d conversations\n", len(historySync.Data.Conversations))

	syncedCount := 0
	for _, conversation := range historySync.Data.Conversations {
		if conversation.ID == nil {
			continue
		}
		chatJID := *conversation.ID
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			continue
		}
		name := GetChatName(client, messageStore, jid, chatJID, conversation, "", logger)
		messages := conversation.Messages
		if len(messages) == 0 {
			continue
		}
		latestMsg := messages[0]
		if latestMsg == nil || latestMsg.Message == nil {
			continue
		}
		timestamp := time.Time{}
		if ts := latestMsg.Message.GetMessageTimestamp(); ts != 0 {
			timestamp = time.Unix(int64(ts), 0)
		} else {
			continue
		}
		messageStore.StoreChat(chatJID, name, timestamp)

		for _, msg := range messages {
			if msg == nil || msg.Message == nil {
				continue
			}
			var content string
			if msg.Message.Message != nil {
				if conv := msg.Message.Message.GetConversation(); conv != "" {
					content = conv
				} else if ext := msg.Message.Message.GetExtendedTextMessage(); ext != nil {
					content = ext.GetText()
				}
			}
			var mediaType, filename, url string
			var mediaKey, fileSHA256, fileEncSHA256 []byte
			var fileLength uint64
			if msg.Message.Message != nil {
				mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
			}
			if content == "" && mediaType == "" {
				continue
			}
			var sender string
			isFromMe := false
			if msg.Message.Key != nil {
				if msg.Message.Key.FromMe != nil {
					isFromMe = *msg.Message.Key.FromMe
				}
				if !isFromMe && msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "" {
					sender = *msg.Message.Key.Participant
				} else if isFromMe {
					sender = client.Store.ID.User
				} else {
					sender = jid.User
				}
			} else {
				sender = jid.User
			}
			msgID := ""
			if msg.Message.Key != nil && msg.Message.Key.ID != nil {
				msgID = *msg.Message.Key.ID
			}
			ts := time.Time{}
			if t := msg.Message.GetMessageTimestamp(); t != 0 {
				ts = time.Unix(int64(t), 0)
			} else {
				continue
			}
			if err := messageStore.StoreMessage(msgID, chatJID, sender, content, ts, isFromMe,
				mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength); err == nil {
				syncedCount++
			}
		}
	}
	fmt.Printf("History sync complete. Stored %d messages.\n", syncedCount)
}

// analyzeOggOpus tries to extract duration and generate a simple waveform from an Ogg Opus file
func analyzeOggOpus(data []byte) (duration uint32, waveform []byte, err error) {
	if len(data) < 4 || string(data[0:4]) != "OggS" {
		return 0, nil, fmt.Errorf("not a valid Ogg file (missing OggS signature)")
	}
	var lastGranule uint64
	var sampleRate uint32 = 48000
	var preSkip uint16
	var foundOpusHead bool

	for i := 0; i < len(data); {
		if i+27 >= len(data) {
			break
		}
		if string(data[i:i+4]) != "OggS" {
			i++
			continue
		}
		granulePos := binary.LittleEndian.Uint64(data[i+6 : i+14])
		pageSeqNum := binary.LittleEndian.Uint32(data[i+18 : i+22])
		numSegments := int(data[i+26])
		if i+27+numSegments >= len(data) {
			break
		}
		segmentTable := data[i+27 : i+27+numSegments]
		pageSize := 27 + numSegments
		for _, segLen := range segmentTable {
			pageSize += int(segLen)
		}
		if !foundOpusHead && pageSeqNum <= 1 {
			pageData := data[i : i+pageSize]
			headPos := bytes.Index(pageData, []byte("OpusHead"))
			if headPos >= 0 && headPos+12 < len(pageData) {
				headPos += 8
				if headPos+12 <= len(pageData) {
					preSkip = binary.LittleEndian.Uint16(pageData[headPos+10 : headPos+12])
					sampleRate = binary.LittleEndian.Uint32(pageData[headPos+12 : headPos+16])
					foundOpusHead = true
				}
			}
		}
		if granulePos != 0 {
			lastGranule = granulePos
		}
		i += pageSize
	}
	if lastGranule > 0 {
		durationSeconds := float64(lastGranule-uint64(preSkip)) / float64(sampleRate)
		duration = uint32(math.Ceil(durationSeconds))
	} else {
		duration = uint32(float64(len(data)) / 2000.0)
	}
	if duration < 1 {
		duration = 1
	} else if duration > 300 {
		duration = 300
	}
	waveform = placeholderWaveform(duration)
	return duration, waveform, nil
}

func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// placeholderWaveform generates a synthetic waveform for WhatsApp voice messages
func placeholderWaveform(duration uint32) []byte {
	const waveformLength = 64
	waveform := make([]byte, waveformLength)
	rand.Seed(int64(duration))
	baseAmplitude := 35.0
	frequencyFactor := float64(minInt(int(duration), 120)) / 30.0
	for i := range waveform {
		pos := float64(i) / float64(waveformLength)
		val := baseAmplitude * math.Sin(pos*math.Pi*frequencyFactor*8)
		val += (baseAmplitude / 2) * math.Sin(pos*math.Pi*frequencyFactor*16)
		val += (rand.Float64() - 0.5) * 15
		fadeInOut := math.Sin(pos * math.Pi)
		val = val * (0.7 + 0.3*fadeInOut)
		val = val + 50
		if val < 0 {
			val = 0
		} else if val > 100 {
			val = 100
		}
		waveform[i] = byte(val)
	}
	return waveform
}
