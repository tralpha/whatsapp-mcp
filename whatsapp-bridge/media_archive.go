package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var errArchiveNotFound = errors.New("archive object not found")

type mediaArchive interface {
	Get(context.Context, string) ([]byte, error)
	Put(context.Context, string, string, []byte) (string, error)
}

type archiveMetadataStore interface {
	GetArchiveKey(string, string) (string, error)
	StoreArchiveSuccess(string, string, string, string) error
	StoreArchiveError(string, string, error) error
}

func loadArchivedMedia(ctx context.Context, archive mediaArchive, store archiveMetadataStore, messageID, chatJID string) ([]byte, bool) {
	if archive == nil {
		return nil, false
	}
	key, err := store.GetArchiveKey(messageID, chatJID)
	if err != nil || key == "" {
		return nil, false
	}
	data, err := archive.Get(ctx, key)
	if err != nil {
		_ = store.StoreArchiveError(messageID, chatJID, err)
		return nil, false
	}
	return data, true
}

func storeRecoveredMedia(ctx context.Context, archive mediaArchive, store archiveMetadataStore, account, messageID, chatJID, filename, contentType string, data []byte) {
	if archive == nil {
		return
	}
	key := archiveObjectKey(account, chatJID, messageID, filename)
	etag, err := archive.Put(ctx, key, contentType, data)
	if err != nil {
		_ = store.StoreArchiveError(messageID, chatJID, err)
		return
	}
	if err := store.StoreArchiveSuccess(messageID, chatJID, key, etag); err != nil {
		_ = store.StoreArchiveError(messageID, chatJID, err)
	}
}

type r2Archive struct {
	client *s3.Client
	bucket string
}

func newMediaArchiveFromEnv(ctx context.Context) (mediaArchive, error) {
	accountID := strings.TrimSpace(os.Getenv("R2_ACCOUNT_ID"))
	bucket := strings.TrimSpace(os.Getenv("R2_BUCKET"))
	accessKey := strings.TrimSpace(os.Getenv("R2_ACCESS_KEY_ID"))
	secretKey := strings.TrimSpace(os.Getenv("R2_SECRET_ACCESS_KEY"))
	values := []string{accountID, bucket, accessKey, secretKey}
	configured := 0
	for _, value := range values {
		if value != "" {
			configured++
		}
	}
	if configured == 0 {
		return nil, nil
	}
	if configured != len(values) {
		return nil, errors.New("incomplete R2 configuration: R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, and R2_SECRET_ACCESS_KEY are all required")
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load R2 configuration: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String("https://" + accountID + ".r2.cloudflarestorage.com")
		options.UsePathStyle = true
	})
	return &r2Archive{client: client, bucket: bucket}, nil
}

func (archive *r2Archive) Get(ctx context.Context, key string) ([]byte, error) {
	output, err := archive.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(archive.bucket), Key: aws.String(key)})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
			return nil, errArchiveNotFound
		}
		return nil, fmt.Errorf("get R2 object: %w", err)
	}
	defer output.Body.Close()
	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("read R2 object: %w", err)
	}
	return data, nil
}

func (archive *r2Archive) Put(ctx context.Context, key, contentType string, data []byte) (string, error) {
	output, err := archive.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(archive.bucket), Key: aws.String(key), ContentType: aws.String(contentType), Body: bytes.NewReader(data),
	})
	if err != nil {
		return "", fmt.Errorf("put R2 object: %w", err)
	}
	return strings.Trim(aws.ToString(output.ETag), `"`), nil
}

func archiveObjectKey(account, chatJID, messageID, filename string) string {
	digest := sha256.Sum256([]byte(chatJID))
	chatHash := hex.EncodeToString(digest[:8])
	cleanName := path.Base(strings.ReplaceAll(filename, "\\", "/"))
	if cleanName == "." || cleanName == "/" || cleanName == "" {
		cleanName = "media"
	}
	return path.Join(account, chatHash, messageID, cleanName)
}
