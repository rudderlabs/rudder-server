package filemanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/iterator"

	"github.com/rudderlabs/rudder-go-kit/googleutil"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type GCSConfig struct {
	Bucket         string
	Prefix         string
	Credentials    string
	EndPoint       *string
	ForcePathStyle *bool
	DisableSSL     *bool
	JSONReads      bool
}

// NewGCSManager creates a new file manager for Google Cloud Storage
func NewGCSManager(
	config map[string]interface{}, log logger.Logger, defaultTimeout func() time.Duration,
) (*gcsManager, error) {
	return &gcsManager{
		baseManager: &baseManager{
			logger:         log,
			defaultTimeout: defaultTimeout,
		},
		config: gcsConfig(config),
	}, nil
}

func (m *gcsManager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) ListSession {
	return &gcsListSession{
		baseListSession: &baseListSession{
			ctx:        ctx,
			startAfter: startAfter,
			prefix:     prefix,
			maxItems:   maxItems,
		},
		manager: m,
	}
}

func (m *gcsManager) Download(ctx context.Context, output *os.File, key string) error {
	client, err := m.getClient(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, m.getTimeout())
	defer cancel()

	rc, err := client.Bucket(m.config.Bucket).Object(key).NewReader(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	_, err = io.Copy(output, rc)
	return err
}

func (m *gcsManager) Upload(ctx context.Context, file *os.File, prefixes ...string) (UploadedFile, error) {
	fileName := path.Join(m.config.Prefix, path.Join(prefixes...), path.Base(file.Name()))

	client, err := m.getClient(ctx)
	if err != nil {
		return UploadedFile{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, m.getTimeout())
	defer cancel()

	obj := client.Bucket(m.config.Bucket).Object(fileName)
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, file); err != nil {
		err = fmt.Errorf("copying file to GCS: %v", err)
		if closeErr := w.Close(); closeErr != nil {
			return UploadedFile{}, fmt.Errorf("closing writer: %q, while: %w", closeErr, err)
		}

		return UploadedFile{}, err
	}

	if err := w.Close(); err != nil {
		return UploadedFile{}, fmt.Errorf("closing writer: %w", err)
	}
	attrs := w.Attrs()

	return UploadedFile{Location: m.objectURL(attrs), ObjectName: fileName}, err
}

func (m *gcsManager) Delete(ctx context.Context, keys []string) (err error) {
	client, err := m.getClient(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, m.getTimeout())
	defer cancel()

	for _, key := range keys {
		if err := client.Bucket(m.config.Bucket).Object(key).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
			return err
		}
	}
	return
}

func (m *gcsManager) Prefix() string {
	return m.config.Prefix
}

func (m *gcsManager) GetObjectNameFromLocation(location string) (string, error) {
	splitStr := strings.Split(location, m.config.Bucket)
	object := strings.TrimLeft(splitStr[len(splitStr)-1], "/")
	return object, nil
}

func (m *gcsManager) GetDownloadKeyFromFileLocation(location string) string {
	splitStr := strings.Split(location, m.config.Bucket)
	key := strings.TrimLeft(splitStr[len(splitStr)-1], "/")
	return key
}

func (m *gcsManager) objectURL(objAttrs *storage.ObjectAttrs) string {
	if m.config.EndPoint != nil && *m.config.EndPoint != "" {
		endpoint := strings.TrimSuffix(*m.config.EndPoint, "/")
		return fmt.Sprintf("%s/%s/%s", endpoint, objAttrs.Bucket, objAttrs.Name)
	}
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s", objAttrs.Bucket, objAttrs.Name)
}

func (m *gcsManager) getClient(ctx context.Context) (*storage.Client, error) {
	m.clientMu.Lock()
	defer m.clientMu.Unlock()

	if m.client != nil {
		return m.client, nil
	}

	var options []option.ClientOption
	if m.config.EndPoint != nil && *m.config.EndPoint != "" {
		options = append(options, option.WithEndpoint(*m.config.EndPoint))
	}
	if !googleutil.ShouldSkipCredentialsInit(m.config.Credentials) {
		if err := googleutil.CompatibleGoogleCredentialsJSON([]byte(m.config.Credentials)); err != nil {
			return m.client, err
		}
		options = append(options, option.WithCredentialsJSON([]byte(m.config.Credentials)))
	}
	if m.config.JSONReads {
		options = append(options, storage.WithJSONReads())
	}

	ctx, cancel := context.WithTimeout(ctx, m.getTimeout())
	defer cancel()

	var err error
	m.client, err = storage.NewClient(ctx, options...)
	return m.client, err
}

type gcsManager struct {
	*baseManager
	config *GCSConfig

	client   *storage.Client
	clientMu sync.Mutex
}

func gcsConfig(config map[string]interface{}) *GCSConfig {
	var bucketName, prefix, credentials string
	var endPoint *string
	var forcePathStyle, disableSSL *bool
	var jsonReads bool

	if config["bucketName"] != nil {
		tmp, ok := config["bucketName"].(string)
		if ok {
			bucketName = tmp
		}
	}
	if config["prefix"] != nil {
		tmp, ok := config["prefix"].(string)
		if ok {
			prefix = tmp
		}
	}
	if config["credentials"] != nil {
		tmp, ok := config["credentials"].(string)
		if ok {
			credentials = tmp
		}
	}
	if config["endPoint"] != nil {
		tmp, ok := config["endPoint"].(string)
		if ok {
			endPoint = &tmp
		}
	}
	if config["forcePathStyle"] != nil {
		tmp, ok := config["forcePathStyle"].(bool)
		if ok {
			forcePathStyle = &tmp
		}
	}
	if config["disableSSL"] != nil {
		tmp, ok := config["disableSSL"].(bool)
		if ok {
			disableSSL = &tmp
		}
	}
	if config["jsonReads"] != nil {
		tmp, ok := config["jsonReads"].(bool)
		if ok {
			jsonReads = tmp
		}
	}
	return &GCSConfig{
		Bucket:         bucketName,
		Prefix:         prefix,
		Credentials:    credentials,
		EndPoint:       endPoint,
		ForcePathStyle: forcePathStyle,
		DisableSSL:     disableSSL,
		JSONReads:      jsonReads,
	}
}

type gcsListSession struct {
	*baseListSession
	manager *gcsManager

	Iterator *storage.ObjectIterator
}

func (l *gcsListSession) Next() (fileObjects []*FileInfo, err error) {
	manager := l.manager
	maxItems := l.maxItems
	fileObjects = make([]*FileInfo, 0)

	// Create GCS storage client
	client, err := manager.getClient(l.ctx)
	if err != nil {
		return
	}

	// Create GCS Bucket handle
	if l.Iterator == nil {
		l.Iterator = client.Bucket(manager.config.Bucket).Objects(l.ctx, &storage.Query{
			Prefix:      l.prefix,
			Delimiter:   "",
			StartOffset: l.startAfter,
		})
	}
	var attrs *storage.ObjectAttrs
	for {
		if maxItems <= 0 {
			break
		}
		attrs, err = l.Iterator.Next()
		if err == iterator.Done || err != nil {
			if err == iterator.Done {
				err = nil
			}
			break
		}
		fileObjects = append(fileObjects, &FileInfo{attrs.Name, attrs.Updated})
		maxItems--
	}
	return
}
