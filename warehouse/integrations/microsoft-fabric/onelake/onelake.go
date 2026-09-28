package onelake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric/auth"
)

const uploadChunkSize = 4 * 1024 * 1024

type tokenProvider interface {
	Bootstrap(context.Context) error
	StorageToken(context.Context) (string, error)
}

type Manager struct {
	baseURL    *url.URL
	prefix     string
	credential tokenProvider
	httpClient *http.Client
	timeout    time.Duration
}

var _ filemanager.FileManager = (*Manager)(nil)

func New(config map[string]any) (*Manager, error) {
	httpClient := &http.Client{}
	credential, err := auth.New(stringConfig(config, "tenantId"), stringConfig(config, "clientId"), stringConfig(config, "clientSecret"), httpClient)
	if err != nil {
		return nil, err
	}
	return newManager(config, credential, httpClient)
}

func newManager(config map[string]any, credential tokenProvider, httpClient *http.Client) (*Manager, error) {
	host := strings.TrimSpace(stringConfig(config, "host"))
	if host == "" {
		return nil, errors.New("host is required")
	}
	if !strings.Contains(host, "://") {
		host = "https://" + host
	}
	baseURL, err := url.Parse(host)
	if err != nil || baseURL.Host == "" {
		return nil, errors.New("host must be a valid host or URL")
	}
	workspaceID := stringConfig(config, "fabricWorkspaceId")
	lakehouseID := stringConfig(config, "lakehouseId")
	if _, err := uuid.Parse(workspaceID); err != nil {
		return nil, errors.New("fabricWorkspaceId must be a GUID")
	}
	if _, err := uuid.Parse(lakehouseID); err != nil {
		return nil, errors.New("lakehouseId must be a GUID")
	}
	baseURL.RawQuery = ""
	baseURL.Fragment = ""
	return &Manager{
		baseURL:    baseURL,
		prefix:     path.Join(workspaceID, lakehouseID+".Lakehouse", "Files"),
		credential: credential,
		httpClient: httpClient,
		timeout:    120 * time.Second,
	}, nil
}

func stringConfig(config map[string]any, key string) string {
	value, _ := config[key].(string)
	return value
}

func (m *Manager) Prefix() string { return m.prefix }

func (m *Manager) SetTimeout(timeout time.Duration) {
	m.timeout = timeout
}

func (m *Manager) Upload(ctx context.Context, file *os.File, prefixes ...string) (filemanager.UploadedFile, error) {
	objectName := path.Join(path.Join(prefixes...), path.Base(file.Name()))
	return m.UploadReader(ctx, objectName, file)
}

func (m *Manager) UploadReader(ctx context.Context, objectName string, reader io.Reader) (filemanager.UploadedFile, error) {
	objectName, err := cleanObjectName(objectName)
	if err != nil {
		return filemanager.UploadedFile{}, err
	}
	if err := m.credential.Bootstrap(ctx); err != nil {
		return filemanager.UploadedFile{}, fmt.Errorf("bootstrapping Fabric service principal: %w", err)
	}
	objectURL := m.objectURL(objectName)
	if err := m.request(ctx, http.MethodPut, objectURL+"?resource=file", nil, nil); err != nil {
		return filemanager.UploadedFile{}, fmt.Errorf("creating OneLake file: %w", err)
	}

	buffer := make([]byte, uploadChunkSize)
	var offset int64
	for {
		read, readErr := io.ReadFull(reader, buffer)
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) && !errors.Is(readErr, io.EOF) {
			return filemanager.UploadedFile{}, fmt.Errorf("reading OneLake upload: %w", readErr)
		}
		if read > 0 {
			appendURL := objectURL + "?action=append&position=" + strconv.FormatInt(offset, 10)
			if err := m.request(ctx, http.MethodPatch, appendURL, bytes.NewReader(buffer[:read]), map[string]string{"Content-Type": "application/octet-stream"}); err != nil {
				return filemanager.UploadedFile{}, fmt.Errorf("appending OneLake file: %w", err)
			}
			offset += int64(read)
		}
		if errors.Is(readErr, io.ErrUnexpectedEOF) || errors.Is(readErr, io.EOF) {
			break
		}
	}
	if err := m.request(ctx, http.MethodPatch, objectURL+"?action=flush&position="+strconv.FormatInt(offset, 10), nil, nil); err != nil {
		return filemanager.UploadedFile{}, fmt.Errorf("flushing OneLake file: %w", err)
	}
	return filemanager.UploadedFile{Location: objectURL, ObjectName: objectName}, nil
}

func (m *Manager) Download(ctx context.Context, output io.WriterAt, key string, _ ...filemanager.DownloadOption) error {
	objectName := m.GetDownloadKeyFromFileLocation(key)
	resp, err := m.do(ctx, http.MethodGet, m.objectURL(objectName), nil, nil)
	if err != nil {
		return fmt.Errorf("downloading OneLake file: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	buffer := make([]byte, 128*1024)
	var offset int64
	for {
		read, readErr := resp.Body.Read(buffer)
		if read > 0 {
			written, writeErr := output.WriteAt(buffer[:read], offset)
			if writeErr != nil {
				return writeErr
			}
			if written != read {
				return io.ErrShortWrite
			}
			offset += int64(written)
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (m *Manager) Delete(ctx context.Context, keys []string) error {
	for _, key := range keys {
		objectName := m.GetDownloadKeyFromFileLocation(key)
		if err := m.request(ctx, http.MethodDelete, m.objectURL(objectName), nil, nil); err != nil {
			return fmt.Errorf("deleting OneLake file: %w", err)
		}
	}
	return nil
}

func (m *Manager) GetObjectNameFromLocation(location string) (string, error) {
	parsed, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("parsing OneLake location: %w", err)
	}
	prefix := "/" + strings.Trim(m.prefix, "/") + "/"
	if parsed.Host != m.baseURL.Host || !strings.HasPrefix(parsed.EscapedPath(), prefix) && !strings.HasPrefix(parsed.Path, prefix) {
		return "", errors.New("location is outside the configured OneLake path")
	}
	return strings.TrimPrefix(parsed.Path, prefix), nil
}

func (m *Manager) GetDownloadKeyFromFileLocation(location string) string {
	if objectName, err := m.GetObjectNameFromLocation(location); err == nil {
		return objectName
	}
	return strings.TrimPrefix(location, "/")
}

func (m *Manager) objectURL(objectName string) string {
	copyURL := *m.baseURL
	copyURL.Path = path.Join(copyURL.Path, m.prefix, objectName)
	return copyURL.String()
}

func cleanObjectName(objectName string) (string, error) {
	cleaned := path.Clean(strings.TrimSpace(objectName))
	if cleaned == "." || cleaned == "" || strings.HasPrefix(cleaned, "../") || path.IsAbs(cleaned) {
		return "", errors.New("invalid OneLake object name")
	}
	return cleaned, nil
}

func (m *Manager) request(ctx context.Context, method, requestURL string, body io.Reader, headers map[string]string) error {
	resp, err := m.do(ctx, method, requestURL, body, headers)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func (m *Manager) do(ctx context.Context, method, requestURL string, body io.Reader, headers map[string]string) (*http.Response, error) {
	if err := m.credential.Bootstrap(ctx); err != nil {
		return nil, fmt.Errorf("bootstrapping Fabric service principal: %w", err)
	}
	token, err := m.credential.StorageToken(ctx)
	if err != nil {
		return nil, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, m.timeout)
	req, err := http.NewRequestWithContext(requestCtx, method, requestURL, body)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("x-ms-version", "2023-11-03")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_ = resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("OneLake returned HTTP %d for %s", resp.StatusCode, strings.ToLower(method))
	}
	resp.Body = &cancelReadCloser{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

type listResponse struct {
	Paths []struct {
		Name         string `json:"name"`
		LastModified string `json:"lastModified"`
	} `json:"paths"`
	Continuation string `json:"continuation"`
}

type listSession struct {
	manager      *Manager
	ctx          context.Context
	prefix       string
	startAfter   string
	maxItems     int64
	continuation string
	done         bool
}

func (m *Manager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) filemanager.ListSession {
	return &listSession{manager: m, ctx: ctx, prefix: prefix, startAfter: startAfter, maxItems: maxItems}
}

func (s *listSession) Next() ([]*filemanager.FileInfo, error) {
	if s.done {
		return nil, nil
	}
	query := url.Values{}
	query.Set("resource", "filesystem")
	query.Set("recursive", "true")
	query.Set("directory", path.Join(strings.TrimPrefix(s.manager.prefix, strings.Split(s.manager.prefix, "/")[0]+"/"), s.prefix))
	if s.maxItems > 0 {
		query.Set("maxResults", strconv.FormatInt(s.maxItems, 10))
	}
	if s.continuation != "" {
		query.Set("continuation", s.continuation)
	}
	filesystemURL := *s.manager.baseURL
	workspace, _, _ := strings.Cut(s.manager.prefix, "/")
	filesystemURL.Path = path.Join(filesystemURL.Path, workspace)
	filesystemURL.RawQuery = query.Encode()
	resp, err := s.manager.do(s.ctx, http.MethodGet, filesystemURL.String(), nil, nil)
	if err != nil {
		return nil, fmt.Errorf("listing OneLake files: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var listing listResponse
	if err := jsonrs.NewDecoder(resp.Body).Decode(&listing); err != nil {
		return nil, fmt.Errorf("decoding OneLake listing: %w", err)
	}
	s.continuation = resp.Header.Get("x-ms-continuation")
	if s.continuation == "" {
		s.continuation = listing.Continuation
	}
	s.done = s.continuation == ""
	files := make([]*filemanager.FileInfo, 0, len(listing.Paths))
	basePrefix := strings.TrimPrefix(s.manager.prefix, workspace+"/") + "/"
	for _, item := range listing.Paths {
		name := strings.TrimPrefix(item.Name, basePrefix)
		if name <= s.startAfter {
			continue
		}
		modified, _ := time.Parse(time.RFC1123, item.LastModified)
		files = append(files, &filemanager.FileInfo{Key: name, LastModified: modified})
	}
	return files, nil
}
