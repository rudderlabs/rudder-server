package onelake

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	storageScope       = "https://storage.azure.com/.default"
	defaultOneLakeHost = "onelake.dfs.fabric.microsoft.com"
)

var _ filemanager.FileManager = (*Manager)(nil)

// credentials shares one Entra credential, and so one token cache, per service principal
// across the many short-lived Managers created per upload and download.
var credentials sync.Map

func sharedCredential(tenantID, clientID, clientSecret string) (azcore.TokenCredential, error) {
	secretHash := sha256.Sum256([]byte(clientSecret))
	key := tenantID + "\x00" + clientID + "\x00" + hex.EncodeToString(secretHash[:])
	if credential, ok := credentials.Load(key); ok {
		return credential.(azcore.TokenCredential), nil
	}
	credential, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
	if err != nil {
		return nil, err
	}
	actual, _ := credentials.LoadOrStore(key, credential)
	return actual.(azcore.TokenCredential), nil
}

type Config struct {
	Host              string
	FabricWorkspaceID string
	LakehouseID       string
	TenantID          string
	ClientID          string
	ClientSecret      string
	Prefix            string
}

type Manager struct {
	config     Config
	credential azcore.TokenCredential
	client     *http.Client
	logger     logger.Logger

	mu      sync.RWMutex
	timeout time.Duration
}

func New(config map[string]any, log logger.Logger) (*Manager, error) {
	oneLakeHost, err := HostFromConfig(config)
	if err != nil {
		return nil, err
	}
	cfg := Config{
		Host:              oneLakeHost,
		FabricWorkspaceID: stringConfig(config, "fabricWorkspaceId"),
		LakehouseID:       stringConfig(config, "lakehouseId"),
		TenantID:          stringConfig(config, "tenantId"),
		ClientID:          stringConfig(config, "clientId"),
		ClientSecret:      stringConfig(config, "clientSecret"),
		Prefix:            strings.Trim(stringConfig(config, "prefix"), "/"),
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	credential, err := sharedCredential(cfg.TenantID, cfg.ClientID, cfg.ClientSecret)
	if err != nil {
		return nil, fmt.Errorf("creating OneLake credential: %w", err)
	}
	return newManager(cfg, credential, http.DefaultClient, log), nil
}

func newManager(cfg Config, credential azcore.TokenCredential, client *http.Client, log logger.Logger) *Manager {
	if log == nil {
		log = logger.NewLogger()
	}
	return &Manager{
		config:     cfg,
		credential: credential,
		client:     client,
		logger:     log.Child("onelake"),
		timeout:    120 * time.Second,
	}
}

func HostFromConfig(config map[string]any) (string, error) {
	host := stringConfig(config, "oneLakeHost")
	if host == "" {
		host = stringConfig(config, "onelakeHost")
	}
	if host == "" {
		host = stringConfig(config, "oneLakeEndpoint")
	}
	if strings.TrimSpace(host) == "" {
		return defaultOneLakeHost, nil
	}
	host = strings.TrimSpace(host)
	if strings.Contains(host, "://") {
		parsed, err := url.Parse(host)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Path != "" {
			return "", errors.New("oneLakeHost must be an HTTPS OneLake hostname without a path")
		}
		host = parsed.Host
	}
	if err := validateOneLakeHost(host); err != nil {
		return "", err
	}
	return strings.ToLower(host), nil
}

func validateOneLakeHost(host string) error {
	if strings.TrimSpace(host) == "" {
		return errors.New("oneLakeHost is required for OneLake")
	}
	if strings.Contains(host, "/") {
		return errors.New("oneLakeHost must be a hostname without a path")
	}
	if strings.Contains(host, ":") {
		return errors.New("oneLakeHost must be a hostname without a port")
	}
	host = strings.Trim(strings.ToLower(host), ".")
	if net.ParseIP(host) != nil {
		return errors.New("oneLakeHost must be a Microsoft OneLake endpoint")
	}
	if host == defaultOneLakeHost || strings.HasSuffix(host, ".dfs.fabric.microsoft.com") {
		return nil
	}
	return errors.New("oneLakeHost must be a Microsoft OneLake endpoint")
}

func validateConfig(cfg Config) error {
	for name, value := range map[string]string{
		"fabricWorkspaceId": cfg.FabricWorkspaceID,
		"lakehouseId":       cfg.LakehouseID,
		"tenantId":          cfg.TenantID,
		"clientId":          cfg.ClientID,
		"clientSecret":      cfg.ClientSecret,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required for OneLake", name)
		}
	}
	if !isCanonicalGUID(cfg.FabricWorkspaceID) {
		return fmt.Errorf("fabricWorkspaceId must be a canonical GUID")
	}
	if !isCanonicalGUID(cfg.LakehouseID) {
		return fmt.Errorf("lakehouseId must be a canonical GUID")
	}
	if err := validateOneLakeHost(cfg.Host); err != nil {
		return err
	}
	return nil
}

func isCanonicalGUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value
}

func stringConfig(config map[string]any, key string) string {
	value, _ := config[key].(string)
	return value
}

func (m *Manager) baseURL() url.URL {
	return url.URL{
		Scheme: "https",
		Host:   m.config.Host,
		Path: "/" + path.Join(
			m.config.FabricWorkspaceID,
			m.config.LakehouseID,
			"Files",
		),
	}
}

func (m *Manager) objectURL(key string) url.URL {
	u := m.baseURL()
	u.Path = path.Join(u.Path, strings.TrimPrefix(key, "/"))
	return u
}

func (m *Manager) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	m.mu.RLock()
	timeout := m.timeout
	m.mu.RUnlock()
	return context.WithTimeout(ctx, timeout)
}

func (m *Manager) request(ctx context.Context, method string, u url.URL, body io.Reader, contentLength *int64, headers http.Header) (*http.Response, error) {
	token, err := m.credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{storageScope}})
	if err != nil {
		return nil, fmt.Errorf("authenticating with OneLake: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("creating OneLake request: %w", err)
	}
	if contentLength != nil {
		req.ContentLength = *contentLength
	}
	req.Header.Set("Authorization", "Bearer "+token.Token)
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling OneLake: %w", err)
	}
	return resp, nil
}

func responseError(operation string, response *http.Response) error {
	defer func() { _ = response.Body.Close() }()
	switch response.StatusCode {
	case http.StatusNotFound:
		return fmt.Errorf("lakehouse_not_found: %s failed with HTTP %d; verify workspace and Lakehouse GUIDs", operation, response.StatusCode)
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("lakehouse_access: %s failed with HTTP %d; grant Contributor on the source workspace", operation, response.StatusCode)
	default:
		return fmt.Errorf("lakehouse_access: %s failed with HTTP %d", operation, response.StatusCode)
	}
}

func (m *Manager) Upload(ctx context.Context, file *os.File, prefixes ...string) (filemanager.UploadedFile, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return filemanager.UploadedFile{}, fmt.Errorf("seeking upload file: %w", err)
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return filemanager.UploadedFile{}, fmt.Errorf("stating upload file: %w", err)
	}
	parts := append(append([]string(nil), prefixes...), path.Base(file.Name()))
	objectName, err := m.prefixedObjectName(parts...)
	if err != nil {
		return filemanager.UploadedFile{}, err
	}
	return m.upload(ctx, objectName, file, fileInfo.Size())
}

func (m *Manager) UploadReader(ctx context.Context, objectName string, reader io.Reader) (filemanager.UploadedFile, error) {
	objectName, err := m.prefixedObjectName(objectName)
	if err != nil {
		return filemanager.UploadedFile{}, err
	}
	var size int64
	if readerWithLen, ok := reader.(interface{ Len() int }); ok {
		size = int64(readerWithLen.Len())
	} else {
		data, err := io.ReadAll(reader)
		if err != nil {
			return filemanager.UploadedFile{}, fmt.Errorf("buffering OneLake upload: %w", err)
		}
		reader = bytes.NewReader(data)
		size = int64(len(data))
	}
	return m.upload(ctx, objectName, reader, size)
}

func (m *Manager) prefixedObjectName(parts ...string) (string, error) {
	validated := make([]string, 0, len(parts)+1)
	if m.config.Prefix != "" {
		prefix, err := normalizeObjectName(m.config.Prefix)
		if err != nil {
			return "", err
		}
		validated = append(validated, prefix)
	}
	for _, part := range parts {
		part, err := normalizeObjectName(part)
		if err != nil {
			return "", err
		}
		validated = append(validated, part)
	}
	return normalizeObjectName(path.Join(validated...))
}

func (m *Manager) upload(ctx context.Context, objectName string, reader io.Reader, size int64) (filemanager.UploadedFile, error) {
	objectName, err := normalizeObjectName(objectName)
	if err != nil {
		return filemanager.UploadedFile{}, err
	}
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	u := m.objectURL(objectName)
	query := u.Query()
	query.Set("resource", "file")
	u.RawQuery = query.Encode()
	resp, err := m.request(ctx, http.MethodPut, u, nil, nil, nil)
	if err != nil {
		return filemanager.UploadedFile{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return filemanager.UploadedFile{}, responseError("creating file", resp)
	}
	_ = resp.Body.Close()

	query = u.Query()
	query.Del("resource")
	query.Set("action", "append")
	query.Set("position", "0")
	u.RawQuery = query.Encode()
	headers := http.Header{"Content-Type": []string{"application/octet-stream"}}
	resp, err = m.request(ctx, http.MethodPatch, u, reader, &size, headers)
	if err != nil {
		return filemanager.UploadedFile{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return filemanager.UploadedFile{}, responseError("uploading file", resp)
	}
	_ = resp.Body.Close()

	query.Set("action", "flush")
	query.Set("position", strconv.FormatInt(size, 10))
	u.RawQuery = query.Encode()
	resp, err = m.request(ctx, http.MethodPatch, u, nil, nil, nil)
	if err != nil {
		return filemanager.UploadedFile{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return filemanager.UploadedFile{}, responseError("flushing file", resp)
	}
	_ = resp.Body.Close()

	locationURL := m.objectURL(objectName)
	return filemanager.UploadedFile{Location: locationURL.String(), ObjectName: objectName}, nil
}

func (m *Manager) Download(ctx context.Context, output io.WriterAt, key string, options ...filemanager.DownloadOption) error {
	if len(options) > 0 {
		return errors.New("OneLake range downloads are unsupported")
	}
	objectName, err := m.GetObjectNameFromLocation(key)
	if err != nil {
		return err
	}
	objectName, err = normalizeObjectName(objectName)
	if err != nil {
		return err
	}
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()
	resp, err := m.request(ctx, http.MethodGet, m.objectURL(objectName), nil, nil, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return responseError("downloading file", resp)
	}
	defer func() { _ = resp.Body.Close() }()
	_, err = io.Copy(&writerAtAdapter{writer: output}, resp.Body)
	if err != nil {
		return fmt.Errorf("writing OneLake download: %w", err)
	}
	return nil
}

func (m *Manager) Delete(ctx context.Context, keys []string) error {
	for _, key := range keys {
		objectName, err := m.GetObjectNameFromLocation(key)
		if err != nil {
			return err
		}
		objectName, err = normalizeObjectName(objectName)
		if err != nil {
			return err
		}
		requestCtx, cancel := m.withTimeout(ctx)
		resp, err := m.request(requestCtx, http.MethodDelete, m.objectURL(objectName), nil, nil, nil)
		cancel()
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
			return responseError("deleting file", resp)
		}
		_ = resp.Body.Close()
	}
	return nil
}

func (m *Manager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) filemanager.ListSession {
	objectPrefix := m.config.Prefix
	var err error
	if prefix != "" {
		objectPrefix, err = m.prefixedObjectName(prefix)
	}
	return &listSession{manager: m, ctx: ctx, startAfter: startAfter, prefix: objectPrefix, maxItems: maxItems, initErr: err}
}

func (m *Manager) Prefix() string { return m.config.Prefix }

func (m *Manager) SetTimeout(timeout time.Duration) {
	m.mu.Lock()
	m.timeout = timeout
	m.mu.Unlock()
}

func (m *Manager) GetObjectNameFromLocation(location string) (string, error) {
	u, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("parsing OneLake location: %w", err)
	}
	base := m.baseURL()
	if u.Host != "" {
		if u.Scheme != base.Scheme || u.Host != base.Host || !strings.HasPrefix(u.Path, base.Path+"/") {
			return "", errors.New("location is outside the configured OneLake Lakehouse")
		}
		return normalizeObjectName(strings.TrimPrefix(strings.TrimPrefix(u.Path, base.Path), "/"))
	}
	return normalizeObjectName(location)
}

func (m *Manager) GetDownloadKeyFromFileLocation(location string) string {
	key, err := m.GetObjectNameFromLocation(location)
	if err != nil {
		return ""
	}
	return key
}

func normalizeObjectName(name string) (string, error) {
	if name == "" || strings.Contains(name, `\`) || strings.HasPrefix(name, "/") {
		return "", errors.New("OneLake object name is invalid")
	}
	for segment := range strings.SplitSeq(name, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", errors.New("OneLake object name is invalid")
		}
	}
	return name, nil
}

type writerAtAdapter struct {
	writer io.WriterAt
	offset int64
}

func (w *writerAtAdapter) Write(data []byte) (int, error) {
	n, err := w.writer.WriteAt(data, w.offset)
	w.offset += int64(n)
	return n, err
}

type listSession struct {
	manager      *Manager
	ctx          context.Context
	startAfter   string
	prefix       string
	maxItems     int64
	continuation string
	initErr      error
	done         bool
}

func (s *listSession) Next() ([]*filemanager.FileInfo, error) {
	if s.initErr != nil {
		s.done = true
		return nil, s.initErr
	}
	if s.done || s.maxItems <= 0 {
		return nil, nil
	}
	ctx, cancel := s.manager.withTimeout(s.ctx)
	defer cancel()

	u := s.manager.baseURL()
	query := u.Query()
	query.Set("resource", "filesystem")
	query.Set("recursive", "true")
	query.Set("directory", s.prefix)
	query.Set("maxResults", strconv.FormatInt(s.maxItems, 10))
	if s.continuation != "" {
		query.Set("continuation", s.continuation)
	}
	u.RawQuery = query.Encode()
	resp, err := s.manager.request(ctx, http.MethodGet, u, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, responseError("listing files", resp)
	}
	defer func() { _ = resp.Body.Close() }()

	var listing struct {
		Paths []struct {
			Name         string `json:"name"`
			LastModified string `json:"lastModified"`
			IsDirectory  bool   `json:"isDirectory"`
		} `json:"paths"`
	}
	if err := jsonrs.NewDecoder(io.LimitReader(resp.Body, 4*1024*1024)).Decode(&listing); err != nil {
		return nil, fmt.Errorf("decoding OneLake file listing: %w", err)
	}
	s.continuation = resp.Header.Get("x-ms-continuation")
	s.done = s.continuation == ""
	files := make([]*filemanager.FileInfo, 0, len(listing.Paths))
	for _, item := range listing.Paths {
		if item.IsDirectory || strings.Compare(item.Name, s.startAfter) <= 0 {
			continue
		}
		modified, _ := http.ParseTime(item.LastModified)
		files = append(files, &filemanager.FileInfo{Key: item.Name, LastModified: modified})
	}
	return files, nil
}
