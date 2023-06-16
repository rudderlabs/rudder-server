package alerta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/httputil"

	"github.com/cenkalti/backoff"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type OptFn func(*Client)

type Tags map[string]string

type Priority string

type Severity string

type Environment string

type Alert struct {
	Resource    string      `json:"resource"`    // warehouse-upload-aborted
	Event       string      `json:"event"`       // <tags_list>
	Environment Environment `json:"environment"` // [PRODUCTION,DEVELOPMENT,PROXYMODE,CARBONCOPY]
	Severity    Severity    `json:"severity"`    // warning,critical,normal // Get the full list from https://docs.alerta.io/api/alert.html#severity-table
	Text        string      `json:"text"`        // <event> is critical
	Timeout     int         `json:"timeout"`     // 86400
	TagList     []string    `json:"tags"`        // {priority=P1,destID=27CHciD6leAhurSyFAeN4dp14qZ,destType=RS,namespace=hosted,cluster=rudder}
}

type SendAlertOpts struct {
	Tags        Tags
	Text        string
	Severity    Severity
	Priority    Priority
	Environment Environment
}

const (
	SeverityCritical Severity = "critical"
	SeverityWarning  Severity = "warning"
	SeverityNormal   Severity = "normal"
	SeverityOk       Severity = "ok"
)

const (
	PriorityP1 Priority = "P1"
	PriorityP2 Priority = "P2"
	PriorityP3 Priority = "P3"
)

const (
	PRODUCTION  Environment = "PRODUCTION"
	DEVELOPMENT Environment = "DEVELOPMENT"
	PROXYMODE   Environment = "PROXYMODE"
	CARBONCOPY  Environment = "CARBONCOPY"
)

var (
	defaultTimeout     = 30 * time.Second
	defaultMaxRetries  = 3
	defaultPriority    = PriorityP1
	defaultSeverity    = SeverityCritical
	defaultEnvironment = DEVELOPMENT
)

type AlertSender interface {
	SendAlert(ctx context.Context, resource string, opts SendAlertOpts) error
}

type Client struct {
	client         *http.Client
	retries        int
	url            string
	config         *config.Config
	alertTimeout   int
	kuberNamespace string
}

func WithHTTPClient(httpClient *http.Client) OptFn {
	return func(c *Client) {
		c.client = httpClient
	}
}

func WithTimeout(timeout time.Duration) OptFn {
	return func(c *Client) {
		c.client.Timeout = timeout
	}
}

func WithMaxRetries(retries int) OptFn {
	return func(c *Client) {
		c.retries = retries
	}
}

func WithConfig(config *config.Config) OptFn {
	return func(c *Client) {
		c.config = config
	}
}

func WithAlertTimeout(timeout int) OptFn {
	return func(c *Client) {
		c.alertTimeout = timeout
	}
}

func WithKubeNamespace(namespace string) OptFn {
	return func(c *Client) {
		c.kuberNamespace = namespace
	}
}

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("alerta")
}

func NewClient(baseURL string, fns ...OptFn) AlertSender {
	c := &Client{
		url: baseURL,
		client: &http.Client{
			Timeout: defaultTimeout,
		},

		retries: defaultMaxRetries,
		config:  config.Default,
	}

	for _, fn := range fns {
		fn(c)
	}

	if !c.isEnabled() {
		pkgLogger.Info("Alerta client is disabled")
		return &NOOP{}
	}

	return c
}

func (c *Client) retry(ctx context.Context, fn func() error) error {
	var opts backoff.BackOff

	opts = backoff.NewExponentialBackOff()
	opts = backoff.WithMaxRetries(opts, uint64(c.retries))
	opts = backoff.WithContext(opts, ctx)

	return backoff.Retry(fn, opts)
}

func (c *Client) defaultTags(opts *SendAlertOpts) Tags {
	var (
		tags      = make(Tags)
		namespace = c.kuberNamespace
	)

	if namespace == "" {
		namespace = config.GetNamespaceIdentifier()
	}

	tags["namespace"] = namespace
	tags["priority"] = string(opts.Priority)

	return tags
}

func (c *Client) isEnabled() bool {
	return c.config.GetBool("ALERTA_ENABLED", true)
}

func (c *Client) setDefaultsOpts(resource string, opts *SendAlertOpts) {
	if opts.Priority == "" {
		opts.Priority = defaultPriority
	}
	if opts.Severity == "" {
		opts.Severity = defaultSeverity
	}
	if opts.Text == "" {
		opts.Text = fmt.Sprintf("%s is %s", resource, opts.Severity)
	}
	if opts.Environment == "" {
		opts.Environment = defaultEnvironment
	}
}

func (c *Client) SendAlert(ctx context.Context, resource string, opts SendAlertOpts) error {
	c.setDefaultsOpts(resource, &opts)

	// default tags
	tags := c.defaultTags(&opts)
	for k, v := range opts.Tags {
		tags[k] = v
	}

	var tagList []string
	for k, v := range tags {
		tagList = append(tagList, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(tagList)

	var (
		event        = strings.Join(tagList, ",")
		alertTimeout = c.alertTimeout
	)

	if alertTimeout == 0 {
		alertTimeout = c.config.GetInt("alerta.timeout", 86400)
	}

	payload := Alert{
		Resource:    resource,
		Timeout:     alertTimeout,
		Event:       event,
		TagList:     tagList,
		Environment: opts.Environment,
		Severity:    opts.Severity,
		Text:        opts.Text,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshalling alerta: %w", err)
	}

	return c.retry(ctx, func() error {
		url := fmt.Sprintf("%s/alert", c.url)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("creating http request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		req.Header.Set("Authorization", c.config.GetString("ALERTA_AUTH_TOKEN", ""))

		resp, err := c.client.Do(req)
		if err != nil {
			return fmt.Errorf("http request to %q: %w", c.url, err)
		}
		defer func() { httputil.CloseResponse(resp) }()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)

			err = fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
			if !httputil.RetriableStatus(resp.StatusCode) {
				return backoff.Permanent(fmt.Errorf("non retriable: %w", err))
			}
			return err
		}
		return err
	})
}
