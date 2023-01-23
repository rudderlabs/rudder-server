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

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type OptFn func(c *ClientImpl)

type Tags []string

type Service []string

type Alert struct {
	Resource    string   `json:"resource"`    // warehouse-upload-aborted
	Event       string   `json:"event"`       // CRITICAL/rudder/<resource>:<group>
	Environment string   `json:"environment"` // PRODUCTION,DEVELOPMENT,PROXYMODE,CARBONCOPY
	Severity    Severity `json:"severity"`    // warning,critical,normal // Get the full list from https://docs.alerta.io/api/alert.html#severity-table
	Group       string   `json:"group"`       // cluster=rudder,destID=27CHciD6leAhurSyFAeN4dp14qZ,destType=RS,namespace=hosted,notificationServiceMode=CARBONCOPY,priority=P1,sendToNotificationService=true,team=warehouse
	Text        string   `json:"text"`        // <event> is CRITICAL warehouse-upload-aborted-count: 1
	Service     Service  `json:"service"`     // {upload_aborted}
	Timeout     int      `json:"timeout"`     // 86400
	Tags        Tags     `json:"tags"`        // {sendToNotificationService=true,notificationServiceMode=CARBONCOPY,team=warehouse,priority=P1,destID=27CHciD6leAhurSyFAeN4dp14qZ,destType=RS,namespace=hosted,cluster=rudder}
}

type Priority string

type Severity string

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

var (
	defaultTimeout    = 30 * time.Second
	defaultMaxRetries = 3
	defaultSeverity   = SeverityCritical
	defaultPriority   = PriorityP1
	defaultTeam       = "No Team"
)

type Client interface {
	SendAlert(ctx context.Context, resource string, service Service) error
}

type ClientImpl struct {
	client   *http.Client
	severity Severity
	priority Priority
	retries  int
	url      string
	text     string
	tags     Tags
	team     string
}

func WithHTTPClient(httpClient *http.Client) OptFn {
	return func(c *ClientImpl) {
		c.client = httpClient
	}
}

func WithTimeout(timeout time.Duration) OptFn {
	return func(c *ClientImpl) {
		c.client.Timeout = timeout
	}
}

func WithMaxRetries(retries int) OptFn {
	return func(c *ClientImpl) {
		c.retries = retries
	}
}

func WithSeverity(severity Severity) OptFn {
	return func(c *ClientImpl) {
		c.severity = severity
	}
}

func WithText(text string) OptFn {
	return func(c *ClientImpl) {
		c.text = text
	}
}

func WithTags(tags Tags) OptFn {
	return func(c *ClientImpl) {
		c.tags = tags
	}
}

func WithPriority(priority Priority) OptFn {
	return func(c *ClientImpl) {
		c.priority = priority
	}
}

func WithTeam(team string) OptFn {
	return func(c *ClientImpl) {
		c.team = team
	}
}

func NewClient(baseURL string, fns ...OptFn) *ClientImpl {
	c := &ClientImpl{
		url: baseURL,
		client: &http.Client{
			Timeout: defaultTimeout,
		},

		retries:  defaultMaxRetries,
		severity: defaultSeverity,
		priority: defaultPriority,
		team:     defaultTeam,
	}

	for _, fn := range fns {
		fn(c)
	}

	return c
}

func (c *ClientImpl) retry(ctx context.Context, fn func() error) error {
	var opts backoff.BackOff

	opts = backoff.NewExponentialBackOff()
	opts = backoff.WithMaxRetries(opts, uint64(c.retries))
	opts = backoff.WithContext(opts, ctx)

	return backoff.Retry(fn, opts)
}

func (c *ClientImpl) SendAlert(ctx context.Context, resource string, service Service) error {
	environment := config.GetString("alerta.environment", "PRODUCTION")
	timeout := config.GetInt("alerta.timeout", 86400)

	// Adding default tags
	c.tags = append(c.tags, fmt.Sprintf("sendToNotificationService=%t", true))
	c.tags = append(c.tags, fmt.Sprintf("notificationServiceMode=%s", environment))
	c.tags = append(c.tags, fmt.Sprintf("priority=%s", string(c.priority)))
	c.tags = append(c.tags, fmt.Sprintf("team=%s", c.team))
	if len(config.GetKubeNamespace()) > 0 {
		c.tags = append(c.tags, fmt.Sprintf("namespace=%s", config.GetKubeNamespace()))
	}

	sort.Strings(c.tags)

	group := strings.Join(c.tags, ",")
	event := fmt.Sprintf("%s/rudder/%s:%s", c.severity, resource, group)
	text := fmt.Sprintf("%s is %s %s", event, c.severity, c.text)

	payload := Alert{
		Resource:    resource,
		Environment: environment,
		Timeout:     timeout,
		Group:       group,
		Service:     service,
		Event:       event,
		Text:        text,
		Tags:        c.tags,
		Severity:    c.severity,
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

		resp, err := c.client.Do(req)
		if err != nil {
			return fmt.Errorf("http request to %q: %w", c.url, err)
		}
		defer func() { httputil.CloseResponse(resp) }()

		if resp.StatusCode != http.StatusOK {
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
