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

var (
	defaultTimeout    = 30 * time.Second
	defaultMaxRetries = 3
	defaultSeverity   = "CRITICAL"
	defaultPriority   = "P1"
	defaultTeam       = "No Team"
)

type OptFn func(c *Client)

type Tags []string

type Service []string

type Alert struct {
	Resource    string  `json:"resource"`    // warehouse-upload-aborted
	Event       string  `json:"event"`       // CRITICAL/rudder/<resource>:<group>
	Environment string  `json:"environment"` // PRODUCTION,DEVELOPMENT,PROXYMODE,CARBONCOPY
	Severity    string  `json:"severity"`    // warning,critical,normal // Get the full list from https://docs.alerta.io/api/alert.html#severity-table
	Group       string  `json:"group"`       // cluster=rudder,destID=27CHciD6leAhurSyFAeN4dp14qZ,destType=RS,namespace=hosted,notificationServiceMode=CARBONCOPY,priority=P1,sendToNotificationService=true,team=warehouse
	Text        string  `json:"text"`        // <event> is CRITICAL warehouse-upload-aborted-count: 1
	Service     Service `json:"service"`     // {upload_aborted}
	Timeout     int     `json:"timeout"`     // 86400
	Tags        Tags    `json:"tags"`        // {sendToNotificationService=true,notificationServiceMode=CARBONCOPY,team=warehouse,priority=P1,destID=27CHciD6leAhurSyFAeN4dp14qZ,destType=RS,namespace=hosted,cluster=rudder}
}

type Client struct {
	client      *http.Client
	retries     int
	url         string
	environment string
	timeout     int
	severity    string
	text        string
	tags        Tags
	priority    string
	team        string
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

func WithSeverity(severity string) OptFn {
	return func(c *Client) {
		c.severity = strings.ToUpper(severity)
	}
}

func WithText(text string) OptFn {
	return func(c *Client) {
		c.text = text
	}
}

func WithTags(tags Tags) OptFn {
	return func(c *Client) {
		c.tags = tags
	}
}

func WithPriority(priority string) OptFn {
	return func(c *Client) {
		c.priority = priority
	}
}

func WithTeam(team string) OptFn {
	return func(c *Client) {
		c.team = team
	}
}

func NewClient(config *config.Config, fns ...OptFn) *Client {
	c := &Client{
		client: &http.Client{
			Timeout: defaultTimeout,
		},

		retries:  defaultMaxRetries,
		severity: defaultSeverity,
		priority: defaultPriority,
		team:     defaultTeam,

		url:         config.GetString("alerta.url", "https://alerta.rudderstack.com/api/"),
		environment: config.GetString("alerta.environment", "PRODUCTION"),
		timeout:     config.GetInt("alerta.timeout", 86400),
	}

	for _, fn := range fns {
		fn(c)
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

func (c *Client) SendAlert(ctx context.Context, resource string, service Service) error {
	var url string

	// Default tags
	c.tags = append(c.tags, fmt.Sprintf("sendToNotificationService=%t", true))
	c.tags = append(c.tags, fmt.Sprintf("notificationServiceMode=%s", c.environment))
	c.tags = append(c.tags, fmt.Sprintf("priority=%s", c.priority))
	c.tags = append(c.tags, fmt.Sprintf("team=%s", c.team))
	sort.Strings(c.tags)

	group := strings.Join(c.tags, ",")
	event := fmt.Sprintf("%s/rudder/%s:%s", c.severity, resource, group)
	text := fmt.Sprintf("%s is %s %s", event, c.severity, c.text)

	payload := Alert{
		Resource:    resource,
		Environment: c.environment,
		Timeout:     c.timeout,
		Tags:        c.tags,
		Severity:    c.severity,
		Group:       group,
		Service:     service,
		Event:       event,
		Text:        text,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshalling alerta: %w", err)
	}

	return c.retry(ctx, func() error {
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
			return fmt.Errorf("unexpected status code %q on %s: %v", resp.Status, c.url, string(body))
		}
		return err
	})
}
