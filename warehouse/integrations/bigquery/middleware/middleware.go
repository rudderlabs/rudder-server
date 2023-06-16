package middleware

import (
	"context"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"
)

type Opt func(*Client)

type loggerMW interface {
	Infow(msg string, keysAndValues ...interface{})
}

type Client struct {
	*bigquery.Client

	since              func(time.Time) time.Duration
	logger             loggerMW
	keysAndValues      []any
	slowQueryThreshold time.Duration
}

func WithLogger(logger loggerMW) Opt {
	return func(s *Client) {
		s.logger = logger
	}
}

func WithKeyAndValues(keyAndValues ...any) Opt {
	return func(s *Client) {
		s.keysAndValues = keyAndValues
	}
}

func WithSlowQueryThreshold(slowQueryThreshold time.Duration) Opt {
	return func(s *Client) {
		s.slowQueryThreshold = slowQueryThreshold
	}
}

func WithSince(since func(time.Time) time.Duration) Opt {
	return func(s *Client) {
		s.since = since
	}
}

func New(client *bigquery.Client, opts ...Opt) *Client {
	s := &Client{
		Client:             client,
		since:              time.Since,
		slowQueryThreshold: 300 * time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (client *Client) Run(ctx context.Context, query *bigquery.Query) (*bigquery.Job, error) {
	startedAt := time.Now()
	job, err := query.Run(ctx)
	client.logQuery(query, client.since(startedAt))
	return job, err
}

func (client *Client) Read(ctx context.Context, query *bigquery.Query) (it *bigquery.RowIterator, err error) {
	startedAt := time.Now()
	it, err = query.Read(ctx)
	client.logQuery(query, client.since(startedAt))
	return it, err
}

func (client *Client) logQuery(query *bigquery.Query, elapsed time.Duration) {
	if elapsed < client.slowQueryThreshold {
		return
	}

	queryStatement := query.QueryConfig.Q

	keysAndValues := []any{
		logfield.Query, queryStatement,
		logfield.QueryExecutionTime, elapsed,
	}
	keysAndValues = append(keysAndValues, client.keysAndValues...)

	client.logger.Infow("executing query", keysAndValues...)
}
