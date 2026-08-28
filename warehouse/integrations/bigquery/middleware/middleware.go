package middleware

import (
	"context"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"
)

type Opt func(*Client)

type loggerMW interface {
	Infow(msg string, keysAndValues ...any)
}

type Client struct {
	*bigquery.Client

	since              func(time.Time) time.Duration
	logger             loggerMW
	keysAndValues      []any
	slowQueryThreshold time.Duration
	runQuery           func(context.Context, *bigquery.Query) (*bigquery.Job, error)
	readQuery          func(context.Context, *bigquery.Query) (*bigquery.RowIterator, error)
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
		runQuery: func(ctx context.Context, query *bigquery.Query) (*bigquery.Job, error) {
			return query.Run(ctx)
		},
		readQuery: func(ctx context.Context, query *bigquery.Query) (*bigquery.RowIterator, error) {
			return query.Read(ctx)
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (client *Client) Run(ctx context.Context, query *bigquery.Query) (*bigquery.Job, error) {
	startedAt := time.Now()
	job, err := client.runQuery(ctx, query)
	client.logQuery(query, client.since(startedAt))
	return job, err
}

func (client *Client) Read(ctx context.Context, query *bigquery.Query) (it *bigquery.RowIterator, err error) {
	startedAt := time.Now()
	it, err = client.readQuery(ctx, query)
	client.logQuery(query, client.since(startedAt))
	return it, err
}

func (client *Client) logQuery(query *bigquery.Query, elapsed time.Duration) {
	if elapsed < client.slowQueryThreshold {
		return
	}

	queryStatement := query.Q

	keysAndValues := []any{
		logfield.Query, queryStatement,
		logfield.QueryExecutionTime, elapsed,
	}
	keysAndValues = append(keysAndValues, client.keysAndValues...)

	client.logger.Infow("executing query", keysAndValues...)
}
