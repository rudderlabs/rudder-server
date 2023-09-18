package driverctx

import (
	"context"
)

// Key name to look for Correlation Id in context
// using custom type to prevent key collision
type contextKey int

const (
	CorrelationIdContextKey contextKey = iota
	ConnIdContextKey
	QueryIdContextKey
	QueryIdCallbackKey
	ConnIdCallbackKey
)

type IdCallbackFunc func(string)

// NewContextWithCorrelationId creates a new context with correlationId value. Used by Logger to populate field corrId.
func NewContextWithCorrelationId(ctx context.Context, correlationId string) context.Context {
	return context.WithValue(ctx, CorrelationIdContextKey, correlationId)
}

// CorrelationIdFromContext retrieves the correlationId stored in context.
func CorrelationIdFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	corrId, ok := ctx.Value(CorrelationIdContextKey).(string)
	if !ok {
		return ""
	}
	return corrId
}

// NewContextWithConnId creates a new context with connectionId value.
func NewContextWithConnId(ctx context.Context, connId string) context.Context {
	if callback, ok := ctx.Value(ConnIdCallbackKey).(IdCallbackFunc); ok {
		callback(connId)
	}
	return context.WithValue(ctx, ConnIdContextKey, connId)
}

// ConnIdFromContext retrieves the connectionId stored in context.
func ConnIdFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	connId, ok := ctx.Value(ConnIdContextKey).(string)
	if !ok {
		return ""
	}
	return connId
}

// NewContextWithQueryId creates a new context with queryId value.
func NewContextWithQueryId(ctx context.Context, queryId string) context.Context {
	if callback, ok := ctx.Value(QueryIdCallbackKey).(IdCallbackFunc); ok {
		callback(queryId)
	}

	return context.WithValue(ctx, QueryIdContextKey, queryId)
}

// QueryIdFromContext retrieves the queryId stored in context.
func QueryIdFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	queryId, ok := ctx.Value(QueryIdContextKey).(string)
	if !ok {
		return ""
	}
	return queryId
}

func NewContextWithQueryIdCallback(ctx context.Context, callback IdCallbackFunc) context.Context {
	return context.WithValue(ctx, QueryIdCallbackKey, callback)
}

func NewContextWithConnIdCallback(ctx context.Context, callback IdCallbackFunc) context.Context {
	return context.WithValue(ctx, ConnIdCallbackKey, callback)
}
