package context

import (
	"context"
	"encoding/json"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type (
	destIDContextKey struct{}
	secretContextKey struct{}
)

// CtxWithDestInfo returns a new context with the given destination.
func CtxWithDestInfo(ctx context.Context, dest *backendconfig.DestinationT) context.Context {
	return context.WithValue(ctx, destIDContextKey{}, dest)
}

// DestInfoFromCtx returns the destination from the context, if present.
func DestInfoFromCtx(ctx context.Context) (*backendconfig.DestinationT, bool) {
	dest, ok := ctx.Value(destIDContextKey{}).(*backendconfig.DestinationT)
	return dest, ok
}

// CtxWithSecret returns a new context with the given secret.
func CtxWithSecret(ctx context.Context, secret json.RawMessage) context.Context {
	return context.WithValue(ctx, secretContextKey{}, secret)
}

// SecretFromCtx returns the secret from the context, if present.
func SecretFromCtx(ctx context.Context) (json.RawMessage, bool) {
	secret, ok := ctx.Value(secretContextKey{}).(json.RawMessage)
	return secret, ok
}
