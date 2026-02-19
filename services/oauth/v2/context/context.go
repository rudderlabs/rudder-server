package context

import (
	"context"
	"encoding/json"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type (
	destContextKey   struct{}
	secretContextKey struct{}
)

// CtxWithDestination returns a new context with the given destination.
func CtxWithDestination(ctx context.Context, dest *backendconfig.DestinationT) context.Context {
	return context.WithValue(ctx, destContextKey{}, dest)
}

// DestinationFromCtx returns the destination from the context, if present.
func DestinationFromCtx(ctx context.Context) (*backendconfig.DestinationT, bool) {
	dest, ok := ctx.Value(destContextKey{}).(*backendconfig.DestinationT)
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
