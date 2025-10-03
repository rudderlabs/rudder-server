package context

import (
	"context"
	"encoding/json"

	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

type (
	destIDContextKey struct{}
	secretContextKey struct{}
)

// CtxWithDestInfo returns a new context with the given DestinationInfo.
func CtxWithDestInfo(ctx context.Context, info *v2.DestinationInfo) context.Context {
	return context.WithValue(ctx, destIDContextKey{}, info)
}

// DestInfoFromCtx returns the DestinationInfo from the context, if present.
func DestInfoFromCtx(ctx context.Context) (*v2.DestinationInfo, bool) {
	info, ok := ctx.Value(destIDContextKey{}).(*v2.DestinationInfo)
	return info, ok
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
