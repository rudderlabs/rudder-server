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

func CtxWithDestInfo(ctx context.Context, info *v2.DestinationInfo) context.Context {
	return context.WithValue(ctx, destIDContextKey{}, info)
}

func DestInfoFromCtx(ctx context.Context) (*v2.DestinationInfo, bool) {
	info, ok := ctx.Value(destIDContextKey{}).(*v2.DestinationInfo)
	return info, ok
}

func CtxWithSecret(ctx context.Context, secret json.RawMessage) context.Context {
	return context.WithValue(ctx, secretContextKey{}, secret)
}

func SecretFromCtx(ctx context.Context) (json.RawMessage, bool) {
	secret, ok := ctx.Value(secretContextKey{}).(json.RawMessage)
	return secret, ok
}
