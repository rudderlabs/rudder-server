package warehouseutils

import (
	"context"
)

type uploadIDContextKey struct{}

func CtxWithUploadID(ctx context.Context, uid int64) context.Context {
	return context.WithValue(ctx, uploadIDContextKey{}, uid)
}

func UploadIDFromCtx(ctx context.Context) (int64, bool) {
	uploadID, ok := ctx.Value(uploadIDContextKey{}).(int64)
	return uploadID, ok
}
