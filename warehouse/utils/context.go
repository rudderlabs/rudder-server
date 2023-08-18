package warehouseutils

import (
	"context"
)

type key int

const (
	uploadID key = iota
)

func CtxWithUploadID(ctx context.Context, uid int64) context.Context {
	return context.WithValue(ctx, uploadID, uid)
}

func UploadIDFromCtx(ctx context.Context) (int64, bool) {
	userIP, ok := ctx.Value(uploadID).(int64)
	return userIP, ok
}
