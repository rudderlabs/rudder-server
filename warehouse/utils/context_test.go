package warehouseutils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContext(t *testing.T) {
	t.Run("UploadID", func(t *testing.T) {
		t.Run("should return uploadID from context", func(t *testing.T) {
			ctx := context.Background()
			uploadContext := CtxWithUploadID(ctx, 1)
			uploadID, ok := UploadIDFromCtx(uploadContext)
			require.True(t, ok)
			require.Equal(t, int64(1), uploadID)
		})

		t.Run("should return false if uploadID is not present in context", func(t *testing.T) {
			ctx := context.Background()
			uploadID, ok := UploadIDFromCtx(ctx)
			require.False(t, ok)
			require.Equal(t, int64(0), uploadID)
		})
	})
}
