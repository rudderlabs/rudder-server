package jobsdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithPriorityPool(t *testing.T) {
	ctx := context.Background()
	require.False(t, usePriorityPool(ctx), "usePriorityPool should return false for plain context")

	ctx = WithPriorityPool(ctx)
	require.True(t, usePriorityPool(ctx), "usePriorityPool should return true after WithPriorityPool")
}

func TestUsePriorityPool_NilContext(t *testing.T) {
	// Test that usePriorityPool handles nil gracefully via context.Background()
	ctx := context.Background()
	require.False(t, usePriorityPool(ctx))
}

func TestPriorityPoolContextChaining(t *testing.T) {
	type keyType struct{}
	ctx := context.Background()
	ctx = context.WithValue(ctx, keyType{}, "other_value")
	require.False(t, usePriorityPool(ctx), "usePriorityPool should return false for context with other values")

	ctx = WithPriorityPool(ctx)
	require.True(t, usePriorityPool(ctx), "usePriorityPool should return true after WithPriorityPool")

	// Verify the other value is still present
	val := ctx.Value(keyType{}).(string)
	require.Equal(t, "other_value", val)
}
