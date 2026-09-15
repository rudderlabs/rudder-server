package bigquery

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestResolveNamespace(t *testing.T) {
	ctx := context.Background()

	// namespace as sanitised by warehouseutils.ToSafeNamespace, which snake-cases
	// the configured value and so splits the letter/digit boundary in `euwe1`.
	const (
		configuredNamespace = "bq_events_zm_dev_dts_euwe1"
		sanitisedNamespace  = "bq_events_zm_dev_dts_euwe_1"
	)

	newBQ := func(t *testing.T, destConfig map[string]any, namespace string) *BigQuery {
		t.Helper()

		bq := New(config.New(), logger.NOP)
		bq.namespace = namespace
		bq.warehouse = model.Warehouse{
			Namespace:   namespace,
			Destination: backendconfig.DestinationT{Config: destConfig},
		}
		return bq
	}

	existing := func(names ...string) func(context.Context, string) (bool, error) {
		return func(_ context.Context, namespace string) (bool, error) {
			for _, name := range names {
				if name == namespace {
					return true, nil
				}
			}
			return false, nil
		}
	}

	t.Run("keeps the configured dataset when it already exists", func(t *testing.T) {
		bq := newBQ(t, map[string]any{"namespace": configuredNamespace}, sanitisedNamespace)

		require.Equal(t, configuredNamespace, bq.resolveNamespace(ctx, existing(configuredNamespace)))
	})

	t.Run("keeps the sanitised dataset when the configured one does not exist", func(t *testing.T) {
		bq := newBQ(t, map[string]any{"namespace": configuredNamespace}, sanitisedNamespace)

		require.Equal(t, sanitisedNamespace, bq.resolveNamespace(ctx, existing()))
	})

	t.Run("prefers the configured dataset when both datasets exist", func(t *testing.T) {
		bq := newBQ(t, map[string]any{"namespace": configuredNamespace}, sanitisedNamespace)

		resolved := bq.resolveNamespace(ctx, existing(configuredNamespace, sanitisedNamespace))
		require.Equal(t, configuredNamespace, resolved)
	})

	t.Run("keeps the sanitised dataset when the existence check fails", func(t *testing.T) {
		bq := newBQ(t, map[string]any{"namespace": configuredNamespace}, sanitisedNamespace)

		resolved := bq.resolveNamespace(ctx, func(context.Context, string) (bool, error) {
			return false, errors.New("permission denied")
		})
		require.Equal(t, sanitisedNamespace, resolved)
	})

	t.Run("no existence check when sanitising did not change the namespace", func(t *testing.T) {
		bq := newBQ(t, map[string]any{"namespace": "bq_events"}, "bq_events")

		called := false
		resolved := bq.resolveNamespace(ctx, func(context.Context, string) (bool, error) {
			called = true
			return true, nil
		})
		require.Equal(t, "bq_events", resolved)
		require.False(t, called, "existence check should be skipped")
	})

	t.Run("no existence check when no namespace is configured", func(t *testing.T) {
		bq := newBQ(t, map[string]any{}, sanitisedNamespace)

		called := false
		resolved := bq.resolveNamespace(ctx, func(context.Context, string) (bool, error) {
			called = true
			return true, nil
		})
		require.Equal(t, sanitisedNamespace, resolved)
		require.False(t, called, "existence check should be skipped")
	})

	t.Run("whitespace only namespace is ignored", func(t *testing.T) {
		bq := newBQ(t, map[string]any{"namespace": "   "}, sanitisedNamespace)

		require.Equal(t, sanitisedNamespace, bq.resolveNamespace(ctx, existing("   ")))
	})
}
