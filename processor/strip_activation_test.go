package processor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestStripActivationMetadata(t *testing.T) {
	tests := []struct {
		name               string
		isReverseETL       bool
		event              types.SingularEventT
		expectedActivation any // value expected under context.activation; nil means the key must be absent
		expectContextKept  bool
	}{
		{
			name:         "rETL: only our metering fields -> activation namespace removed",
			isReverseETL: true,
			event: types.SingularEventT{
				"context": map[string]any{"activation": map[string]any{"fingerprint": "fp-1", "origin": "org-1"}},
			},
			expectedActivation: nil,
			expectContextKept:  true,
		},
		{
			name:         "rETL: only fingerprint -> activation namespace removed",
			isReverseETL: true,
			event: types.SingularEventT{
				"context": map[string]any{"activation": map[string]any{"fingerprint": "fp-1"}},
			},
			expectedActivation: nil,
			expectContextKept:  true,
		},
		{
			name:         "rETL: only origin -> activation namespace removed",
			isReverseETL: true,
			event: types.SingularEventT{
				"context": map[string]any{"activation": map[string]any{"origin": "org-1"}},
			},
			expectedActivation: nil,
			expectContextKept:  true,
		},
		{
			name:         "rETL: metering fields alongside customer field -> only ours stripped",
			isReverseETL: true,
			event: types.SingularEventT{
				"context": map[string]any{"activation": map[string]any{"fingerprint": "fp-2", "origin": "org-2", "custom": "keep-me"}},
			},
			expectedActivation: map[string]any{"custom": "keep-me"},
			expectContextKept:  true,
		},
		{
			name:         "rETL: customer activation without our fields -> untouched",
			isReverseETL: true,
			event: types.SingularEventT{
				"context": map[string]any{"activation": map[string]any{"custom": "keep-me"}},
			},
			expectedActivation: map[string]any{"custom": "keep-me"},
			expectContextKept:  true,
		},
		{
			name:               "rETL: no context -> no-op",
			isReverseETL:       true,
			event:              types.SingularEventT{"messageId": "m1"},
			expectedActivation: nil,
			expectContextKept:  false,
		},
		{
			name:         "rETL: context without activation -> no-op",
			isReverseETL: true,
			event: types.SingularEventT{
				"context": map[string]any{"traits": map[string]any{"email": "a@b.com"}},
			},
			expectedActivation: nil,
			expectContextKept:  true,
		},
		{
			name:         "non-rETL: metering fields preserved (gate)",
			isReverseETL: false,
			event: types.SingularEventT{
				"context": map[string]any{"activation": map[string]any{"fingerprint": "fp-3", "origin": "org-3"}},
			},
			expectedActivation: map[string]any{"fingerprint": "fp-3", "origin": "org-3"},
			expectContextKept:  true,
		},
		{
			name:         "non-rETL: customer activation fully preserved (gate)",
			isReverseETL: false,
			event: types.SingularEventT{
				"context": map[string]any{"activation": map[string]any{"fingerprint": "fp-4", "custom": "keep-me"}},
			},
			expectedActivation: map[string]any{"fingerprint": "fp-4", "custom": "keep-me"},
			expectContextKept:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Mirror the caller's gate: stripping only runs for reverse-ETL sources.
			if tc.isReverseETL {
				stripActivationMetadata(tc.event)
			}

			ctxMap, hasContext := tc.event["context"].(map[string]any)
			require.Equal(t, tc.expectContextKept, hasContext, "context presence mismatch")
			if !hasContext {
				return
			}
			activation, hasActivation := ctxMap["activation"]
			if tc.expectedActivation == nil {
				require.False(t, hasActivation, "expected context.activation to be absent")
				return
			}
			require.True(t, hasActivation, "expected context.activation to be present")
			require.Equal(t, tc.expectedActivation, activation)
		})
	}
}
