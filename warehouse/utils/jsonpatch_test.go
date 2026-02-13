package warehouseutils

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateAndApplyJSONPatch(t *testing.T) {
	tests := []struct {
		name          string
		original      string
		modified      string
		expectedPatch []map[string]any
	}{
		{
			name:          "no changes",
			original:      `{"a":1}`,
			modified:      `{"a":1}`,
			expectedPatch: []map[string]any{},
		},
		{
			name:     "simple add",
			original: `{"a":1}`,
			modified: `{"a":1,"b":2}`,
			expectedPatch: []map[string]any{
				{"op": "add", "path": "/b", "value": float64(2)},
			},
		},
		{
			name:     "simple remove",
			original: `{"a":1,"b":2}`,
			modified: `{"a":1}`,
			expectedPatch: []map[string]any{
				{"op": "remove", "path": "/b"},
			},
		},
		{
			name:     "simple replace",
			original: `{"a":1}`,
			modified: `{"a":2}`,
			expectedPatch: []map[string]any{
				{"op": "replace", "path": "/a", "value": float64(2)},
			},
		},
		{
			name:     "complex nested",
			original: `{"a":{"b":1,"c":[1,2,3]},"d":4}`,
			modified: `{"a":{"b":2,"c":[1,2]},"d":4,"e":5}`,
			expectedPatch: []map[string]any{
				{"op": "replace", "path": "/a/b", "value": float64(2)},
				{"op": "remove", "path": "/a/c/2"},
				{"op": "add", "path": "/e", "value": float64(5)},
			},
		},
		{
			name:     "array add element",
			original: `{"arr":[1,2,3]}`,
			modified: `{"arr":[1,2,3,4]}`,
			expectedPatch: []map[string]any{
				{"op": "add", "path": "/arr/3", "value": float64(4)},
			},
		},
		{
			name:     "array remove element",
			original: `{"arr":[1,2,3]}`,
			modified: `{"arr":[1,3]}`,
			expectedPatch: []map[string]any{
				{"op": "remove", "path": "/arr/1"},
			},
		},
		{
			name:     "array replace element",
			original: `{"arr":[1,2,3]}`,
			modified: `{"arr":[1,4,3]}`,
			expectedPatch: []map[string]any{
				{"op": "replace", "path": "/arr/1", "value": float64(4)},
			},
		},
		{
			name:     "deeply nested object",
			original: `{"a":{"b":{"c":{"d":1}}}}`,
			modified: `{"a":{"b":{"c":{"d":2}}}}`,
			expectedPatch: []map[string]any{
				{"op": "replace", "path": "/a/b/c/d", "value": float64(2)},
			},
		},
		{
			name:     "multiple simultaneous changes",
			original: `{"a":1,"b":2,"c":3}`,
			modified: `{"a":10,"d":4}`,
			expectedPatch: []map[string]any{
				{"op": "replace", "path": "/a", "value": float64(10)},
				{"op": "remove", "path": "/b"},
				{"op": "remove", "path": "/c"},
				{"op": "add", "path": "/d", "value": float64(4)},
			},
		},
		{
			name:     "change type object to array",
			original: `{"a":{}}`,
			modified: `{"a":[1,2,3]}`,
			expectedPatch: []map[string]any{
				{"op": "replace", "path": "/a", "value": []any{float64(1), float64(2), float64(3)}},
			},
		},
		{
			name:     "change type array to object",
			original: `{"a":[1,2,3]}`,
			modified: `{"a":{"b":1}}`,
			expectedPatch: []map[string]any{
				{"op": "replace", "path": "/a", "value": map[string]any{"b": float64(1)}},
			},
		},
		{
			name:     "reorder array elements (should not patch)",
			original: `{"arr":[1,2,3]}`,
			modified: `{"arr":[3,2,1]}`,
			expectedPatch: []map[string]any{
				{"op": "replace", "path": "/arr/0", "value": float64(3)},
				{"op": "replace", "path": "/arr/2", "value": float64(1)},
			},
		},
		{
			name:     "add null value",
			original: `{"a":1}`,
			modified: `{"a":1,"b":null}`,
			expectedPatch: []map[string]any{
				{"op": "add", "path": "/b", "value": nil},
			},
		},
		{
			name:     "replace with null value",
			original: `{"a":1}`,
			modified: `{"a":null}`,
			expectedPatch: []map[string]any{
				{"op": "replace", "path": "/a", "value": nil},
			},
		},
		{
			name:     "remove nested property",
			original: `{"a":{"b":1,"c":2}}`,
			modified: `{"a":{"b":1}}`,
			expectedPatch: []map[string]any{
				{"op": "remove", "path": "/a/c"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			patch, err := GenerateJSONPatch(json.RawMessage(tc.original), json.RawMessage(tc.modified))
			require.NoError(t, err)

			applied, err := ApplyPatchToJSON(json.RawMessage(tc.original), patch)
			require.NoError(t, err)
			require.JSONEq(t, tc.modified, string(applied))
		})
	}
}
