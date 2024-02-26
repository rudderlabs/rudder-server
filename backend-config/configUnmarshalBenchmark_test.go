package backendconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func BenchmarkConfigMapUnmarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var requestData map[string]*ConfigT
		_ = jsonfast.Unmarshal(dummyConfig, &requestData)
	}
}

var dummyConfig []byte = []byte(`{"2P5AYZF3ZRFPohzQP0pjQqUew7j": null,"2P5q8KF55r50AIDWlqzLZPJ8PZ8": null,"2P5xquzeyzbpVhjwDP7yyJen0cp": null,"2P68utZBYWwXM6AKfiBGhf5Ahrp": null,"2c2stEGnBd4TBy3DVB0UBzo4Ir6": {
	"sources": [],
	"libraries": [],
	"settings": {
		"dataRetention": {
			"disableReportingPii": false,
			"useSelfStorage": false,
			"retentionPeriod": "default",
			"storagePreferences": {
				"procErrors": false,
				"gatewayDumps": false
			}
		},
		"eventAuditEnabled": false
	},
	"whtProjects": [],
	"updatedAt": "2024-02-07T15:53:41.076Z",
	"destinationTransformations": [],
	"eventReplays": {},
	"connections": {}
}}`)

var nullBytes = []byte("null")

type NullConfigT struct {
	Config ConfigT
	Valid  bool
}

func (s *NullConfigT) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, nullBytes) {
		s.Valid = false
		return nil
	}

	if err := json.Unmarshal(data, &s.Config); err != nil {
		return fmt.Errorf("null: couldn't unmarshal JSON: %w", err)
	}

	s.Valid = true
	return nil
}

// MarshalJSON implements json.Marshaler.
// It will encode null if this String is null.
func (s NullConfigT) MarshalJSON() ([]byte, error) {
	if !s.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(s.Config)
}

func BenchmarkNullConfigMapUnmarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var requestData map[string]NullConfigT
		_ = jsonfast.Unmarshal(dummyConfig, &requestData)
	}
}
