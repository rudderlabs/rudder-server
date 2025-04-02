package validator

import (
	"errors"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

func TestMessageIDValidator(t *testing.T) {
	t.Run("valid message ID", func(t *testing.T) {
		v := newMessageIDValidator()
		payload := []byte(`{"messageId": "test-msg-id"}`)

		valid, err := v.Validate(payload, nil)
		require.NoError(t, err)
		require.True(t, valid)
	})

	t.Run("missing message ID", func(t *testing.T) {
		v := newMessageIDValidator()
		payload := []byte(`{}`)

		valid, err := v.Validate(payload, nil)
		require.NoError(t, err)
		require.False(t, valid)
	})
}

func TestReqTypeValidator(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		props   *stream.MessageProperties // Add props to test cases
		valid   bool
	}{
		{"valid type", `{"type":"track"}`, &stream.MessageProperties{RequestType: "track"}, true},
		{"invalid type", `{}`, &stream.MessageProperties{RequestType: "invalid"}, false},
		{"missing type", `{}`, &stream.MessageProperties{}, true}, // Empty properties
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newReqTypeValidator()
			valid, err := v.Validate([]byte(tt.payload), tt.props) // Pass props
			require.NoError(t, err)
			require.Equal(t, tt.valid, valid)
		})
	}
}

func TestReceivedAtValidator(t *testing.T) {
	now := time.Now().Format(time.RFC3339Nano)

	tests := []struct {
		name    string
		payload string
		valid   bool
	}{
		{"valid timestamp", `{"receivedAt": "` + now + `"}`, true},
		{"missing field", `{}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newReceivedAtValidator()
			valid, err := v.Validate([]byte(tt.payload), nil)
			require.NoError(t, err)
			require.Equal(t, tt.valid, valid)
		})
	}
}

func TestRudderIDValidator(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		valid   bool
	}{
		{"valid user ID", `{"rudderId": "user123"}`, true},
		{"missing both IDs", `{}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newRudderIDValidator()
			valid, err := v.Validate([]byte(tt.payload), nil)
			require.NoError(t, err)
			require.Equal(t, tt.valid, valid)
		})
	}
}

func TestMediator_Validate(t *testing.T) {
	validPayload := []byte(`{
        "messageId": "msg123",
        "type": "track",
        "receivedAt": "2023-10-01T12:00:00Z",
        "userId": "user123",
		"rudderId": "rud-id",
		"request_ip": "192.168.1.1"
    }`)

	baseProps := &stream.MessageProperties{
		RequestType: "track",
		RequestIP:   "192.168.1.1",
		RoutingKey:  "rand-routeing-key",
		WorkspaceID: "workspace-id",
		SourceID:    "source-id",
		ReceivedAt:  time.Now(),
	}

	tests := []struct {
		name          string
		mockValidator func(*stream.MessageProperties) error
		payload       []byte
		props         *stream.MessageProperties
		want          bool
		wantErr       bool
	}{
		{
			name:          "all validations pass",
			payload:       validPayload,
			props:         baseProps,
			mockValidator: func(*stream.MessageProperties) error { return nil },
			want:          true,
			wantErr:       false,
		},
		{
			name:          "msg properties validation fails",
			payload:       validPayload,
			props:         baseProps,
			mockValidator: func(*stream.MessageProperties) error { return errors.New("validation error") },
			want:          false,
			wantErr:       true,
		},
		{
			name:          "missing message ID",
			payload:       []byte(`{"type": "track"}`),
			props:         baseProps,
			mockValidator: func(*stream.MessageProperties) error { return nil },
			want:          false,
			wantErr:       false,
		},
		{
			name:          "invalid receivedAt format",
			payload:       []byte(`{"receivedAt": "invalid"}`),
			props:         baseProps,
			mockValidator: func(*stream.MessageProperties) error { return nil },
			want:          false,
			wantErr:       false,
		},
		{
			name:          "missing user identifiers",
			payload:       []byte(`{"messageId": "msg123"}`),
			props:         baseProps,
			mockValidator: func(*stream.MessageProperties) error { return nil },
			want:          false,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mediator := NewValidateMediator(logger.NOP, tt.mockValidator)
			got, err := mediator.Validate(tt.payload, tt.props)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.True(t, got == tt.want, "expected %v, got %v", tt.want, got)
		})
	}
}
