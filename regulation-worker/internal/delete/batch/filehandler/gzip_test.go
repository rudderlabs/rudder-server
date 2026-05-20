package filehandler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

func TestRemoveIdentitySingleUser(t *testing.T) {
	inputs := []struct {
		name         string
		idFieldPath  []string
		userID       string
		inputByte    []byte
		expectedByte []byte
	}{
		{
			name:         "CamelCase top-level userId match",
			idFieldPath:  []string{"userId"},
			userID:       "my-user-id",
			inputByte:    []byte("{\"userId\": \"my-user-id\"}\n"),
			expectedByte: []byte(""),
		},
		{
			name:         "CamelCase top-level userId no match",
			idFieldPath:  []string{"userId"},
			userID:       "other-id",
			inputByte:    []byte("{\"userId\": \"my-user-id\"}\n"),
			expectedByte: []byte("{\"userId\": \"my-user-id\"}\n"),
		},
		{
			name:         "CamelCase multiple lines removes matching",
			idFieldPath:  []string{"userId"},
			userID:       "my-another-user-id",
			inputByte:    []byte("{\"userId\": \"my-user-id\", \"context-app-name\": \"my-app-name\"}\n{\"userId\": \"my-another-user-id\"}\n"),
			expectedByte: []byte("{\"userId\": \"my-user-id\", \"context-app-name\": \"my-app-name\"}\n"),
		},
		{
			name:         "CamelCase special characters in userId",
			idFieldPath:  []string{"userId"},
			userID:       "!@#$%^&*()",
			inputByte:    []byte("{\"userId\": \"!@#$%^&*()\"}\n"),
			expectedByte: []byte(""),
		},
		{
			name:         "SnakeCase nested data.user_id match",
			idFieldPath:  []string{"data", "user_id"},
			userID:       "my-user-id",
			inputByte:    []byte("{\"data\": {\"user_id\": \"my-user-id\"}}\n"),
			expectedByte: []byte(""),
		},
		{
			name:         "SnakeCase nested data.user_id no match",
			idFieldPath:  []string{"data", "user_id"},
			userID:       "other-id",
			inputByte:    []byte("{\"data\": {\"user_id\": \"my-user-id\"}}\n"),
			expectedByte: []byte("{\"data\": {\"user_id\": \"my-user-id\"}}\n"),
		},
		{
			name:         "SnakeCase multiple lines removes matching",
			idFieldPath:  []string{"data", "user_id"},
			userID:       "user-id-2",
			inputByte:    []byte("{\"data\": {\"user_id\": \"user-id-1\"}}\n{\"data\": {\"user_id\": \"user-id-2\"}}\n"),
			expectedByte: []byte("{\"data\": {\"user_id\": \"user-id-1\"}}\n"),
		},
	}

	for _, ip := range inputs {
		t.Run(ip.name, func(t *testing.T) {
			h := NewGZIPLocalFileHandler(ip.idFieldPath)
			h.records = ip.inputByte
			err := h.RemoveIdentity(context.TODO(), []model.User{{ID: ip.userID}})
			require.NoError(t, err)
			require.Equal(t, string(ip.expectedByte), string(h.records))
		})
	}
}

func TestRemoveIdentityMultipleUsers(t *testing.T) {
	inputs := []struct {
		name         string
		idFieldPath  []string
		userIds      []model.User
		inputByte    []byte
		expectedByte []byte
	}{
		{
			name:        "CamelCase remove multiple users",
			idFieldPath: []string{"userId"},
			userIds: []model.User{
				{ID: "user-id-1"},
				{ID: "user-id-3"},
			},
			inputByte:    []byte("{\"userId\": \"user-id-1\"}\n{\"userId\": \"user-id-2\"}\n{\"userId\": \"user-id-3\"}\n"),
			expectedByte: []byte("{\"userId\": \"user-id-2\"}\n"),
		},
		{
			name:        "SnakeCase remove multiple users from nested data",
			idFieldPath: []string{"data", "user_id"},
			userIds: []model.User{
				{ID: "user-id-1"},
				{ID: "user-id-3"},
			},
			inputByte:    []byte("{\"data\": {\"user_id\": \"user-id-1\"}}\n{\"data\": {\"user_id\": \"user-id-2\"}}\n{\"data\": {\"user_id\": \"user-id-3\"}}\n"),
			expectedByte: []byte("{\"data\": {\"user_id\": \"user-id-2\"}}\n"),
		},
	}

	for _, ip := range inputs {
		t.Run(ip.name, func(t *testing.T) {
			h := NewGZIPLocalFileHandler(ip.idFieldPath)
			h.records = ip.inputByte
			err := h.RemoveIdentity(context.TODO(), ip.userIds)
			require.NoError(t, err)
			require.Equal(t, string(ip.expectedByte), string(h.records))
		})
	}
}
