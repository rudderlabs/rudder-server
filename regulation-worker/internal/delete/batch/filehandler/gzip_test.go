package filehandler

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

func TestRemoveIdentityRecordsFromGZIPFileWithReseredKeywords(t *testing.T) {
	inputs := []struct {
		casing       Case
		userID       string
		inputByte    []byte
		expectedByte []byte
	}{
		{
			CamelCase,
			"!@#$%^&*()***",
			[]byte("{\"userId\": \"!@#$%^&*()***\", \"context-app-name\": \"my-app-name\"}\n"),
			[]byte(""),
		},
		{
			CamelCase,
			"abc.",
			[]byte("{\"userId\":\"abc1\"}\n{\"userId\":\"abc.\"}\n"),
			[]byte("{\"userId\":\"abc1\"}\n"),
		},
	}

	for _, ip := range inputs {
		h := NewGZIPLocalFileHandler(ip.casing, false, nil)

		h.records = ip.inputByte
		fmt.Println(h.getDeletePattern(model.User{ID: ip.userID}))
		err := h.RemoveIdentity(context.TODO(), []model.User{{ID: ip.userID}})
		require.Nil(t, err)
		fmt.Println(string(h.records))
		require.Equal(t, true, bytes.Equal(h.records, ip.expectedByte))
	}
}

func TestRemoveIdentityRecordsFromGZIPFileWithSingleUserId(t *testing.T) {
	inputs := []struct {
		casing       Case
		userID       string
		inputByte    []byte
		expectedByte []byte
	}{
		{
			CamelCase,
			"my-another-user-id",
			[]byte("{\"userId\": \"my-user-id\", \"context-app-name\": \"my-app-name\"}\n{\"userId\": \"my-another-user-id\"}\n"),
			[]byte("{\"userId\": \"my-user-id\", \"context-app-name\": \"my-app-name\"}\n"),
		},
		{
			CamelCase,
			"my-user-id",
			[]byte("{\"userId\": \"my-user-id\"}\n"),
			[]byte(""),
		},
		{
			CamelCase,
			"valid-user-id",
			[]byte("{\"userId\": \"invalid-user-id\"}\n"),
			[]byte("{\"userId\": \"invalid-user-id\"}\n"),
		},
		{
			SnakeCase,
			"my-another-user-id",
			[]byte("{\"user_id\": \"my-user-id\", \"context-app-name\": \"my-app-name\"}\n{\"user_id\": \"my-another-user-id\"}\n"),
			[]byte("{\"user_id\": \"my-user-id\", \"context-app-name\": \"my-app-name\"}\n"),
		},
		{
			SnakeCase,
			"my-user-id",
			[]byte("{\"user_id\": \"my-user-id\"}\n"),
			[]byte(""),
		},
		{
			SnakeCase,
			"valid-user-id",
			[]byte("{\"user_id\": \"invalid-user-id\"}\n"),
			[]byte("{\"user_id\": \"invalid-user-id\"}\n"),
		},
		{
			SnakeCase,
			"my-.-user-id",
			[]byte("{\"user_id\": \"my-.-user-id\"}\n"),
			[]byte(""), // This change uses regex variables in matched string
		},
		{
			SnakeCase,
			"",
			[]byte("{\"user_id\": \"my-^-user-id\"}\n"),
			[]byte("{\"user_id\": \"my-^-user-id\"}\n"),
		},
	}

	for _, ip := range inputs {
		h := NewGZIPLocalFileHandler(ip.casing, false, nil)

		h.records = ip.inputByte
		err := h.RemoveIdentity(context.TODO(), []model.User{{ID: ip.userID}})
		require.Nil(t, err)
		require.Equal(t, string(h.records), string(ip.expectedByte))
	}
}

func TestRemoveIdentityRecordsFromGZIPByMultipleUserId(t *testing.T) {
	inputs := []struct {
		casing       Case
		userIds      []model.User
		inputByte    []byte
		expectedByte []byte
	}{
		{
			casing: SnakeCase,
			userIds: []model.User{
				{ID: "user-id-1"},
				{ID: "user-id-3"},
			},
			inputByte:    []byte("{\"user_id\": \"user-id-1\"}\n{\"user_id\": \"user-id-2\"}\n{\"user_id\": \"user-id-3\"}\n"),
			expectedByte: []byte("{\"user_id\": \"user-id-2\"}\n"),
		},

		{
			casing: CamelCase,
			userIds: []model.User{
				{ID: "user-id-1"},
				{ID: "user-id-3"},
			},
			inputByte:    []byte("{\"userId\": \"user-id-1\"}\n{\"userId\": \"user-id-2\"}\n{\"userId\": \"user-id-3\"}\n"),
			expectedByte: []byte("{\"userId\": \"user-id-2\"}\n"),
		},
	}

	for _, ip := range inputs {

		h := NewGZIPLocalFileHandler(ip.casing, false, nil)
		h.records = ip.inputByte
		err := h.RemoveIdentity(context.TODO(), ip.userIds)
		require.Nil(t, err)
		require.Equal(t, true, bytes.Equal(h.records, ip.expectedByte))
	}
}

func TestIdentityDeletePattern(t *testing.T) {
	inputs := []struct {
		casing          Case
		userId          string
		expectedPattern string
	}{
		{
			CamelCase,
			"my-user-id",
			"'/\"userId\": *\"my-user-id\"/d'",
		},
		{
			CamelCase,
			"!@#$%^&*()***",
			"'/\"userId\": *\"!@#\\$%\\^&\\*\\(\\)\\*\\*\\*\"/d'",
		},
		{
			SnakeCase,
			"my-user-id",
			"'/\"user_id\": *\"my-user-id\"/d'",
		},
		{
			SnakeCase,
			"my-^-user-id",
			"'/\"user_id\": *\"my-\\^-user-id\"/d'",
		},
		{
			SnakeCase,
			"!@#$%^&*()***",
			"'/\"user_id\": *\"!@#\\$%\\^&\\*\\(\\)\\*\\*\\*\"/d'",
		},
	}

	for _, ip := range inputs {
		h := NewGZIPLocalFileHandler(ip.casing, false, nil)
		actualPattern, err := h.getDeletePattern(model.User{ID: ip.userId})
		require.Nil(t, err)
		require.Equal(t, ip.expectedPattern, actualPattern)
	}
}

func TestIdentityRemovalProcessSucceeds(t *testing.T) {
	ctx := context.TODO()

	manager := NewGZIPLocalFileHandler(SnakeCase, false, nil)
	inputFile := "testdata/test_tracks.json.gz"
	actualOutputFile := "testdata/actual_test_tracks_filtered.json.gz"
	expectedOutputFile := "testdata/expected_test_tracks_filtered.json.gz"

	err := manager.Read(ctx, inputFile)
	require.Nil(t, err)

	err = manager.RemoveIdentity(ctx, []model.User{{ID: "68108b4d-245f-4aba-b240-8fb107c9d7b2"}})
	require.Nil(t, err)

	err = manager.Write(ctx, actualOutputFile)
	require.Nil(t, err)

	defer cleanupFile(actualOutputFile)

	same, err := sameFiles(expectedOutputFile, actualOutputFile)
	require.Nil(t, err)
	require.True(t, same)
}

func TestRemoveIdentityNativeSingleUser(t *testing.T) {
	inputs := []struct {
		name         string
		casing       Case
		idFieldPath  []string
		userID       string
		inputByte    []byte
		expectedByte []byte
	}{
		{
			name:         "CamelCase top-level userId match",
			casing:       CamelCase,
			idFieldPath:  []string{"userId"},
			userID:       "my-user-id",
			inputByte:    []byte("{\"userId\": \"my-user-id\"}\n"),
			expectedByte: []byte(""),
		},
		{
			name:         "CamelCase top-level userId no match",
			casing:       CamelCase,
			idFieldPath:  []string{"userId"},
			userID:       "other-id",
			inputByte:    []byte("{\"userId\": \"my-user-id\"}\n"),
			expectedByte: []byte("{\"userId\": \"my-user-id\"}\n"),
		},
		{
			name:         "CamelCase multiple lines removes matching",
			casing:       CamelCase,
			idFieldPath:  []string{"userId"},
			userID:       "my-another-user-id",
			inputByte:    []byte("{\"userId\": \"my-user-id\", \"context-app-name\": \"my-app-name\"}\n{\"userId\": \"my-another-user-id\"}\n"),
			expectedByte: []byte("{\"userId\": \"my-user-id\", \"context-app-name\": \"my-app-name\"}\n"),
		},
		{
			name:         "CamelCase special characters in userId",
			casing:       CamelCase,
			idFieldPath:  []string{"userId"},
			userID:       "!@#$%^&*()",
			inputByte:    []byte("{\"userId\": \"!@#$%^&*()\"}\n"),
			expectedByte: []byte(""),
		},
		{
			name:         "SnakeCase nested data.user_id match",
			casing:       SnakeCase,
			idFieldPath:  []string{"data", "user_id"},
			userID:       "my-user-id",
			inputByte:    []byte("{\"data\": {\"user_id\": \"my-user-id\"}}\n"),
			expectedByte: []byte(""),
		},
		{
			name:         "SnakeCase nested data.user_id no match",
			casing:       SnakeCase,
			idFieldPath:  []string{"data", "user_id"},
			userID:       "other-id",
			inputByte:    []byte("{\"data\": {\"user_id\": \"my-user-id\"}}\n"),
			expectedByte: []byte("{\"data\": {\"user_id\": \"my-user-id\"}}\n"),
		},
		{
			name:         "SnakeCase multiple lines removes matching",
			casing:       SnakeCase,
			idFieldPath:  []string{"data", "user_id"},
			userID:       "user-id-2",
			inputByte:    []byte("{\"data\": {\"user_id\": \"user-id-1\"}}\n{\"data\": {\"user_id\": \"user-id-2\"}}\n"),
			expectedByte: []byte("{\"data\": {\"user_id\": \"user-id-1\"}}\n"),
		},
	}

	for _, ip := range inputs {
		t.Run(ip.name, func(t *testing.T) {
			h := NewGZIPLocalFileHandler(ip.casing, true, ip.idFieldPath)
			h.records = ip.inputByte
			err := h.RemoveIdentity(context.TODO(), []model.User{{ID: ip.userID}})
			require.NoError(t, err)
			require.Equal(t, string(ip.expectedByte), string(h.records))
		})
	}
}

func TestRemoveIdentityNativeMultipleUsers(t *testing.T) {
	inputs := []struct {
		name         string
		casing       Case
		idFieldPath  []string
		userIds      []model.User
		inputByte    []byte
		expectedByte []byte
	}{
		{
			name:        "CamelCase remove multiple users",
			casing:      CamelCase,
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
			casing:      SnakeCase,
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
			h := NewGZIPLocalFileHandler(ip.casing, true, ip.idFieldPath)
			h.records = ip.inputByte
			err := h.RemoveIdentity(context.TODO(), ip.userIds)
			require.NoError(t, err)
			require.Equal(t, string(ip.expectedByte), string(h.records))
		})
	}
}
