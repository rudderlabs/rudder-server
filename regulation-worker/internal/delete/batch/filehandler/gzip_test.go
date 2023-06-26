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
		h := NewGZIPLocalFileHandler(ip.casing)

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
		h := NewGZIPLocalFileHandler(ip.casing)

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

		h := NewGZIPLocalFileHandler(ip.casing)
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
		h := NewGZIPLocalFileHandler(ip.casing)
		actualPattern, err := h.getDeletePattern(model.User{ID: ip.userId})
		require.Nil(t, err)
		require.Equal(t, ip.expectedPattern, actualPattern)
	}
}

func TestIdentityRemovalProcessSucceeds(t *testing.T) {
	ctx := context.TODO()

	manager := NewGZIPLocalFileHandler(SnakeCase)
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
