package klaviyobulkupload_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
	mockAPI "github.com/rudderlabs/rudder-server/mocks/router/klaviyobulkupload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	klaviyo "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/klaviyobulkupload"
)

func identifierHash(kind, value string) string {
	sum := sha256.Sum256([]byte(kind + ":" + value))
	return hex.EncodeToString(sum[:])
}

func statusJob(id int64, importID, hash string, shared ...string) *jobsdb.JobT {
	allImports := importID
	if len(shared) > 0 {
		allImports = shared[0]
	}
	return &jobsdb.JobT{JobID: id, LastJobStatus: jobsdb.JobStatusT{Parameters: []byte(fmt.Sprintf(`{"importId":%q,"importCount":2,"metadata":{"identifierHash":%q,"chunkImportId":%q}}`, allImports, hash, importID))}}
}

func errorsResponse(t *testing.T, original ...string) *klaviyo.UploadStatusResp {
	t.Helper()
	var result klaviyo.UploadStatusResp
	for _, payload := range original {
		data := []byte(fmt.Sprintf(`{"data":[%s]}`, payload))
		var one klaviyo.UploadStatusResp
		require.NoError(t, jsonrs.Unmarshal(data, &one))
		result.Data = append(result.Data, one.Data...)
	}
	return &result
}

func errorRow(payload string) string {
	return fmt.Sprintf(`{"attributes":{"detail":"rejected","original_payload":%s}}`, payload)
}

func failedStatus(count int) *klaviyo.PollResp {
	var status klaviyo.PollResp
	status.Data.Attributes.Status = "complete"
	status.Data.Attributes.Failed_count = count
	return &status
}

func newStatsManager(api klaviyo.KlaviyoAPIService) *klaviyo.KlaviyoBulkUploader {
	return &klaviyo.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP}
}

func TestRestartSafeReconciliation(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPI.NewMockKlaviyoAPIService(ctrl)
	manager := newStatsManager(api)
	jobs := []*jobsdb.JobT{statusJob(1, "chunk1", identifierHash("id", "111222334"), "chunk1:chunk2"), statusJob(2, "chunk2", identifierHash("email", "second@mail.com"), "chunk1:chunk2")}
	api.EXPECT().GetUploadStatus("chunk1").Return(failedStatus(1), nil)
	api.EXPECT().GetUploadErrors("chunk1").Return(errorsResponse(t, errorRow(`{"id":"111222334","anonymous_id":"unrelated"}`)), nil)
	result := manager.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk1:chunk2","importCount":2}`), FailedJobParameters: "chunk1", ImportingList: jobs})
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, []int64{1}, result.Metadata.AbortedKeys)
	require.Equal(t, []int64{2}, result.Metadata.SucceededKeys)
	require.Equal(t, "rejected", result.Metadata.AbortedReasons[1])
}

func TestKlaviyoPartialMetadataAndDuplicateKeys(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprint(duplicate), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			api := mockAPI.NewMockKlaviyoAPIService(ctrl)
			jobs := []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", ""), statusJob(3, "chunk", identifierHash("id", "3"))}
			if duplicate {
				jobs[1] = statusJob(2, "chunk", identifierHash("id", "1"))
			}
			api.EXPECT().GetUploadStatus("chunk").Return(failedStatus(1), nil)
			api.EXPECT().GetUploadErrors("chunk").Return(errorsResponse(t, errorRow(`{"id":"1"}`)), nil)
			result := newStatsManager(api).GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk","importCount":2}`), FailedJobParameters: "chunk", ImportingList: jobs})
			require.Equal(t, []int64{1, 2}, result.Metadata.AbortedKeys)
			require.Equal(t, []int64{3}, result.Metadata.SucceededKeys)
			if duplicate {
				require.Equal(t, "rejected", result.Metadata.AbortedReasons[2])
			}
		})
	}
}

func TestKlaviyoUnmatchedOrIncompleteResultsAbortRemainder(t *testing.T) {
	for _, returned := range [][]string{{errorRow(`{"id":"unknown"}`)}, {}} {
		t.Run(fmt.Sprint(len(returned)), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			api := mockAPI.NewMockKlaviyoAPIService(ctrl)
			api.EXPECT().GetUploadStatus("chunk").Return(failedStatus(1), nil)
			api.EXPECT().GetUploadErrors("chunk").Return(errorsResponse(t, returned...), nil)
			jobs := []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", identifierHash("id", "2"))}
			result := newStatsManager(api).GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk","importCount":2}`), FailedJobParameters: "chunk", ImportingList: jobs})
			require.ElementsMatch(t, []int64{1, 2}, result.Metadata.AbortedKeys)
			require.Empty(t, result.Metadata.SucceededKeys)
		})
	}
}

func TestKlaviyoCleanPollUsesRouterSuccessPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPI.NewMockKlaviyoAPIService(ctrl)
	api.EXPECT().GetUploadStatus("chunk").Return(failedStatus(0), nil)
	result := newStatsManager(api).Poll(context.Background(), common.AsyncPoll{ImportId: "chunk"})
	require.True(t, result.Complete)
	require.False(t, result.HasFailed)
}

func TestKlaviyoUploadWithoutIdentifierOmitsCorrelationKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPI.NewMockKlaviyoAPIService(ctrl)
	manager := &klaviyo.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP, BatchSize: 1, MaxPayloadSize: 4600000, MaxAllowedProfileSize: 512000}
	api.EXPECT().UploadProfiles(gomock.Any()).DoAndReturn(func(payload klaviyo.Payload) (*klaviyo.UploadResp, error) {
		require.Len(t, payload.Data.Attributes.Profiles.Data, 1)
		resp := &klaviyo.UploadResp{}
		resp.Data.Id = "chunk"
		return resp, nil
	})
	path := filepath.Join(t.TempDir(), "upload.jsonl")
	line := `{"message":{"body":{"JSON":{"data":{"attributes":{"profiles":{"data":[{"type":"profile","attributes":{"jobIdentifier":"1:1"}}]}}}}}},"metadata":{"job_id":1}}` + "\n"
	require.NoError(t, os.WriteFile(path, []byte(line), 0o600))
	output := manager.Upload(context.Background(), &common.AsyncDestinationStruct{Destination: destination, FileName: path, ImportingJobIDs: []int64{1}})
	require.Equal(t, []int64{1}, output.ImportingJobIDs)
	require.Empty(t, output.AbortJobIDs)
	require.Empty(t, output.JobImportingParameters)
}

func TestUploadFixturePersistsOutboundIdentifiers(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPI.NewMockKlaviyoAPIService(ctrl)
	manager := &klaviyo.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP, BatchSize: 2, MaxPayloadSize: 4600000, MaxAllowedProfileSize: 512000}
	api.EXPECT().UploadProfiles(gomock.Any()).DoAndReturn(func(payload klaviyo.Payload) (*klaviyo.UploadResp, error) {
		require.Len(t, payload.Data.Attributes.Profiles.Data, 2)
		for _, profile := range payload.Data.Attributes.Profiles.Data {
			require.Empty(t, profile.Attributes.JobIdentifier)
			require.Empty(t, profile.ID)
			require.NotEmpty(t, profile.Attributes.Email)
		}
		resp := &klaviyo.UploadResp{}
		resp.Data.Id = "chunk"
		return resp, nil
	}).Times(1)
	fixture, err := os.ReadFile(filepath.Join("testdata", "uploadData.jsonl"))
	require.NoError(t, err)
	lines := []byte{}
	// The fixture's jobId field is historical; the batch-router line uses job_id.
	// Use its first two profiles to verify that anonymous_id is not an outbound key.
	for i, line := range splitFixtureLines(string(fixture))[:2] {
		var job map[string]any
		require.NoError(t, jsonrs.Unmarshal([]byte(line), &job))
		job["metadata"] = map[string]any{"job_id": i + 1}
		data, err := jsonrs.Marshal(job)
		require.NoError(t, err)
		lines = append(lines, data...)
		lines = append(lines, '\n')
	}
	path := filepath.Join(t.TempDir(), "upload.jsonl")
	require.NoError(t, os.WriteFile(path, lines, 0o600))
	out := manager.Upload(context.Background(), &common.AsyncDestinationStruct{Destination: destination, FileName: path, ImportingJobIDs: []int64{1, 2}})
	require.Equal(t, []int64{1, 2}, out.ImportingJobIDs)
	require.Len(t, out.JobImportingParameters, 2)
	require.Equal(t, identifierHash("email", "qwe22@mail.com"), gjson.GetBytes(out.JobImportingParameters[1], "identifierHash").String())
	require.Equal(t, identifierHash("email", "example@mail.com"), gjson.GetBytes(out.JobImportingParameters[2], "identifierHash").String())
	require.Equal(t, "chunk", gjson.GetBytes(out.JobImportingParameters[1], "chunkImportId").String())
	require.NotContains(t, string(out.JobImportingParameters[1]), "user1")
}

func TestUploadKeepsDuplicateOutboundIdentifiersAcrossChunks(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPI.NewMockKlaviyoAPIService(ctrl)
	manager := &klaviyo.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP, BatchSize: 1, MaxPayloadSize: 4600000, MaxAllowedProfileSize: 512000}
	for _, chunk := range []string{"chunk1", "chunk2"} {
		api.EXPECT().UploadProfiles(gomock.Any()).DoAndReturn(func(payload klaviyo.Payload) (*klaviyo.UploadResp, error) {
			require.Len(t, payload.Data.Attributes.Profiles.Data, 1)
			resp := &klaviyo.UploadResp{}
			resp.Data.Id = chunk
			return resp, nil
		})
	}
	lines := []byte{}
	for id := 1; id <= 2; id++ {
		lines = append(lines, []byte(fmt.Sprintf(`{"message":{"body":{"JSON":{"data":{"attributes":{"profiles":{"data":[{"type":"profile","attributes":{"email":"same@mail.com","jobIdentifier":"%d:%d"}}]}}}}}},"metadata":{"job_id":%d}}`+"\n", id, id, id))...)
	}
	path := filepath.Join(t.TempDir(), "duplicate.jsonl")
	require.NoError(t, os.WriteFile(path, lines, 0o600))
	out := manager.Upload(context.Background(), &common.AsyncDestinationStruct{Destination: destination, FileName: path, ImportingJobIDs: []int64{1, 2}})
	require.Empty(t, out.AbortJobIDs)
	require.Equal(t, []int64{1, 2}, out.ImportingJobIDs)
	require.Equal(t, "chunk1", gjson.GetBytes(out.JobImportingParameters[1], "chunkImportId").String())
	require.Equal(t, "chunk2", gjson.GetBytes(out.JobImportingParameters[2], "chunkImportId").String())
	jobs := []*jobsdb.JobT{statusJob(1, "chunk1", identifierHash("email", "same@mail.com"), "chunk1:chunk2"), statusJob(2, "chunk2", identifierHash("email", "same@mail.com"), "chunk1:chunk2")}
	result := manager.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk1:chunk2","importCount":2}`), ImportingList: jobs})
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, []int64{1, 2}, result.Metadata.SucceededKeys)
	require.Empty(t, result.Metadata.AbortedKeys)
}

func splitFixtureLines(s string) []string {
	var lines []string
	for line := range strings.SplitSeq(strings.TrimSpace(s), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
