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

	"github.com/rudderlabs/rudder-go-kit/config"
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

func TestRestartSafeReconciliation(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPI.NewMockKlaviyoAPIService(ctrl)
	manager := &klaviyo.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP}
	jobs := []*jobsdb.JobT{statusJob(1, "chunk1", identifierHash("id", "111222334"), "chunk1:chunk2"), statusJob(2, "chunk2", identifierHash("email", "second@mail.com"), "chunk1:chunk2")}
	input := common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk1:chunk2","importCount":2}`), FailedJobParameters: "chunk1=1,0,1:chunk2=0,1,1", ImportingList: jobs}
	api.EXPECT().GetUploadErrors("chunk1").Return(errorsResponse(t, errorRow(`{"id":"111222334","anonymous_id":"unrelated"}`)), nil)
	result := manager.GetUploadStats(input)
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, []int64{1}, result.Metadata.AbortedKeys)
	require.Equal(t, []int64{2}, result.Metadata.SucceededKeys)
	require.Equal(t, "rejected", result.Metadata.AbortedReasons[1])
	for _, job := range jobs {
		require.Empty(t, job.EventPayload)
	}
}

func TestReconciliationAmbiguityAndPolicy(t *testing.T) {
	key := "BatchRouter.KLAVIYO_BULK_UPLOAD.strictResultReconciliation"
	for _, strict := range []bool{true, false} {
		t.Run(fmt.Sprint(strict), func(t *testing.T) {
			config.Set(key, strict)
			t.Cleanup(func() { config.Set(key, true) })
			cases := []struct {
				name, failed string
				jobs         []*jobsdb.JobT
				returned     []string
				want         []int64
			}{
				{"missing metadata clean", "", []*jobsdb.JobT{statusJob(1, "chunk", ""), statusJob(2, "chunk", identifierHash("email", "ok@mail.com"))}, nil, []int64{1}},
				{"unknown error", "chunk=1", []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", identifierHash("id", "2"))}, []string{errorRow(`{"id":"unknown"}`)}, []int64{1, 2}},
				{"duplicate error", "chunk=2", []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", identifierHash("id", "2"))}, []string{errorRow(`{"id":"1"}`), errorRow(`{"id":"1"}`)}, []int64{1, 2}},
				{"short failed count", "chunk=2", []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", identifierHash("id", "2"))}, []string{errorRow(`{"id":"1"}`)}, []int64{1, 2}},
				{"duplicate metadata", "chunk=1", []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", identifierHash("id", "1"))}, []string{errorRow(`{"id":"1"}`)}, []int64{1, 2}},
				{"anonymous only", "chunk=1", []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", identifierHash("id", "2"))}, []string{errorRow(`{"anonymous_id":"1"}`)}, []int64{1, 2}},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					ctrl := gomock.NewController(t)
					api := mockAPI.NewMockKlaviyoAPIService(ctrl)
					manager := &klaviyo.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP}
					if tc.failed != "" {
						api.EXPECT().GetUploadErrors("chunk").Return(errorsResponse(t, tc.returned...), nil)
					}
					counts := "chunk=0,2,2"
					if tc.failed != "" {
						count := strings.TrimPrefix(tc.failed, "chunk=")
						completed := 1
						if count == "2" {
							completed = 0
						}
						counts = fmt.Sprintf("chunk=%s,%d,2", count, completed)
						if tc.name == "short failed count" {
							counts = "chunk=2,0,2"
						}
					}
					result := manager.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk","importCount":2}`), FailedJobParameters: counts, ImportingList: tc.jobs})
					require.Equal(t, http.StatusOK, result.StatusCode)
					require.Equal(t, tc.want, result.Metadata.AbortedKeys)
					require.NotContains(t, result.Metadata.AbortedKeys, int64(0))
					for _, id := range tc.want {
						require.NotEmpty(t, result.Metadata.AbortedReasons[id])
					}
				})
			}
		})
	}
}

func TestKlaviyoPollCleanAndFailedCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPI.NewMockKlaviyoAPIService(ctrl)
	manager := &klaviyo.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP}
	var clean, failed klaviyo.PollResp
	clean.Data.Attributes.Status = "complete"
	clean.Data.Attributes.Total_count = 1
	clean.Data.Attributes.Completed_count = 1
	failed.Data.Attributes.Status = "complete"
	failed.Data.Attributes.Failed_count = 2
	failed.Data.Attributes.Total_count = 2
	api.EXPECT().GetUploadStatus("clean").Return(&clean, nil)
	api.EXPECT().GetUploadStatus("failed").Return(&failed, nil)
	result := manager.Poll(context.Background(), common.AsyncPoll{ImportId: "clean:failed"})
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.True(t, result.HasFailed)
	require.ElementsMatch(t, []string{"clean=0,1,1", "failed=2,0,2"}, strings.Split(result.FailedJobParameters, ":"))
}

func TestKlaviyoStatsTransientErrorAndIncompleteList(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPI.NewMockKlaviyoAPIService(ctrl)
	manager := &klaviyo.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP}
	input := common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk","importCount":2}`), FailedJobParameters: "chunk=1,1,2", ImportingList: []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", identifierHash("id", "2"))}}
	api.EXPECT().GetUploadErrors("chunk").Return(nil, fmt.Errorf("temporary outage"))
	result := manager.GetUploadStats(input)
	require.Equal(t, http.StatusInternalServerError, result.StatusCode)
	require.Empty(t, result.Metadata.SucceededKeys)
	input.ImportingList = input.ImportingList[:1]
	result = manager.GetUploadStats(input)
	require.Equal(t, http.StatusInternalServerError, result.StatusCode)
	require.Contains(t, result.Error, "incomplete")
}

func TestReconciliationMissingImportAndEmptyPoll(t *testing.T) {
	for _, params := range []string{`{"importId":"","importCount":2}`, `{"importId":"chunk:other","importCount":2}`} {
		manager := &klaviyo.KlaviyoBulkUploader{Logger: logger.NOP, StatsFactory: stats.NOP}
		input := common.GetUploadStatsInput{Parameters: []byte(params), ImportingList: []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", identifierHash("id", "2"))}}
		result := manager.GetUploadStats(input)
		require.Equal(t, http.StatusOK, result.StatusCode)
		require.Equal(t, []int64{1, 2}, result.Metadata.AbortedKeys)
		require.Empty(t, result.Metadata.SucceededKeys)
	}
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

func TestKlaviyoIncompleteTerminalCountsAbort(t *testing.T) {
	jobs := []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", identifierHash("id", "2"))}
	manager := &klaviyo.KlaviyoBulkUploader{Logger: logger.NOP, StatsFactory: stats.NOP}
	result := manager.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk","importCount":2}`), FailedJobParameters: "chunk=0,1,2", ImportingList: jobs})
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, []int64{1, 2}, result.Metadata.AbortedKeys)
	require.Empty(t, result.Metadata.SucceededKeys)
}

func TestKlaviyoDisabledStrictAllowsOnlyVerifiedCleanDuplicates(t *testing.T) {
	key := "BatchRouter.KLAVIYO_BULK_UPLOAD.strictResultReconciliation"
	jobs := []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("email", "same@mail.com")), statusJob(2, "chunk", identifierHash("email", "same@mail.com"))}
	for _, strict := range []bool{true, false} {
		t.Run(fmt.Sprint(strict), func(t *testing.T) {
			config.Set(key, strict)
			t.Cleanup(func() { config.Set(key, true) })
			manager := &klaviyo.KlaviyoBulkUploader{Logger: logger.NOP, StatsFactory: stats.NOP}
			result := manager.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk","importCount":2}`), FailedJobParameters: "chunk=0,2,2", ImportingList: jobs})
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.Equal(t, []int64{1, 2}, result.Metadata.SucceededKeys)
			require.Empty(t, result.Metadata.AbortedKeys)
		})
	}
}

func TestKlaviyoErrorPaginationAbortsChunk(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPI.NewMockKlaviyoAPIService(ctrl)
	manager := &klaviyo.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP}
	response := errorsResponse(t, errorRow(`{"id":"1"}`))
	response.Links.Next = "next-page"
	api.EXPECT().GetUploadErrors("chunk").Return(response, nil)
	result := manager.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk","importCount":2}`), FailedJobParameters: "chunk=1,1,2", ImportingList: []*jobsdb.JobT{statusJob(1, "chunk", identifierHash("id", "1")), statusJob(2, "chunk", identifierHash("id", "2"))}})
	require.Equal(t, []int64{1, 2}, result.Metadata.AbortedKeys)
	require.Empty(t, result.Metadata.SucceededKeys)
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
	result := manager.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk1:chunk2","importCount":2}`), FailedJobParameters: "chunk1=0,1,1:chunk2=0,1,1", ImportingList: jobs})
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
