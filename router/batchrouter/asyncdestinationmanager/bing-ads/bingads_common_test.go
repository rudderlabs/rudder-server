package bingads

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"

	bingadssdk "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/testhelper/rudderauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const sessionHTTPTimeout = 60 * time.Second

type bingAdsCredentials struct {
	ClientID       string `json:"clientId"`
	ClientSecret   string `json:"clientSecret"`
	DeveloperToken string `json:"developerToken"`
	RefreshToken   string `json:"refreshToken"`

	CustomerAccountID string `json:"customerAccountId"`
	CustomerID        string `json:"customerId"`
}

func requireIntegrationEnv(t *testing.T, envVar string) string {
	t.Helper()
	raw, exists := os.LookupEnv(envVar)
	if !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", envVar)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), envVar)
	}
	return raw
}

func requireCredentialFields(t *testing.T, envVar string, fields map[string]string) {
	t.Helper()
	for field, value := range fields {
		require.NotEmptyf(t, value, "%s is required in %s", field, envVar)
	}
}

func newBulkService(creds bingAdsCredentials, secret rudderauth.Secret) bingadssdk.BulkServiceI {
	tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: secret.AccessToken()})
	session := bingadssdk.NewSession(bingadssdk.SessionConfig{
		DeveloperToken: creds.DeveloperToken,
		AccountId:      creds.CustomerAccountID,
		CustomerId:     creds.CustomerID,
		HTTPClient:     &http.Client{Timeout: sessionHTTPTimeout},
		TokenSource:    tokenSource,
	})
	return bingadssdk.NewBulkService(session)
}

func resolveRudderAuthSecret(t *testing.T, creds bingAdsCredentials, refreshDestination string) rudderauth.Secret {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Log("Starting rudder-auth container")
	rudderAuth, err := rudderauth.Setup(pool, t, rudderauth.WithEnv(map[string]string{
		"BINGADS_AUDIENCE_CLIENT_ID_DESTINATION":     creds.ClientID,
		"BINGADS_AUDIENCE_CLIENT_SECRET_DESTINATION": creds.ClientSecret,
		"BINGADS_AUDIENCE_DEVELOPER_TOKEN":           creds.DeveloperToken,
	}))
	require.NoError(t, err)
	t.Logf("rudder-auth ready at %s", rudderAuth.URL)

	secret, err := rudderAuth.RefreshToken(context.Background(), rudderauth.RefreshRequest{
		Destination:  refreshDestination,
		RefreshToken: creds.RefreshToken,
	})
	require.NoError(t, err)
	t.Log("Resolved access token from rudder-auth")
	return secret
}

func writeUploadDataFile(t *testing.T, uploader common.AsyncUploadAndTransformManager, jobs []*jobsdb.JobT) string {
	t.Helper()

	tmpDir := t.TempDir()
	t.Setenv("RUDDER_TMPDIR", tmpDir)
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, misc.RudderAsyncDestinationLogs), 0o755))

	filePath := filepath.Join(tmpDir, "uploadData.txt")
	file, err := os.Create(filePath)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()

	for _, job := range jobs {
		line, err := uploader.Transform(job)
		require.NoErrorf(t, err, "transforming job %d", job.JobID)
		_, err = fmt.Fprintln(file, line)
		require.NoError(t, err)
	}
	t.Logf("Wrote %d job(s) to upload file %s", len(jobs), filePath)
	return filePath
}

func runUpload(t *testing.T, uploader common.AsyncDestinationManager, destination *backendconfig.DestinationT, jobs []*jobsdb.JobT) (common.PollStatusResponse, []int64) {
	t.Helper()
	ctx := context.Background()

	importingJobIDs := lo.Map(jobs, func(job *jobsdb.JobT, _ int) int64 { return job.JobID })

	uploadFile := writeUploadDataFile(t, uploader, jobs)

	t.Log("Uploading bulk file")
	uploadOutput := uploader.Upload(ctx, &common.AsyncDestinationStruct{
		ImportingJobIDs: importingJobIDs,
		FailedJobIDs:    []int64{},
		FileName:        uploadFile,
		Destination:     destination,
		Manager:         uploader,
	})
	require.Zerof(t, uploadOutput.FailedCount, "upload reported failures: %s", uploadOutput.FailedReason)
	require.Equal(t, len(importingJobIDs), uploadOutput.ImportingCount)
	require.NotEmpty(t, uploadOutput.ImportingParameters)

	var importParameters common.ImportParameters
	require.NoError(t, jsonrs.Unmarshal(uploadOutput.ImportingParameters, &importParameters))
	importID, ok := importParameters.ImportId.(string)
	require.Truef(t, ok, "import id is not a string: %v", importParameters.ImportId)
	require.NotEmpty(t, importID, "empty import id returned from Upload")
	t.Logf("Import id: %s", importID)

	t.Log("Polling until the import reaches a terminal state")
	var pollResponse common.PollStatusResponse
	require.Eventually(t, func() bool {
		pollResponse = uploader.Poll(ctx, common.AsyncPoll{ImportId: importID})
		return pollResponse.Complete || pollResponse.HasFailed
	}, 5*time.Minute, 15*time.Second)

	require.Equalf(t, http.StatusOK, pollResponse.StatusCode, "poll returned non-200: %+v", pollResponse)
	require.Truef(t, pollResponse.Complete, "import did not complete: %+v", pollResponse)
	t.Logf("Import terminal: complete=%v hasFailed=%v", pollResponse.Complete, pollResponse.HasFailed)

	return pollResponse, importingJobIDs
}

func requireImportSucceeded(t *testing.T, pollResponse common.PollStatusResponse, jobCount int) {
	t.Helper()
	require.Falsef(t, pollResponse.HasFailed,
		"expected all jobs to succeed, but import completed with errors: %+v", pollResponse)
	require.Falsef(t, pollResponse.HasWarning,
		"expected a clean success, but import completed with warnings: %+v", pollResponse)
	t.Logf("All %d job(s) succeeded", jobCount)
}

func fetchUploadStats(t *testing.T, uploader common.AsyncDestinationManager, pollResponse common.PollStatusResponse, importingJobIDs []int64) common.EventStatMeta {
	t.Helper()
	require.Truef(t, pollResponse.HasFailed,
		"expected import to complete with errors, got %+v", pollResponse)
	require.NotEmptyf(t, pollResponse.FailedJobParameters,
		"expected a result file for the failed import, got %+v", pollResponse)

	importingList := lo.Map(importingJobIDs, func(jobID int64, _ int) *jobsdb.JobT {
		return &jobsdb.JobT{JobID: jobID}
	})
	uploadStats := uploader.GetUploadStats(common.GetUploadStatsInput{
		FailedJobParameters: pollResponse.FailedJobParameters,
		ImportingList:       importingList,
	})
	require.Equal(t, http.StatusOK, uploadStats.StatusCode)
	t.Logf("upload result: succeeded=%v aborted=%v reasons=%v",
		uploadStats.Metadata.SucceededKeys, uploadStats.Metadata.AbortedKeys, uploadStats.Metadata.AbortedReasons)
	return uploadStats.Metadata
}
