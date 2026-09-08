package bingads

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/audience"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/testhelper/rudderauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	audienceDestName           = "BING_ADS"
	audienceCredsEnvVar        = "BINGADS_AUDIENCE_INTEGRATION_TEST_CREDENTIALS"
	audienceRefreshDestination = "bingads_audience"
)

func TestBingAdsAudienceIntegration(t *testing.T) {
	creds := getAudienceCredentials(t)
	misc.Init()

	secret := resolveRudderAuthSecret(t, creds.bingAdsCredentials, audienceRefreshDestination)

	const eventsPerAction = 5
	actions := []string{"Add", "Remove", "Replace"}

	audiences := []struct {
		name       string
		audienceID string
	}{
		{name: "hashed", audienceID: creds.HashedAudienceID},
		{name: "unhashed", audienceID: creds.UnhashedAudienceID},
	}

	for _, aud := range audiences {
		t.Run(aud.name, func(t *testing.T) {
			t.Logf("audience=%s clubbing %d Add/Remove/Replace events in a single import", aud.name, len(actions)*eventsPerAction)
			uploader := newAudienceUploader(t, creds, secret)
			destination := newAudienceDestination(creds, aud.audienceID)

			jobs := make([]*jobsdb.JobT, 0, len(actions)*eventsPerAction)
			var jobID int64
			for _, action := range actions {
				for range eventsPerAction {
					jobID++
					email := fmt.Sprintf("audience-%d@example.com", jobID)
					hashedEmail := hashEmail(email)
					t.Logf("job=%d action=%s email=%s hashedEmail=%s", jobID, action, email, hashedEmail)
					jobs = append(jobs, newAudienceJob(t, jobID, action, hashedEmail))
				}
			}
			pollResponse, ids := runUploadAndPoll(t, uploader, destination, jobs)
			requireImportSucceeded(t, pollResponse, len(ids))
		})
	}

	t.Run("valid and invalid emails in one import", func(t *testing.T) {
		uploader := newAudienceUploader(t, creds, secret)
		destination := newAudienceDestination(creds, creds.HashedAudienceID)

		jobs := make([]*jobsdb.JobT, 0, eventsPerAction)
		var abortedJobIDs, succeededJobIDs []int64
		for jobID := int64(1); jobID <= eventsPerAction; jobID++ {
			var email string
			if jobID%2 == 0 {
				email = hashEmail(fmt.Sprintf("audience-valid-%d@example.com", jobID))
				succeededJobIDs = append(succeededJobIDs, jobID)
				t.Logf("job=%d action=Add email=%s (valid hash, expecting success)", jobID, email)
			} else {
				email = fmt.Sprintf("audience-invalid-%d@example.com", jobID)
				abortedJobIDs = append(abortedJobIDs, jobID)
				t.Logf("job=%d action=Add email=%s (plain-text, expecting abort)", jobID, email)
			}
			jobs = append(jobs, newAudienceJob(t, jobID, "Add", email))
		}

		pollResponse, ids := runUploadAndPoll(t, uploader, destination, jobs)
		meta := fetchUploadStats(t, uploader, pollResponse, ids)
		require.ElementsMatch(t, abortedJobIDs, meta.AbortedKeys, "unexpected set of aborted jobs")
		require.ElementsMatch(t, succeededJobIDs, meta.SucceededKeys, "unexpected set of succeeded jobs")
		for _, jobID := range abortedJobIDs {
			require.Equal(t, "EmailMustBeHashed", meta.AbortedReasons[jobID], "expected an abort reason for job %d", jobID)
		}
	})
}

func hashEmail(email string) string {
	sum := sha256.Sum256([]byte(email))
	return hex.EncodeToString(sum[:])
}

func newAudienceDestination(creds *audienceCredentials, audienceID string) *backendconfig.DestinationT {
	return &backendconfig.DestinationT{
		Name: "BingAds",
		Config: map[string]any{
			"customerAccountId": creds.CustomerAccountID,
			"customerId":        creds.CustomerID,
			"audienceId":        audienceID,
		},
		WorkspaceID: "integration_test_workspace",
	}
}

type audienceCredentials struct {
	bingAdsCredentials

	HashedAudienceID   string `json:"hashedAudienceId"`
	UnhashedAudienceID string `json:"unhashedAudienceId"`
}

func getAudienceCredentials(t *testing.T) *audienceCredentials {
	t.Helper()

	raw := requireIntegrationEnv(t, audienceCredsEnvVar)

	var creds audienceCredentials
	require.NoErrorf(t, jsonrs.Unmarshal([]byte(raw), &creds), "unmarshalling %s", audienceCredsEnvVar)

	requireCredentialFields(t, audienceCredsEnvVar, map[string]string{
		"clientId":           creds.ClientID,
		"clientSecret":       creds.ClientSecret,
		"developerToken":     creds.DeveloperToken,
		"refreshToken":       creds.RefreshToken,
		"customerAccountId":  creds.CustomerAccountID,
		"customerId":         creds.CustomerID,
		"hashedAudienceId":   creds.HashedAudienceID,
		"unhashedAudienceId": creds.UnhashedAudienceID,
	})
	t.Log("Loaded audience credentials")
	return &creds
}

func newAudienceUploader(t *testing.T, creds *audienceCredentials, secret rudderauth.Secret) *audience.BingAdsBulkUploader {
	t.Helper()
	t.Log("Creating audience uploader")

	return audience.NewBingAdsBulkUploader(
		logger.NOP, stats.NOP, audienceDestName,
		newBulkService(creds.bingAdsCredentials, secret), &audience.Client{},
	)
}

func newAudienceJob(t *testing.T, jobID int64, action, hashedEmail string) *jobsdb.JobT {
	t.Helper()

	payload := map[string]any{
		"body": map[string]any{
			"JSON": map[string]any{
				"Action": action,
				"List": []map[string]any{
					{"hashedEmail": hashedEmail},
				},
			},
		},
	}
	raw, err := jsonrs.Marshal(payload)
	require.NoError(t, err)
	return &jobsdb.JobT{JobID: jobID, EventPayload: raw}
}
