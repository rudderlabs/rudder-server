package bingads

import (
	"fmt"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	offlineconversions "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/offline-conversions"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/testhelper/rudderauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	offlineConversionsDestName           = "BING_ADS_OFFLINE_CONVERSIONS"
	offlineConversionsCredsEnvVar        = "BINGADS_OFFLINE_CONVERSIONS_INTEGRATION_TEST_CREDENTIALS"
	offlineConversionsRefreshDestination = "bingads_offline_conversions"

	offlineConversionGoalName = "Qualified Lead"
)

func TestBingAdsOfflineConversionsIntegration(t *testing.T) {
	creds := getOfflineConversionsCredentials(t)
	misc.Init()

	secret := resolveRudderAuthSecret(t, *creds, offlineConversionsRefreshDestination)

	now := time.Now().UTC()
	conversionTime := now.Add(-1 * time.Hour).Format(time.RFC3339)
	adjustedConversionTime := now.Add(-30 * time.Minute).Format(time.RFC3339)

	testCases := []struct {
		name            string
		action          string
		isHashRequired  bool
		fields          map[string]any
		wantAbortReason string
	}{
		{
			name:           "insert pre-hashed email/phone (isHashRequired=false)",
			action:         "insert",
			isHashRequired: false,
			fields: map[string]any{
				"conversionTime":         conversionTime,
				"conversionValue":        "100",
				"conversionCurrencyCode": "USD",
				"email":                  "973dfe463ec85785f5f95af5ba3906eedb2d931c24e69824a89ea65dba4e813b",
				"phone":                  "8a59780bb8cd2ba022bfa5ba2ea3b6e07af17a7d8b30c1f9b3390e36f69019e4",
			},
		},
		{
			name:           "insert raw email/phone (isHashRequired=true)",
			action:         "insert",
			isHashRequired: true,
			fields: map[string]any{
				"conversionTime":         conversionTime,
				"conversionValue":        "100",
				"conversionCurrencyCode": "USD",
				"email":                  "test@example.com",
				"phone":                  "+15551234567",
			},
		},
		{
			name:           "update Restate raw email/phone (isHashRequired=true)",
			action:         "update",
			isHashRequired: true,
			fields: map[string]any{
				"conversionTime":         conversionTime,
				"adjustedConversionTime": adjustedConversionTime,
				"conversionValue":        "150",
				"conversionCurrencyCode": "USD",
				"email":                  "test@example.com",
				"phone":                  "+15551234567",
			},
		},
		{
			name:           "delete Retract raw email/phone (isHashRequired=true)",
			action:         "delete",
			isHashRequired: true,
			fields: map[string]any{
				"conversionTime":         conversionTime,
				"adjustedConversionTime": adjustedConversionTime,
				"email":                  "test@example.com",
				"phone":                  "+15551234567",
			},
		},
		{
			name:            "insert invalid conversion name -> OfflineConversionNameInvalid",
			action:          "insert",
			isHashRequired:  true,
			wantAbortReason: "OfflineConversionNameInvalid",
			fields: map[string]any{
				"conversionName":         "NonExistentGoalName",
				"conversionTime":         conversionTime,
				"conversionValue":        "100",
				"conversionCurrencyCode": "USD",
				"email":                  "test@example.com",
				"phone":                  "+15551234567",
			},
		},
		{
			name:            "insert invalid msclkid -> OfflineConversionMicrosoftClickIdInvalid",
			action:          "insert",
			isHashRequired:  false,
			wantAbortReason: "OfflineConversionMicrosoftClickIdInvalid",
			fields: map[string]any{
				"conversionTime":         conversionTime,
				"conversionValue":        "100",
				"conversionCurrencyCode": "USD",
				"microsoftClickId":       "invalid-click-id",
			},
		},
	}

	const eventsPerCase = 5
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("action=%s isHashRequired=%v events=%d", tc.action, tc.isHashRequired, eventsPerCase)
			uploader := newOfflineConversionsUploader(t, creds, secret, tc.isHashRequired)
			destination := newOfflineConversionsDestination(creds)

			jobs := make([]*jobsdb.JobT, 0, eventsPerCase)
			var abortedJobIDs, succeededJobIDs []int64
			for jobID := int64(1); jobID <= eventsPerCase; jobID++ {
				invalid := tc.wantAbortReason != "" && jobID%2 == 1

				var fields map[string]any
				if tc.wantAbortReason != "" && !invalid {
					fields = validConversionFields(conversionTime, conversionEmail(jobID, tc.isHashRequired))
				} else {
					fields = lo.Assign(tc.fields)
					if _, ok := fields["email"]; ok {
						fields["email"] = conversionEmail(jobID, tc.isHashRequired)
					}
				}

				if invalid {
					abortedJobIDs = append(abortedJobIDs, jobID)
				} else {
					succeededJobIDs = append(succeededJobIDs, jobID)
				}
				t.Logf("job=%d action=%s invalid=%v", jobID, tc.action, invalid)
				jobs = append(jobs, newConversionJob(t, jobID, tc.action, fields))
			}

			pollResponse, ids := runUploadAndPoll(t, uploader, destination, jobs)
			if tc.wantAbortReason == "" {
				requireImportSucceeded(t, pollResponse, len(ids))
				return
			}

			meta := fetchUploadStats(t, uploader, pollResponse, ids)
			require.ElementsMatch(t, abortedJobIDs, meta.AbortedKeys, "unexpected set of aborted jobs")
			require.ElementsMatch(t, succeededJobIDs, meta.SucceededKeys, "unexpected set of succeeded jobs")
			for _, jobID := range abortedJobIDs {
				require.Equalf(t, tc.wantAbortReason, meta.AbortedReasons[jobID],
					"unexpected abort reason for job %d", jobID)
			}
		})
	}
}

func getOfflineConversionsCredentials(t *testing.T) *bingAdsCredentials {
	t.Helper()

	raw := requireIntegrationEnv(t, offlineConversionsCredsEnvVar)

	var creds bingAdsCredentials
	require.NoErrorf(t, jsonrs.Unmarshal([]byte(raw), &creds), "unmarshalling %s", offlineConversionsCredsEnvVar)

	requireCredentialFields(t, offlineConversionsCredsEnvVar, map[string]string{
		"clientId":          creds.ClientID,
		"clientSecret":      creds.ClientSecret,
		"developerToken":    creds.DeveloperToken,
		"refreshToken":      creds.RefreshToken,
		"customerAccountId": creds.CustomerAccountID,
		"customerId":        creds.CustomerID,
	})
	t.Log("Loaded offline conversions credentials")
	return &creds
}

func newOfflineConversionsDestination(creds *bingAdsCredentials) *backendconfig.DestinationT {
	return &backendconfig.DestinationT{
		Name: "BingAds",
		Config: map[string]any{
			"customerAccountId": creds.CustomerAccountID,
			"customerId":        creds.CustomerID,
		},
		WorkspaceID: "integration_test_workspace",
	}
}

func newOfflineConversionsUploader(t *testing.T, creds *bingAdsCredentials, secret rudderauth.Secret, isHashRequired bool) *offlineconversions.BingAdsBulkUploader {
	t.Helper()
	t.Logf("Creating offline conversions uploader (isHashRequired=%v)", isHashRequired)

	return offlineconversions.NewBingAdsBulkUploader(
		logger.NOP, stats.NOP, offlineConversionsDestName,
		newBulkService(*creds, secret), isHashRequired,
	)
}

func conversionEmail(jobID int64, isHashRequired bool) string {
	email := fmt.Sprintf("offline-%d@example.com", jobID)
	if isHashRequired {
		return email
	}
	return hashEmail(email)
}

func validConversionFields(conversionTime, email string) map[string]any {
	return map[string]any{
		"conversionTime":         conversionTime,
		"conversionValue":        "100",
		"conversionCurrencyCode": "USD",
		"email":                  email,
	}
}

func newConversionJob(t *testing.T, jobID int64, action string, fields map[string]any) *jobsdb.JobT {
	t.Helper()

	record := lo.Assign(map[string]any{"conversionName": offlineConversionGoalName}, fields)

	payload := map[string]any{
		"type":   "record",
		"action": action,
		"fields": record,
	}
	raw, err := jsonrs.Marshal(payload)
	require.NoError(t, err)
	return &jobsdb.JobT{JobID: jobID, EventPayload: raw}
}
