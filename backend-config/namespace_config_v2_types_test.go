package backendconfig

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

const v2SamplePayload = `{
	"version": 2,
	"sourceDefinitions": {
		"webhook": {
			"id": "1wIhTgHIVDRoJXpGvyLmnGXjHl0",
			"category": "webhook",
			"type": "cloud",
			"options": {"hydration": {"enabled": true}, "sdkExecutionEnvironment": "server"},
			"config": null,
			"createdAt": "2020-12-07T14:20:58.237Z",
			"updatedAt": "2026-08-06T05:34:18.760Z"
		},
		"snowflake": {
			"id": "1lKfm47IlzXQNXD3C3pk6mZtb1S",
			"category": "warehouse",
			"type": "warehouse",
			"options": null,
			"updatedAt": "2026-08-01T05:34:18.760Z"
		}
	},
	"destinationDefinitions": {
		"WEBHOOK": {
			"id": "1aIXqM806xAVm7BwUXYh6IeM7uP",
			"displayName": "Webhook",
			"config": {"transformAtV1": "processor", "secretKeys": ["apiKey"]},
			"responseRules": null,
			"category": null,
			"updatedAt": "2026-06-17T11:23:34.585Z"
		}
	},
	"accountDefinitions": {
		"DESTINATION_SALESFORCE_OAUTH": {
			"name": "DESTINATION_SALESFORCE_OAUTH",
			"type": "salesforce",
			"category": "destination",
			"authenticationType": "oauth",
			"config": {"refreshOAuthToken": true},
			"updatedAt": "2026-03-26T03:15:41.921Z"
		}
	},
	"workspaces": {
		"workspace-1": {
			"sources": {"source-1": {"name": "My Source", "sourceDefinitionName": "webhook"}},
			"destinations": {},
			"connections": {},
			"updatedAt": "2026-07-24T17:01:55.777Z"
		},
		"workspace-2": null
	}
}`

func TestV2NamespaceConfigUnmarshal(t *testing.T) {
	var conf v2NamespaceConfig
	require.NoError(t, jsonrs.Unmarshal([]byte(v2SamplePayload), &conf))
	require.Equal(t, v2ConfigVersion, conf.Version)

	t.Run("catalogues are keyed by definition name", func(t *testing.T) {
		require.Len(t, conf.SourceDefinitions, 2)
		require.Len(t, conf.DestinationDefinitions, 1)
		require.Len(t, conf.AccountDefinitions, 1)

		webhook, ok := conf.SourceDefinitions["webhook"]
		require.True(t, ok)
		require.Equal(t, "1wIhTgHIVDRoJXpGvyLmnGXjHl0", webhook.ID)
		require.Equal(t, "webhook", webhook.Category)
		require.Equal(t, "cloud", webhook.Type)
		require.True(t, webhook.Options.Hydration.Enabled)
		require.Equal(t, "2026-08-06T05:34:18.760Z", webhook.UpdatedAt.Format(updatedAfterTimeFormat))

		// a null options object must not fail the whole catalogue
		require.False(t, conf.SourceDefinitions["snowflake"].Options.Hydration.Enabled)

		dest, ok := conf.DestinationDefinitions["WEBHOOK"]
		require.True(t, ok)
		require.Equal(t, "Webhook", dest.DisplayName)
		require.Equal(t, "processor", dest.Config["transformAtV1"])

		account, ok := conf.AccountDefinitions["DESTINATION_SALESFORCE_OAUTH"]
		require.True(t, ok)
		require.Equal(t, "oauth", account.AuthenticationType)
	})

	t.Run("source and destination definitions are named by their catalogue key alone", func(t *testing.T) {
		source := conf.SourceDefinitions["webhook"]
		require.Empty(t, source.Name, "no name on the wire")
		require.Equal(t, SourceDefinitionT{
			ID:       "1wIhTgHIVDRoJXpGvyLmnGXjHl0",
			Name:     "webhook",
			Category: "webhook",
			Type:     "cloud",
			Options:  source.Options,
		}, source.toV1("webhook"))

		destination := conf.DestinationDefinitions["WEBHOOK"]
		require.Empty(t, destination.Name, "no name on the wire")
		require.Equal(t, DestinationDefinitionT{
			ID:          "1aIXqM806xAVm7BwUXYh6IeM7uP",
			Name:        "WEBHOOK",
			DisplayName: "Webhook",
			Config:      destination.Config,
		}, destination.toV1("WEBHOOK"))

		// account definitions do carry a name, and the key stays authoritative
		account := conf.AccountDefinitions["DESTINATION_SALESFORCE_OAUTH"]
		require.Equal(t, "DESTINATION_SALESFORCE_OAUTH", account.Name)
		require.Equal(t, AccountDefinition{
			Name:               "DESTINATION_SALESFORCE_OAUTH",
			AuthenticationType: "oauth",
			Config:             account.Config,
		}, account.toV1("DESTINATION_SALESFORCE_OAUTH"))
	})

	t.Run("workspace keeps its raw body and its updatedAt", func(t *testing.T) {
		ws := conf.Workspaces["workspace-1"]
		require.NotNil(t, ws)
		require.Equal(t, "2026-07-24T17:01:55.777Z", ws.UpdatedAt.Format(updatedAfterTimeFormat))

		// the body stays untyped: the mapper, not the resolver, owns the workspace schema
		var body struct {
			Sources map[string]struct {
				Name                 string `json:"name"`
				SourceDefinitionName string `json:"sourceDefinitionName"`
			} `json:"sources"`
		}
		require.NoError(t, jsonrs.Unmarshal(ws.Raw, &body))
		require.Equal(t, "My Source", body.Sources["source-1"].Name)
		require.Equal(t, "webhook", body.Sources["source-1"].SourceDefinitionName)
	})

	t.Run("a workspace that was not updated arrives as null", func(t *testing.T) {
		ws, ok := conf.Workspaces["workspace-2"]
		require.True(t, ok, "the key must survive: it is the membership signal")
		require.Nil(t, ws)
	})

	t.Run("the raw body outlives the buffer the decoder was given", func(t *testing.T) {
		payload := []byte(`{"workspaces":{"workspace-1":{"updatedAt":"2026-07-24T17:01:55.777Z","sources":{}}}}`)
		var conf v2NamespaceConfig
		require.NoError(t, jsonrs.Unmarshal(payload, &conf))

		raw := conf.Workspaces["workspace-1"].Raw
		for i := range payload { // scribble over it: std and sonnet slice rather than copy
			payload[i] = 'x'
		}
		require.Contains(t, string(raw), `"updatedAt":"2026-07-24T17:01:55.777Z"`)
	})

	t.Run("updatedAt", func(t *testing.T) {
		t.Run("missing leaves the zero time, the resolver decides what to do about it", func(t *testing.T) {
			var conf v2NamespaceConfig
			require.NoError(t, jsonrs.Unmarshal([]byte(`{"workspaces":{"workspace-1":{"sources":{}}}}`), &conf))
			require.Equal(t, time.Time{}, conf.Workspaces["workspace-1"].UpdatedAt)
			require.NotEmpty(t, conf.Workspaces["workspace-1"].Raw)
		})

		for _, tc := range []struct {
			name, updatedAt, wantErr string
		}{
			{"not a string", `1784909557360`, "not of expected type"},
			{"not a timestamp", `"yesterday"`, "parsing workspace updatedAt"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				payload := fmt.Sprintf(`{"workspaces":{"workspace-1":{"updatedAt":%s}}}`, tc.updatedAt)
				var conf v2NamespaceConfig
				require.ErrorContains(t, jsonrs.Unmarshal([]byte(payload), &conf), tc.wantErr)
			})
		}
	})
}
