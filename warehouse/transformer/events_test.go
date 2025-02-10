package transformer

import (
	"net/http"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/testhelper"
)

func TestEvents(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	transformerResource, err := transformertest.Setup(pool, t)
	require.NoError(t, err)

	identifyDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"anonymous_id":             "anonymousId",
				"channel":                  "web",
				"context_destination_id":   "destinationID",
				"context_destination_type": "POSTGRES",
				"context_ip":               "1.2.3.4",
				"context_passed_ip":        "1.2.3.4",
				"context_request_ip":       "5.6.7.8",
				"context_source_id":        "sourceID",
				"context_source_type":      "sourceType",
				"context_traits_email":     "rhedricks@example.com",
				"context_traits_logins":    float64(2),
				"context_traits_name":      "Richard Hendricks",
				"email":                    "rhedricks@example.com",
				"id":                       "messageId",
				"logins":                   float64(2),
				"name":                     "Richard Hendricks",
				"original_timestamp":       "2021-09-01T00:00:00.000Z",
				"product_id":               "9578257311",
				"rating":                   3.0,
				"received_at":              "2021-09-01T00:00:00.000Z",
				"review_body":              "OK for the price. It works but the material feels flimsy.",
				"review_id":                "86ac1cd43",
				"sent_at":                  "2021-09-01T00:00:00.000Z",
				"timestamp":                "2021-09-01T00:00:00.000Z",
				"user_id":                  "userId",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"anonymous_id":             "string",
					"channel":                  "string",
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_ip":               "string",
					"context_passed_ip":        "string",
					"context_request_ip":       "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"context_traits_email":     "string",
					"context_traits_logins":    "int",
					"context_traits_name":      "string",
					"email":                    "string",
					"id":                       "string",
					"logins":                   "int",
					"name":                     "string",
					"original_timestamp":       "datetime",
					"product_id":               "string",
					"rating":                   "int",
					"received_at":              "datetime",
					"review_body":              "string",
					"review_id":                "string",
					"sent_at":                  "datetime",
					"timestamp":                "datetime",
					"user_id":                  "string",
					"uuid_ts":                  "datetime",
				},
				"receivedAt": "2021-09-01T00:00:00.000Z",
				"table":      "identifies",
			},
			"userId": "",
		}
	}
	userDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"context_destination_id":   "destinationID",
				"context_destination_type": "POSTGRES",
				"context_ip":               "1.2.3.4",
				"context_passed_ip":        "1.2.3.4",
				"context_request_ip":       "5.6.7.8",
				"context_source_id":        "sourceID",
				"context_source_type":      "sourceType",
				"context_traits_email":     "rhedricks@example.com",
				"context_traits_logins":    float64(2),
				"context_traits_name":      "Richard Hendricks",
				"email":                    "rhedricks@example.com",
				"id":                       "userId",
				"logins":                   float64(2),
				"name":                     "Richard Hendricks",
				"original_timestamp":       "2021-09-01T00:00:00.000Z",
				"product_id":               "9578257311",
				"rating":                   3.0,
				"received_at":              "2021-09-01T00:00:00.000Z",
				"review_body":              "OK for the price. It works but the material feels flimsy.",
				"review_id":                "86ac1cd43",
				"sent_at":                  "2021-09-01T00:00:00.000Z",
				"timestamp":                "2021-09-01T00:00:00.000Z",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_ip":               "string",
					"context_passed_ip":        "string",
					"context_request_ip":       "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"context_traits_email":     "string",
					"context_traits_logins":    "int",
					"context_traits_name":      "string",
					"email":                    "string",
					"id":                       "string",
					"logins":                   "int",
					"name":                     "string",
					"original_timestamp":       "datetime",
					"product_id":               "string",
					"rating":                   "int",
					"received_at":              "datetime",
					"review_body":              "string",
					"review_id":                "string",
					"sent_at":                  "datetime",
					"timestamp":                "datetime",
					"uuid_ts":                  "datetime",
				},
				"receivedAt": "2021-09-01T00:00:00.000Z",
				"table":      "users",
			},
			"userId": "",
		}
	}
	identifyDefaultMergeOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"merge_property_1_type":  "anonymous_id",
				"merge_property_1_value": "anonymousId",
				"merge_property_2_type":  "user_id",
				"merge_property_2_value": "userId",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"merge_property_1_type":  "string",
					"merge_property_1_value": "string",
					"merge_property_2_type":  "string",
					"merge_property_2_value": "string",
				},
				"isMergeRule":  true,
				"mergePropOne": "anonymousId",
				"mergePropTwo": "userId",
				"receivedAt":   "2021-09-01T00:00:00.000Z",
				"table":        "rudder_identity_merge_rules",
			},
			"userId": "",
		}
	}
	aliasDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"anonymous_id":             "anonymousId",
				"channel":                  "web",
				"context_ip":               "1.2.3.4",
				"context_passed_ip":        "1.2.3.4",
				"context_request_ip":       "5.6.7.8",
				"context_traits_email":     "rhedricks@example.com",
				"context_traits_logins":    float64(2),
				"id":                       "messageId",
				"original_timestamp":       "2021-09-01T00:00:00.000Z",
				"received_at":              "2021-09-01T00:00:00.000Z",
				"sent_at":                  "2021-09-01T00:00:00.000Z",
				"timestamp":                "2021-09-01T00:00:00.000Z",
				"title":                    "Home | RudderStack",
				"url":                      "https://www.rudderstack.com",
				"user_id":                  "userId",
				"previous_id":              "previousId",
				"context_destination_id":   "destinationID",
				"context_destination_type": "POSTGRES",
				"context_source_id":        "sourceID",
				"context_source_type":      "sourceType",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"anonymous_id":             "string",
					"channel":                  "string",
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"context_ip":               "string",
					"context_passed_ip":        "string",
					"context_request_ip":       "string",
					"context_traits_email":     "string",
					"context_traits_logins":    "int",
					"id":                       "string",
					"original_timestamp":       "datetime",
					"received_at":              "datetime",
					"sent_at":                  "datetime",
					"timestamp":                "datetime",
					"title":                    "string",
					"url":                      "string",
					"user_id":                  "string",
					"previous_id":              "string",
					"uuid_ts":                  "datetime",
				},
				"receivedAt": "2021-09-01T00:00:00.000Z",
				"table":      "aliases",
			},
			"userId": "",
		}
	}
	aliasMergeDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"merge_property_1_type":  "user_id",
				"merge_property_1_value": "userId",
				"merge_property_2_type":  "user_id",
				"merge_property_2_value": "previousId",
			},
			"metadata": map[string]any{
				"table":        "rudder_identity_merge_rules",
				"columns":      map[string]any{"merge_property_1_type": "string", "merge_property_1_value": "string", "merge_property_2_type": "string", "merge_property_2_value": "string"},
				"isMergeRule":  true,
				"receivedAt":   "2021-09-01T00:00:00.000Z",
				"mergePropOne": "userId",
				"mergePropTwo": "previousId",
			},
			"userId": "",
		}
	}
	extractDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"name":                     "Home",
				"context_ip":               "1.2.3.4",
				"context_traits_email":     "rhedricks@example.com",
				"context_traits_logins":    float64(2),
				"context_traits_name":      "Richard Hendricks",
				"id":                       "recordID",
				"event":                    "event",
				"received_at":              "2021-09-01T00:00:00.000Z",
				"title":                    "Home | RudderStack",
				"url":                      "https://www.rudderstack.com",
				"context_destination_id":   "destinationID",
				"context_destination_type": "POSTGRES",
				"context_source_id":        "sourceID",
				"context_source_type":      "sourceType",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"name":                     "string",
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"context_ip":               "string",
					"context_traits_email":     "string",
					"context_traits_logins":    "int",
					"context_traits_name":      "string",
					"id":                       "string",
					"event":                    "string",
					"received_at":              "datetime",
					"title":                    "string",
					"url":                      "string",
					"uuid_ts":                  "datetime",
				},
				"receivedAt": "2021-09-01T00:00:00.000Z",
				"table":      "event",
			},
			"userId": "",
		}
	}
	pageDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"name":                     "Home",
				"anonymous_id":             "anonymousId",
				"channel":                  "web",
				"context_ip":               "1.2.3.4",
				"context_passed_ip":        "1.2.3.4",
				"context_request_ip":       "5.6.7.8",
				"context_traits_email":     "rhedricks@example.com",
				"context_traits_logins":    float64(2),
				"context_traits_name":      "Richard Hendricks",
				"id":                       "messageId",
				"original_timestamp":       "2021-09-01T00:00:00.000Z",
				"received_at":              "2021-09-01T00:00:00.000Z",
				"sent_at":                  "2021-09-01T00:00:00.000Z",
				"timestamp":                "2021-09-01T00:00:00.000Z",
				"title":                    "Home | RudderStack",
				"url":                      "https://www.rudderstack.com",
				"user_id":                  "userId",
				"context_destination_id":   "destinationID",
				"context_destination_type": "POSTGRES",
				"context_source_id":        "sourceID",
				"context_source_type":      "sourceType",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"name":                     "string",
					"anonymous_id":             "string",
					"channel":                  "string",
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"context_ip":               "string",
					"context_passed_ip":        "string",
					"context_request_ip":       "string",
					"context_traits_email":     "string",
					"context_traits_logins":    "int",
					"context_traits_name":      "string",
					"id":                       "string",
					"original_timestamp":       "datetime",
					"received_at":              "datetime",
					"sent_at":                  "datetime",
					"timestamp":                "datetime",
					"title":                    "string",
					"url":                      "string",
					"user_id":                  "string",
					"uuid_ts":                  "datetime",
				},
				"receivedAt": "2021-09-01T00:00:00.000Z",
				"table":      "pages",
			},
			"userId": "",
		}
	}
	pageMergeDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"merge_property_1_type":  "anonymous_id",
				"merge_property_1_value": "anonymousId",
				"merge_property_2_type":  "user_id",
				"merge_property_2_value": "userId",
			},
			"metadata": map[string]any{
				"table":        "rudder_identity_merge_rules",
				"columns":      map[string]any{"merge_property_1_type": "string", "merge_property_1_value": "string", "merge_property_2_type": "string", "merge_property_2_value": "string"},
				"isMergeRule":  true,
				"receivedAt":   "2021-09-01T00:00:00.000Z",
				"mergePropOne": "anonymousId",
				"mergePropTwo": "userId",
			},
			"userId": "",
		}
	}
	screenDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"name":                     "Main",
				"anonymous_id":             "anonymousId",
				"channel":                  "web",
				"context_ip":               "1.2.3.4",
				"context_passed_ip":        "1.2.3.4",
				"context_request_ip":       "5.6.7.8",
				"context_traits_email":     "rhedricks@example.com",
				"context_traits_logins":    float64(2),
				"context_traits_name":      "Richard Hendricks",
				"id":                       "messageId",
				"original_timestamp":       "2021-09-01T00:00:00.000Z",
				"received_at":              "2021-09-01T00:00:00.000Z",
				"sent_at":                  "2021-09-01T00:00:00.000Z",
				"timestamp":                "2021-09-01T00:00:00.000Z",
				"title":                    "Home | RudderStack",
				"url":                      "https://www.rudderstack.com",
				"user_id":                  "userId",
				"context_destination_id":   "destinationID",
				"context_destination_type": "POSTGRES",
				"context_source_id":        "sourceID",
				"context_source_type":      "sourceType",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"name":                     "string",
					"anonymous_id":             "string",
					"channel":                  "string",
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"context_ip":               "string",
					"context_passed_ip":        "string",
					"context_request_ip":       "string",
					"context_traits_email":     "string",
					"context_traits_logins":    "int",
					"context_traits_name":      "string",
					"id":                       "string",
					"original_timestamp":       "datetime",
					"received_at":              "datetime",
					"sent_at":                  "datetime",
					"timestamp":                "datetime",
					"title":                    "string",
					"url":                      "string",
					"user_id":                  "string",
					"uuid_ts":                  "datetime",
				},
				"receivedAt": "2021-09-01T00:00:00.000Z",
				"table":      "screens",
			},
			"userId": "",
		}
	}
	screenMergeDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"merge_property_1_type":  "anonymous_id",
				"merge_property_1_value": "anonymousId",
				"merge_property_2_type":  "user_id",
				"merge_property_2_value": "userId",
			},
			"metadata": map[string]any{
				"table":        "rudder_identity_merge_rules",
				"columns":      map[string]any{"merge_property_1_type": "string", "merge_property_1_value": "string", "merge_property_2_type": "string", "merge_property_2_value": "string"},
				"isMergeRule":  true,
				"receivedAt":   "2021-09-01T00:00:00.000Z",
				"mergePropOne": "anonymousId",
				"mergePropTwo": "userId",
			},
			"userId": "",
		}
	}
	groupDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"anonymous_id":             "anonymousId",
				"channel":                  "web",
				"context_ip":               "1.2.3.4",
				"context_passed_ip":        "1.2.3.4",
				"context_request_ip":       "5.6.7.8",
				"context_traits_email":     "rhedricks@example.com",
				"context_traits_logins":    float64(2),
				"id":                       "messageId",
				"original_timestamp":       "2021-09-01T00:00:00.000Z",
				"received_at":              "2021-09-01T00:00:00.000Z",
				"sent_at":                  "2021-09-01T00:00:00.000Z",
				"timestamp":                "2021-09-01T00:00:00.000Z",
				"title":                    "Home | RudderStack",
				"url":                      "https://www.rudderstack.com",
				"user_id":                  "userId",
				"group_id":                 "groupId",
				"context_destination_id":   "destinationID",
				"context_destination_type": "POSTGRES",
				"context_source_id":        "sourceID",
				"context_source_type":      "sourceType",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"anonymous_id":             "string",
					"channel":                  "string",
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"context_ip":               "string",
					"context_passed_ip":        "string",
					"context_request_ip":       "string",
					"context_traits_email":     "string",
					"context_traits_logins":    "int",
					"id":                       "string",
					"original_timestamp":       "datetime",
					"received_at":              "datetime",
					"sent_at":                  "datetime",
					"timestamp":                "datetime",
					"title":                    "string",
					"url":                      "string",
					"user_id":                  "string",
					"group_id":                 "string",
					"uuid_ts":                  "datetime",
				},
				"receivedAt": "2021-09-01T00:00:00.000Z",
				"table":      "groups",
			},
			"userId": "",
		}
	}
	groupMergeDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"merge_property_1_type":  "anonymous_id",
				"merge_property_1_value": "anonymousId",
				"merge_property_2_type":  "user_id",
				"merge_property_2_value": "userId",
			},
			"metadata": map[string]any{
				"table":        "rudder_identity_merge_rules",
				"columns":      map[string]any{"merge_property_1_type": "string", "merge_property_1_value": "string", "merge_property_2_type": "string", "merge_property_2_value": "string"},
				"isMergeRule":  true,
				"receivedAt":   "2021-09-01T00:00:00.000Z",
				"mergePropOne": "anonymousId",
				"mergePropTwo": "userId",
			},
			"userId": "",
		}
	}
	trackDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"anonymous_id":             "anonymousId",
				"channel":                  "web",
				"context_destination_id":   "destinationID",
				"context_destination_type": "POSTGRES",
				"context_ip":               "1.2.3.4",
				"context_passed_ip":        "1.2.3.4",
				"context_request_ip":       "5.6.7.8",
				"context_source_id":        "sourceID",
				"context_source_type":      "sourceType",
				"context_traits_email":     "rhedricks@example.com",
				"context_traits_logins":    float64(2),
				"context_traits_name":      "Richard Hendricks",
				"event":                    "event",
				"event_text":               "event",
				"id":                       "messageId",
				"original_timestamp":       "2021-09-01T00:00:00.000Z",
				"received_at":              "2021-09-01T00:00:00.000Z",
				"sent_at":                  "2021-09-01T00:00:00.000Z",
				"timestamp":                "2021-09-01T00:00:00.000Z",
				"user_id":                  "userId",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"anonymous_id":             "string",
					"channel":                  "string",
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_ip":               "string",
					"context_passed_ip":        "string",
					"context_request_ip":       "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"context_traits_email":     "string",
					"context_traits_logins":    "int",
					"context_traits_name":      "string",
					"event":                    "string",
					"event_text":               "string",
					"id":                       "string",
					"original_timestamp":       "datetime",
					"received_at":              "datetime",
					"sent_at":                  "datetime",
					"timestamp":                "datetime",
					"user_id":                  "string",
					"uuid_ts":                  "datetime",
				},
				"receivedAt": "2021-09-01T00:00:00.000Z",
				"table":      "tracks",
			},
			"userId": "",
		}
	}
	trackEventDefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"anonymous_id":             "anonymousId",
				"channel":                  "web",
				"context_destination_id":   "destinationID",
				"context_destination_type": "POSTGRES",
				"context_ip":               "1.2.3.4",
				"context_passed_ip":        "1.2.3.4",
				"context_request_ip":       "5.6.7.8",
				"context_source_id":        "sourceID",
				"context_source_type":      "sourceType",
				"context_traits_email":     "rhedricks@example.com",
				"context_traits_logins":    float64(2),
				"context_traits_name":      "Richard Hendricks",
				"event":                    "event",
				"event_text":               "event",
				"id":                       "messageId",
				"original_timestamp":       "2021-09-01T00:00:00.000Z",
				"product_id":               "9578257311",
				"rating":                   3.0,
				"received_at":              "2021-09-01T00:00:00.000Z",
				"review_body":              "OK for the price. It works but the material feels flimsy.",
				"review_id":                "86ac1cd43",
				"sent_at":                  "2021-09-01T00:00:00.000Z",
				"timestamp":                "2021-09-01T00:00:00.000Z",
				"user_id":                  "userId",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"anonymous_id":             "string",
					"channel":                  "string",
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_ip":               "string",
					"context_passed_ip":        "string",
					"context_request_ip":       "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"context_traits_email":     "string",
					"context_traits_logins":    "int",
					"context_traits_name":      "string",
					"event":                    "string",
					"event_text":               "string",
					"id":                       "string",
					"original_timestamp":       "datetime",
					"product_id":               "string",
					"rating":                   "int",
					"received_at":              "datetime",
					"review_body":              "string",
					"review_id":                "string",
					"sent_at":                  "datetime",
					"timestamp":                "datetime",
					"user_id":                  "string",
					"uuid_ts":                  "datetime",
				},
				"receivedAt": "2021-09-01T00:00:00.000Z",
				"table":      "event",
			},
			"userId": "",
		}
	}
	trackMergedefaultOutput := func() testhelper.OutputBuilder {
		return testhelper.OutputBuilder{
			"data": map[string]any{
				"merge_property_1_type":  "anonymous_id",
				"merge_property_1_value": "anonymousId",
				"merge_property_2_type":  "user_id",
				"merge_property_2_value": "userId",
			},
			"metadata": map[string]any{
				"columns": map[string]any{
					"merge_property_1_type":  "string",
					"merge_property_1_value": "string",
					"merge_property_2_type":  "string",
					"merge_property_2_value": "string",
				},
				"isMergeRule":  true,
				"mergePropOne": "anonymousId",
				"mergePropTwo": "userId",
				"receivedAt":   "2021-09-01T00:00:00.000Z",
				"table":        "rudder_identity_merge_rules",
			},
			"userId": "",
		}
	}

	testCases := []struct {
		name             string
		configOverride   map[string]any
		eventPayload     string
		metadata         ptrans.Metadata
		destination      backendconfig.DestinationT
		expectedResponse ptrans.Response
	}{
		{
			name:         "identify (POSTGRES)",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     identifyDefaultOutput(),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     userDefaultOutput(),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (S3_DATALAKE)",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "S3_DATALAKE"),
			destination: getDestination("S3_DATALAKE", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: identifyDefaultOutput().
							SetDataField("_timestamp", "2021-09-01T00:00:00.000Z").
							SetColumnField("_timestamp", "datetime").
							RemoveDataFields("timestamp").
							RemoveColumnFields("timestamp").
							SetDataField("context_destination_type", "S3_DATALAKE"),
						Metadata:   getMetadata("identify", "S3_DATALAKE"),
						StatusCode: http.StatusOK,
					},
					{
						Output: userDefaultOutput().
							RemoveDataFields("timestamp", "original_timestamp", "sent_at").
							RemoveColumnFields("timestamp", "original_timestamp", "sent_at").
							SetDataField("context_destination_type", "S3_DATALAKE"),
						Metadata:   getMetadata("identify", "S3_DATALAKE"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) without traits",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: identifyDefaultOutput().
							RemoveDataFields("product_id", "review_id").
							RemoveColumnFields("product_id", "review_id"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: userDefaultOutput().
							RemoveDataFields("product_id", "review_id").
							RemoveColumnFields("product_id", "review_id"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) without userProperties",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: identifyDefaultOutput().
							RemoveDataFields("rating", "review_body").
							RemoveColumnFields("rating", "review_body"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: userDefaultOutput().
							RemoveDataFields("rating", "review_body").
							RemoveColumnFields("rating", "review_body"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) without context.traits",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: identifyDefaultOutput().
							RemoveDataFields("context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name").
							RemoveColumnFields("context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: userDefaultOutput().
							RemoveDataFields("context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name").
							RemoveColumnFields("context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) without context",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: identifyDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: userDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name", "email", "logins", "name"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) not allowUsersContextTraits",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: identifyDefaultOutput().
							RemoveDataFields("email", "logins", "name").
							RemoveColumnFields("email", "logins", "name"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: userDefaultOutput().
							RemoveDataFields("email", "logins", "name").
							RemoveColumnFields("email", "logins", "name"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) user_id already exists",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"user_id":"user_id","rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     identifyDefaultOutput(),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     userDefaultOutput(),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) store rudder event",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent":          true,
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: identifyDefaultOutput().
							SetDataField("rudder_event", "{\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"ip\":\"1.2.3.4\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2,\"name\":\"Richard Hendricks\"},\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\"},\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"traits\":{\"product_id\":\"9578257311\",\"review_id\":\"86ac1cd43\"},\"type\":\"identify\",\"userId\":\"userId\",\"userProperties\":{\"rating\":3,\"review_body\":\"OK for the price. It works but the material feels flimsy.\"}}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     userDefaultOutput(),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) partial rules",
			eventPayload: `{"type":"identify","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: identifyDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
					{
						Output: userDefaultOutput().
							RemoveDataFields("context_request_ip").
							RemoveColumnFields("context_request_ip"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) no userID",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: identifyDefaultOutput().
							RemoveDataFields("user_id").
							RemoveColumnFields("user_id"),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) skipUsersTable (destOpts)",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: backendconfig.DestinationT{
				Name: "POSTGRES",
				Config: map[string]any{
					"allowUsersContextTraits": true,
					"skipUsersTable":          true,
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "POSTGRES",
				},
			},
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     identifyDefaultOutput(),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "identify (POSTGRES) skipUsersTable (intrOpts)",
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipUsersTable":true}}}}`,
			metadata:     getMetadata("identify", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     identifyDefaultOutput(),
						Metadata:   getMetadata("identify", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "identify (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipUsersTable":true}}}}`,
			metadata:     getMetadata("identify", "BQ"),
			destination: getDestination("BQ", map[string]any{
				"allowUsersContextTraits": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: identifyDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getMetadata("identify", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     identifyDefaultMergeOutput(),
						Metadata:   getMetadata("identify", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output: userDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getMetadata("identify", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},

		{
			name:         "alias (Postgres)",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("alias", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     aliasDefaultOutput(),
						Metadata:   getMetadata("alias", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) without traits",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("alias", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: aliasDefaultOutput().
							RemoveDataFields("title", "url").
							RemoveColumnFields("title", "url"),
						Metadata:   getMetadata("alias", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) without context",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata:     getMetadata("alias", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: aliasDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins"),
						Metadata:   getMetadata("alias", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) store rudder event",
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("alias", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: aliasDefaultOutput().
							SetDataField("rudder_event", "{\"type\":\"alias\",\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\",\"ip\":\"1.2.3.4\",\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2}},\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"previousId\":\"previousId\",\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"traits\":{\"title\":\"Home | RudderStack\",\"url\":\"https://www.rudderstack.com\"},\"userId\":\"userId\"}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getMetadata("alias", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "alias (Postgres) partial rules",
			eventPayload: `{"type":"alias","messageId":"messageId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("alias", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: aliasDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getMetadata("alias", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "alias (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("alias", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: aliasDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getMetadata("alias", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     aliasMergeDefaultOutput(),
						Metadata:   getMetadata("alias", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},

		{
			name:         "extract (Postgres)",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("extract", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     extractDefaultOutput(),
						Metadata:   getMetadata("extract", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) without properties",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("extract", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: extractDefaultOutput().
							RemoveDataFields("name", "title", "url").
							RemoveColumnFields("name", "title", "url"),
						Metadata:   getMetadata("extract", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) without context",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata:     getMetadata("extract", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: extractDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getMetadata("extract", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) RudderCreatedTable",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"accounts","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("extract", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: extractDefaultOutput().
							SetDataField("event", "accounts").
							SetTableName("_accounts"),
						Metadata:   getMetadata("extract", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) RudderCreatedTable with skipReservedKeywordsEscaping",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"accounts","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipReservedKeywordsEscaping":true}}}}`,
			metadata:     getMetadata("extract", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: extractDefaultOutput().
							SetDataField("event", "accounts").
							SetTableName("accounts"),
						Metadata:   getMetadata("extract", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "extract (Postgres) RudderIsolatedTable",
			eventPayload: `{"type":"extract","recordId":"recordID","event":"users","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("extract", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: extractDefaultOutput().
							SetDataField("event", "users").
							SetTableName("_users"),
						Metadata:   getMetadata("extract", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},

		{
			name:         "page (Postgres)",
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("page", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     pageDefaultOutput(),
						Metadata:   getMetadata("page", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "page (Postgres) without properties",
			eventPayload: `{"type":"page","name":"Home","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("page", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: pageDefaultOutput().
							RemoveDataFields("title", "url").
							RemoveColumnFields("title", "url"),
						Metadata:   getMetadata("page", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "page (Postgres) without context",
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata:     getMetadata("page", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: pageDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getMetadata("page", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "page (Postgres) store rudder event",
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("page", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: pageDefaultOutput().
							SetDataField("rudder_event", "{\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"ip\":\"1.2.3.4\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2,\"name\":\"Richard Hendricks\"},\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\"},\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"properties\":{\"name\":\"Home\",\"title\":\"Home | RudderStack\",\"url\":\"https://www.rudderstack.com\"},\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"type\":\"page\",\"userId\":\"userId\"}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getMetadata("page", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "page (Postgres) partial rules",
			eventPayload: `{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("page", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: pageDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getMetadata("page", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "page (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("page", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: pageDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getMetadata("page", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     pageMergeDefaultOutput(),
						Metadata:   getMetadata("page", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},

		{
			name:         "screen (Postgres)",
			eventPayload: `{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("screen", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     screenDefaultOutput(),
						Metadata:   getMetadata("screen", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "screen (Postgres) without properties",
			eventPayload: `{"type":"screen","name":"Main","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("screen", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: screenDefaultOutput().
							RemoveDataFields("title", "url").
							RemoveColumnFields("title", "url"),
						Metadata:   getMetadata("screen", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "screen (Postgres) without context",
			eventPayload: `{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata:     getMetadata("screen", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: screenDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getMetadata("screen", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "screen (Postgres) store rudder event",
			eventPayload: `{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("screen", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: screenDefaultOutput().
							SetDataField("rudder_event", "{\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"ip\":\"1.2.3.4\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2,\"name\":\"Richard Hendricks\"},\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\"},\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"properties\":{\"name\":\"Main\",\"title\":\"Home | RudderStack\",\"url\":\"https://www.rudderstack.com\"},\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"type\":\"screen\",\"userId\":\"userId\"}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getMetadata("screen", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "screen (Postgres) partial rules",
			eventPayload: `{"type":"screen","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("screen", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: screenDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getMetadata("screen", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "screen (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("screen", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: screenDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getMetadata("screen", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     screenMergeDefaultOutput(),
						Metadata:   getMetadata("screen", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},

		{
			name:         "group (Postgres)",
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("group", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     groupDefaultOutput(),
						Metadata:   getMetadata("group", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "group (Postgres) without traits",
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("group", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: groupDefaultOutput().
							RemoveDataFields("title", "url").
							RemoveColumnFields("title", "url"),
						Metadata:   getMetadata("group", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "group (Postgres) without context",
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`,
			metadata:     getMetadata("group", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: groupDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins"),
						Metadata:   getMetadata("group", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "group (Postgres) store rudder event",
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("group", "POSTGRES"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: groupDefaultOutput().
							SetDataField("rudder_event", "{\"type\":\"group\",\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\",\"ip\":\"1.2.3.4\",\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2}},\"groupId\":\"groupId\",\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"traits\":{\"title\":\"Home | RudderStack\",\"url\":\"https://www.rudderstack.com\"},\"userId\":\"userId\"}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getMetadata("group", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "group (Postgres) partial rules",
			eventPayload: `{"type":"group","messageId":"messageId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("group", "POSTGRES"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: groupDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getMetadata("group", "POSTGRES"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "group (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("group", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: groupDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime").
							SetTableName("_groups"),
						Metadata:   getMetadata("group", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     groupMergeDefaultOutput(),
						Metadata:   getMetadata("group", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},

		{
			name:         "track (POSTGRES)",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     trackDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     trackEventDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) without properties",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     trackDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
							RemoveDataFields("product_id", "review_id").
							RemoveColumnFields("product_id", "review_id"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) without userProperties",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     trackDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
							RemoveDataFields("rating", "review_body").
							RemoveColumnFields("rating", "review_body"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) without context",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: trackDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
							SetDataField("context_ip", "5.6.7.8"). // overriding the default value
							RemoveDataFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name").
							RemoveColumnFields("context_passed_ip", "context_traits_email", "context_traits_logins", "context_traits_name"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) RudderCreatedTable",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"accounts","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: trackDefaultOutput().
							SetDataField("event", "accounts").
							SetDataField("event_text", "accounts"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
							SetDataField("event", "accounts").
							SetDataField("event_text", "accounts").
							SetTableName("_accounts"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) RudderCreatedTable with skipReservedKeywordsEscaping",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"accounts","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipReservedKeywordsEscaping":true}}}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: trackDefaultOutput().
							SetDataField("event", "accounts").
							SetDataField("event_text", "accounts"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
							SetDataField("event", "accounts").
							SetDataField("event_text", "accounts").
							SetTableName("accounts"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) RudderIsolatedTable",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"users","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: trackDefaultOutput().
							SetDataField("event", "users").
							SetDataField("event_text", "users"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
							SetDataField("event", "users").
							SetDataField("event_text", "users").
							SetTableName("_users"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) empty event",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: trackDefaultOutput().
							SetDataField("event", "").
							RemoveDataFields("event_text").
							RemoveColumnFields("event_text"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) no event",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: trackDefaultOutput().
							SetDataField("event", "").
							RemoveDataFields("event_text").
							RemoveColumnFields("event_text"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) store rudder event",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination: getDestination("POSTGRES", map[string]any{
				"storeFullEvent": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: trackDefaultOutput().
							SetDataField("rudder_event", "{\"type\":\"track\",\"anonymousId\":\"anonymousId\",\"channel\":\"web\",\"context\":{\"destinationId\":\"destinationID\",\"destinationType\":\"POSTGRES\",\"ip\":\"1.2.3.4\",\"sourceId\":\"sourceID\",\"sourceType\":\"sourceType\",\"traits\":{\"email\":\"rhedricks@example.com\",\"logins\":2,\"name\":\"Richard Hendricks\"}},\"event\":\"event\",\"messageId\":\"messageId\",\"originalTimestamp\":\"2021-09-01T00:00:00.000Z\",\"properties\":{\"product_id\":\"9578257311\",\"review_id\":\"86ac1cd43\"},\"receivedAt\":\"2021-09-01T00:00:00.000Z\",\"request_ip\":\"5.6.7.8\",\"sentAt\":\"2021-09-01T00:00:00.000Z\",\"timestamp\":\"2021-09-01T00:00:00.000Z\",\"userId\":\"userId\",\"userProperties\":{\"rating\":3,\"review_body\":\"OK for the price. It works but the material feels flimsy.\"}}").
							SetColumnField("rudder_event", "json"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     trackEventDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) partial rules",
			eventPayload: `{"type":"track","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","event":"event","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: trackDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
							RemoveDataFields("anonymous_id", "channel", "context_request_ip").
							RemoveColumnFields("anonymous_id", "channel", "context_request_ip"),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) skipTracksTable (destOpts)",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination: getDestination("POSTGRES", map[string]any{
				"skipTracksTable": true,
			}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     trackEventDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name:         "track (POSTGRES) skipTracksTable (intrOpts)",
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipTracksTable":true}}}}`,
			metadata:     getTrackMetadata("POSTGRES", "webhook"),
			destination:  getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output:     trackEventDefaultOutput(),
						Metadata:   getTrackMetadata("POSTGRES", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "track (BQ) merge event",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getTrackMetadata("BQ", "webhook"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: trackDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getTrackMetadata("BQ", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output: trackEventDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getTrackMetadata("BQ", "webhook"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     trackMergedefaultOutput(),
						Metadata:   getTrackMetadata("BQ", "webhook"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},

		{
			name: "merge (Postgres)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload:     `{"type":"merge"}`,
			metadata:         getMetadata("merge", "POSTGRES"),
			destination:      getDestination("POSTGRES", map[string]any{}),
			expectedResponse: ptrans.Response{},
		},
		{
			name: "merge (BQ)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"},{"type":"mobile","value":"+1-202-555-0146"}]}`,
			metadata:     getMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
							"data": map[string]any{
								"merge_property_1_type":  "email",
								"merge_property_1_value": "alex@example.com",
								"merge_property_2_type":  "mobile",
								"merge_property_2_value": "+1-202-555-0146",
							},
							"metadata": map[string]any{
								"table":        "rudder_identity_merge_rules",
								"columns":      map[string]any{"merge_property_1_type": "string", "merge_property_1_value": "string", "merge_property_2_type": "string", "merge_property_2_value": "string"},
								"isMergeRule":  true,
								"receivedAt":   "2021-09-01T00:00:00.000Z",
								"mergePropOne": "alex@example.com",
								"mergePropTwo": "+1-202-555-0146",
							},
							"userId": "",
						},
						Metadata:   getMetadata("merge", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "merge (BQ) not enableIDResolution",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": false,
			},
			eventPayload:     `{"type":"merge"}`,
			metadata:         getMetadata("merge", "BQ"),
			destination:      getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{},
		},
		{
			name: "merge (BQ) missing mergeProperties",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge"}`,
			metadata:     getMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesMissing.Error(),
						StatusCode: response.ErrMergePropertiesMissing.StatusCode(),
						Metadata:   getMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) invalid mergeProperties",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge", "mergeProperties": "invalid"}`,
			metadata:     getMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesNotArray.Error(),
						StatusCode: response.ErrMergePropertiesNotArray.StatusCode(),
						Metadata:   getMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) empty mergeProperties",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge", "mergeProperties": []}`,
			metadata:     getMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesNotSufficient.Error(),
						StatusCode: response.ErrMergePropertiesNotSufficient.StatusCode(),
						Metadata:   getMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) single mergeProperties",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"}]}`,
			metadata:     getMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertiesNotSufficient.Error(),
						StatusCode: response.ErrMergePropertiesNotSufficient.StatusCode(),
						Metadata:   getMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) invalid merge property one",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":["invalid",{"type":"email","value":"alex@example.com"}]}`,
			metadata:     getMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertyOneInvalid.Error(),
						StatusCode: response.ErrMergePropertyOneInvalid.StatusCode(),
						Metadata:   getMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) invalid merge property two",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"},"invalid"]}`,
			metadata:     getMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertyTwoInvalid.Error(),
						StatusCode: response.ErrMergePropertyTwoInvalid.StatusCode(),
						Metadata:   getMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (BQ) missing mergeProperty",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type1":"email","value1":"alex@example.com"},{"type1":"mobile","value1":"+1-202-555-0146"}]}`,
			metadata:     getMetadata("merge", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				FailedEvents: []ptrans.TransformerResponse{
					{
						Error:      response.ErrMergePropertyEmpty.Error(),
						StatusCode: response.ErrMergePropertyEmpty.StatusCode(),
						Metadata:   getMetadata("merge", "BQ"),
					},
				},
			},
		},
		{
			name: "merge (SNOWFLAKE)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"},{"type":"mobile","value":"+1-202-555-0146"}]}`,
			metadata:     getMetadata("merge", "SNOWFLAKE"),
			destination:  getDestination("SNOWFLAKE", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: map[string]any{
							"data": map[string]any{
								"MERGE_PROPERTY_1_TYPE":  "email",
								"MERGE_PROPERTY_1_VALUE": "alex@example.com",
								"MERGE_PROPERTY_2_TYPE":  "mobile",
								"MERGE_PROPERTY_2_VALUE": "+1-202-555-0146",
							},
							"metadata": map[string]any{
								"table":        "RUDDER_IDENTITY_MERGE_RULES",
								"columns":      map[string]any{"MERGE_PROPERTY_1_TYPE": "string", "MERGE_PROPERTY_1_VALUE": "string", "MERGE_PROPERTY_2_TYPE": "string", "MERGE_PROPERTY_2_VALUE": "string"},
								"isMergeRule":  true,
								"receivedAt":   "2021-09-01T00:00:00.000Z",
								"mergePropOne": "alex@example.com",
								"mergePropTwo": "+1-202-555-0146",
							},
							"userId": "",
						},
						Metadata:   getMetadata("merge", "SNOWFLAKE"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "alias (BQ)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("alias", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: aliasDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getMetadata("alias", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     aliasMergeDefaultOutput(),
						Metadata:   getMetadata("alias", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "alias (BQ) no userId and previousId",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("alias", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: aliasDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime").
							RemoveDataFields("user_id", "previous_id").
							RemoveColumnFields("user_id", "previous_id"),
						Metadata:   getMetadata("alias", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "alias (BQ) empty userId and previousId",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"","previousId":"","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("alias", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: aliasDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime").
							RemoveDataFields("user_id", "previous_id").
							RemoveColumnFields("user_id", "previous_id"),
						Metadata:   getMetadata("alias", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "page (BQ)",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("page", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: pageDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime"),
						Metadata:   getMetadata("page", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output:     pageMergeDefaultOutput(),
						Metadata:   getMetadata("page", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "page (BQ) no anonymousID",
			configOverride: map[string]any{
				"Warehouse.enableIDResolution": true,
			},
			eventPayload: `{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`,
			metadata:     getMetadata("page", "BQ"),
			destination:  getDestination("BQ", map[string]any{}),
			expectedResponse: ptrans.Response{
				Events: []ptrans.TransformerResponse{
					{
						Output: pageDefaultOutput().
							SetDataField("context_destination_type", "BQ").
							SetColumnField("loaded_at", "datetime").
							RemoveDataFields("anonymous_id").
							RemoveColumnFields("anonymous_id"),
						Metadata:   getMetadata("page", "BQ"),
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]any{
							"data": map[string]any{
								"merge_property_1_type":  "user_id",
								"merge_property_1_value": "userId",
							},
							"metadata": map[string]any{
								"table":        "rudder_identity_merge_rules",
								"columns":      map[string]any{"merge_property_1_type": "string", "merge_property_1_value": "string"},
								"isMergeRule":  true,
								"receivedAt":   "2021-09-01T00:00:00.000Z",
								"mergePropOne": "userId",
							},
							"userId": "",
						},
						Metadata:   getMetadata("page", "BQ"),
						StatusCode: http.StatusOK,
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := setupConfig(transformerResource, tc.configOverride)

			processorTransformer := ptrans.NewTransformer(c, logger.NOP, stats.Default)
			warehouseTransformer := New(c, logger.NOP, stats.NOP)

			eventContexts := []testhelper.EventContext{
				{
					Payload:     []byte(tc.eventPayload),
					Metadata:    tc.metadata,
					Destination: tc.destination,
				},
			}
			testhelper.ValidateEvents(t, eventContexts, processorTransformer, warehouseTransformer, tc.expectedResponse)
		})
	}
}
