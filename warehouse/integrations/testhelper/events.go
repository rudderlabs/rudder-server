package testhelper

import (
	b64 "encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/stretchr/testify/require"
)

const (
	GoogleSheetsPayload = `{
	  "batch": [
		{
		  "messageId": "%s",
		  "userId": "%s",
		  "recordId": "%s",
		  "context": {
			"sources": {
			  "batch_id": "e84d84e1-be39-41cb-85e6-66874b6a4730",
			  "job_id": "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
			  "job_run_id": "%s",
			  "task_id": "Sheet1",
			  "task_run_id": "%s",
			  "version": "v2.1.1"
			}
		  },
		  "properties": {
			"HEADER": "HBD5",
			"HEADER4": "esgseg78"
		  },
		  "event": "google_sheet",
		  "type": "track",
		  "channel": "sources"
		}
	  ]
	}`
	AsyncWhPayload = `{
		"source_id":"%s",
		"job_run_id":"%s",
		"task_run_id":"%s",
		"channel":"sources",
		"async_job_type":"deletebyjobrunid",
		"destination_id":"%s",
		"start_time":"%s",
		"workspace_id":"%s"
	}`
	IdentifyPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "identify",
	  "eventOrderNo": "1",
	  "context": {
		"traits": {
		  "trait1": "new-val"
		}
	  },
	  "timestamp": "2020-02-02T00:23:09.544Z"
	}`
	TrackPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "track",
	  "event": "Product Track",
	  "properties": {
		"review_id": "12345",
		"product_id": "123",
		"rating": 3,
		"review_body": "Average product, expected much more."
	  }
	}`
	PagePayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "page",
	  "name": "Home",
	  "properties": {
		"title": "Home | RudderStack",
		"url": "https://www.rudderstack.com"
	  }
	}`
	ScreenPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "screen",
	  "name": "Main",
	  "properties": {
		"prop_key": "prop_value"
	  }
	}`
	AliasPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "alias",
	  "previousId": "name@surname.com"
	}`
	GroupPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "group",
	  "groupId": "groupId",
	  "traits": {
		"name": "MyGroup",
		"industry": "IT",
		"employees": 450,
		"plan": "basic"
	  }
	}`
	ModifiedIdentifyPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "identify",
	  "context": {
		"traits": {
		  "trait1": "new-val"
		},
		"ip": "14.5.67.21",
		"library": {
		  "name": "http"
		}
	  },
	  "timestamp": "2020-02-02T00:23:09.544Z"
	}`
	ModifiedTrackPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "track",
	  "event": "Product Track",
	  "properties": {
		"review_id": "12345",
		"product_id": "123",
		"rating": 3,
		"revenue": 4.99,
		"review_body": "Average product, expected much more."
	  },
	  "context": {
		"ip": "14.5.67.21",
		"library": {
		  "name": "http"
		}
	  }
	}`
	ModifiedPagePayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "page",
	  "name": "Home",
	  "properties": {
		"title": "Home | RudderStack",
		"url": "https://www.rudderstack.com"
	  },
	  "context": {
		"ip": "14.5.67.21",
		"library": {
		  "name": "http"
		}
	  }
	}`
	ModifiedScreenPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "screen",
	  "name": "Main",
	  "properties": {
		"prop_key": "prop_value"
	  },
	  "context": {
		"ip": "14.5.67.21",
		"library": {
		  "name": "http"
		}
	  }
	}`
	ModifiedAliasPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "alias",
	  "previousId": "name@surname.com",
	  "context": {
		"ip": "14.5.67.21",
		"library": {
		  "name": "http"
		}
	  }
	}`
	ModifiedGroupPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "group",
	  "groupId": "groupId",
	  "traits": {
		"name": "MyGroup",
		"industry": "IT",
		"employees": 450,
		"plan": "basic"
	  },
	  "context": {
		"ip": "14.5.67.21",
		"library": {
		  "name": "http"
		}
	  }
	}`
	ModifiedGoogleSheetsPayload = `{
	  "batch": [
		{
		  "messageId": "%s",
		  "userId": "%s",
		  "recordId": "%s",
		  "context": {
			"sources": {
			  "batch_id": "e84d84e1-be39-41cb-85e6-66874b6a4730",
			  "job_id": "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
			  "job_run_id": "%s",
			  "task_id": "Sheet1",
			  "task_run_id": "%s",
			  "version": "v2.1.1"
			},
			"ip": "14.5.67.21",
			"library": {
			  "name": "http"
			}
		  },
		  "properties": {
			"HEADER": "HBD5",
			"HEADER4": "esgseg78"
		  },
		  "event": "google_sheet",
		  "type": "track",
		  "channel": "sources"
		}
	  ]
	}`
	ReservedKeywordsIdentifyPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "identify",
	  "integrations": {
		"%s": {
		  "options": {
			"skipReservedKeywordsEscaping": true
		  }
		}
	  },
	  "eventOrderNo": "1",
	  "context": {
		"traits": {
		  "trait1": "new-val",
		  "as": "non escaped column",
		  "between": "non escaped column"
		}
	  },
	  "timestamp": "2020-02-02T00:23:09.544Z"
	}`
	ReservedKeywordsTrackPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "track",
	  "integrations": {
		"%s": {
		  "options": {
			"skipReservedKeywordsEscaping": true
		  }
		}
	  },
	  "event": "Product Track",
	  "properties": {
		"as": "non escaped column",
		"between": "non escaped column",
		"review_id": "12345",
		"product_id": "123",
		"rating": 3,
		"review_body": "Average product, expected much more."
	  }
	}`
	ReservedKeywordsPagePayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "page",
	  "integrations": {
		"%s": {
		  "options": {
			"skipReservedKeywordsEscaping": true
		  }
		}
	  },
	  "name": "Home",
	  "properties": {
		"as": "non escaped column",
		"between": "non escaped column",
		"title": "Home | RudderStack",
		"url": "https://www.rudderstack.com"
	  }
	}`
	ReservedKeywordsScreenPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "screen",
	  "integrations": {
		"%s": {
		  "options": {
			"skipReservedKeywordsEscaping": true
		  }
		}
	  },
	  "name": "Main",
	  "properties": {
		"as": "non escaped column",
		"between": "non escaped column",
		"prop_key": "prop_value"
	  }
	}`
	ReservedKeywordsGroupPayload = `{
	  "userId": "%s",
	  "messageId": "%s",
	  "type": "group",
	  "integrations": {
		"%s": {
		  "options": {
			"skipReservedKeywordsEscaping": true
		  }
		}
	  },
	  "groupId": "groupId",
	  "traits": {
		"as": "non escaped column",
		"between": "non escaped column",
		"name": "MyGroup",
		"industry": "IT",
		"employees": 450,
		"plan": "basic"
	  }
	}`
	ReservedGoogleSheetsPayload = `{
	  "batch": [
		{
		  "messageId": "%s",
		  "userId": "%s",
		  "recordId": "%s",
		  "integrations": {
			"%s": {
			  "options": {
				"skipReservedKeywordsEscaping": true
			  }
			}
		  },
		  "context": {
			"sources": {
			  "batch_id": "e84d84e1-be39-41cb-85e6-66874b6a4730",
			  "job_id": "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
			  "job_run_id": "%s",
			  "task_id": "Sheet1",
			  "task_run_id": "%s",
			  "version": "v2.1.1"
			},
			"as": "non escaped column",
			"between": "non escaped column"
		  },
		  "properties": {
			"as": "non escaped column",
			"between": "non escaped column",
			"prop_key": "prop_value"
		  },
		  "event": "google_sheet",
		  "type": "track",
		  "channel": "sources"
		}
	  ]
	}`
)

func SendEvents(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()

	eventsMap := wareHouseTest.EventsMap

	require.NotNil(t, eventsMap)
	require.NotEmpty(t, eventsMap)

	if count, exists := eventsMap["identifies"]; exists {
		t.Logf("Sending %d identifies events", count)

		for i := 0; i < count; i++ {
			payloadIdentify := strings.NewReader(
				fmt.Sprintf(
					IdentifyPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadIdentify, "identify", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["tracks"]; exists {
		t.Logf("Sending %d tracks events", count)

		for i := 0; i < count; i++ {
			payloadTrack := strings.NewReader(
				fmt.Sprintf(
					TrackPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadTrack, "track", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["pages"]; exists {
		t.Logf("Sending %d pages events", count)

		for i := 0; i < count; i++ {
			payloadPage := strings.NewReader(
				fmt.Sprintf(
					PagePayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadPage, "page", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["screens"]; exists {
		t.Logf("Sending %d screens events", count)

		for i := 0; i < count; i++ {
			payloadScreen := strings.NewReader(
				fmt.Sprintf(
					ScreenPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadScreen, "screen", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["aliases"]; exists {
		t.Logf("Sending %d aliases events", count)

		for i := 0; i < count; i++ {
			payloadAlias := strings.NewReader(
				fmt.Sprintf(
					AliasPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadAlias, "alias", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["groups"]; exists {
		t.Logf("Sending %d groups events", count)

		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(
				fmt.Sprintf(
					GroupPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadGroup, "group", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["google_sheet"]; exists {
		t.Logf("Sending sources events")

		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(
				fmt.Sprintf(
					GoogleSheetsPayload,
					wareHouseTest.msgID(),
					wareHouseTest.UserID,
					wareHouseTest.recordID(),
					wareHouseTest.JobRunID,
					wareHouseTest.TaskRunID,
				),
			)
			send(t, payloadGroup, "import", wareHouseTest.WriteKey, "POST")
		}
	}
}

func SendModifiedEvents(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()

	eventsMap := wareHouseTest.EventsMap

	require.NotNil(t, eventsMap)
	require.NotEmpty(t, eventsMap)

	if count, exists := eventsMap["identifies"]; exists {
		t.Logf("Sending %d modified identifies events", count)

		for i := 0; i < count; i++ {
			payloadIdentify := strings.NewReader(
				fmt.Sprintf(
					ModifiedIdentifyPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadIdentify, "identify", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["tracks"]; exists {
		t.Logf("Sending %d modified tracks events", count)

		for i := 0; i < count; i++ {
			payloadTrack := strings.NewReader(
				fmt.Sprintf(
					ModifiedTrackPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadTrack, "track", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["pages"]; exists {
		t.Logf("Sending %d modified pages events", count)

		for i := 0; i < count; i++ {
			payloadPage := strings.NewReader(
				fmt.Sprintf(
					ModifiedPagePayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadPage, "page", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["screens"]; exists {
		t.Logf("Sending %d modified screens events", count)

		for i := 0; i < count; i++ {
			payloadScreen := strings.NewReader(
				fmt.Sprintf(
					ModifiedScreenPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadScreen, "screen", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["aliases"]; exists {
		t.Logf("Sending %d modified aliases events", count)

		for i := 0; i < count; i++ {
			payloadAlias := strings.NewReader(
				fmt.Sprintf(
					ModifiedAliasPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadAlias, "alias", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["groups"]; exists {
		t.Logf("Sending %d modified groups events", count)

		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(
				fmt.Sprintf(ModifiedGroupPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadGroup, "group", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["google_sheet"]; exists {
		t.Logf("Sending %d modified sources events", count)

		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(
				fmt.Sprintf(
					ModifiedGoogleSheetsPayload,
					wareHouseTest.msgID(),
					wareHouseTest.UserID,
					wareHouseTest.recordID(),
					wareHouseTest.JobRunID,
					wareHouseTest.TaskRunID,
				),
			)
			send(t, payloadGroup, "import", wareHouseTest.WriteKey, "POST")
		}
	}
}

func SendIntegratedEvents(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()

	eventsMap := wareHouseTest.EventsMap

	require.NotNil(t, eventsMap)
	require.NotEmpty(t, eventsMap)

	if count, exists := eventsMap["identifies"]; exists {
		t.Logf("Sending %d integrated identifies events", count)

		for i := 0; i < count; i++ {
			payloadIdentify := strings.NewReader(
				fmt.Sprintf(
					ReservedKeywordsIdentifyPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
					wareHouseTest.Provider,
				),
			)
			send(t, payloadIdentify, "identify", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["tracks"]; exists {
		t.Logf("Sending %d integrated tracks events", count)

		for i := 0; i < count; i++ {
			payloadTrack := strings.NewReader(
				fmt.Sprintf(
					ReservedKeywordsTrackPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
					wareHouseTest.Provider,
				),
			)
			send(t, payloadTrack, "track", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["pages"]; exists {
		t.Logf("Sending %d integrated pages events", count)

		for i := 0; i < count; i++ {
			payloadPage := strings.NewReader(
				fmt.Sprintf(
					ReservedKeywordsPagePayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
					wareHouseTest.Provider,
				),
			)
			send(t, payloadPage, "page", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["screens"]; exists {
		t.Logf("Sending %d integrated screens events", count)

		for i := 0; i < count; i++ {
			payloadScreen := strings.NewReader(
				fmt.Sprintf(
					ReservedKeywordsScreenPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
					wareHouseTest.Provider,
				),
			)
			send(t, payloadScreen, "screen", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["aliases"]; exists {
		t.Logf("Sending %d integrated aliases events", count)

		for i := 0; i < count; i++ {
			payloadAlias := strings.NewReader(
				fmt.Sprintf(
					AliasPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
				),
			)
			send(t, payloadAlias, "alias", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["groups"]; exists {
		t.Logf("Sending %d integrated groups events", count)

		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(
				fmt.Sprintf(
					ReservedKeywordsGroupPayload,
					wareHouseTest.UserID,
					wareHouseTest.msgID(),
					wareHouseTest.Provider,
				),
			)
			send(t, payloadGroup, "group", wareHouseTest.WriteKey, "POST")
		}
	}

	if count, exists := eventsMap["google_sheet"]; exists {
		t.Logf("Sending %d integrated sources events", count)

		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(
				fmt.Sprintf(
					ReservedGoogleSheetsPayload,
					wareHouseTest.msgID(),
					wareHouseTest.UserID,
					wareHouseTest.recordID(),
					wareHouseTest.Provider,
					wareHouseTest.JobRunID,
					wareHouseTest.TaskRunID,
				),
			)
			send(t, payloadGroup, "import", wareHouseTest.WriteKey, "POST")
		}
	}
}

func send(t testing.TB, payload *strings.Reader, eventType, writeKey, method string) {
	t.Helper()

	t.Logf("Sending event: %s for writeKey: %s", eventType, writeKey)

	url := fmt.Sprintf("http://localhost:%s/v1/%s", "8080", eventType)
	httpClient := &http.Client{}

	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		t.Errorf("Error occurred while creating new http request for sending event with error: %s", err.Error())
		return
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization",
		fmt.Sprintf("Basic %s", b64.StdEncoding.EncodeToString(
			[]byte(fmt.Sprintf("%s:", writeKey)),
		)),
	)

	res, err := httpClient.Do(req)
	if err != nil {
		t.Errorf("Error occurred while making http request for sending event with error: %s", err.Error())
		return
	}
	defer func() { httputil.CloseResponse(res) }()

	_, err = io.ReadAll(res.Body)
	if err != nil {
		t.Errorf("Error occurred while reading http response for sending event with error: %s", err.Error())
		return
	}
	if res.Status != "200 OK" {
		return
	}

	t.Logf("Send successfully for event: %s and writeKey: %s", eventType, writeKey)
}
