package testhelper

import (
	b64 "encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
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
)

func SendEvents(t testing.TB, wareHouseTest *WareHouseTest, eventsMap EventsCountMap) {
	t.Helper()

	require.NotNil(t, eventsMap)
	require.NotEmpty(t, eventsMap)

	if count, exists := eventsMap["identifies"]; exists {
		t.Logf("Sending %d identifies events", count)

		for i := 0; i < count; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(IdentifyPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadIdentify, "identify", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["tracks"]; exists {
		t.Logf("Sending %d tracks events", count)

		for i := 0; i < count; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(TrackPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadTrack, "track", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["pages"]; exists {
		t.Logf("Sending %d pages events", count)

		for i := 0; i < count; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(PagePayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadPage, "page", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["screens"]; exists {
		t.Logf("Sending %d screens events", count)

		for i := 0; i < count; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ScreenPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadScreen, "screen", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["aliases"]; exists {
		t.Logf("Sending %d aliases events", count)

		for i := 0; i < count; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(AliasPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadAlias, "alias", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["groups"]; exists {
		t.Logf("Sending %d groups events", count)

		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(GroupPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadGroup, "group", wareHouseTest.WriteKey)
		}
	}
}

func SendModifiedEvents(t testing.TB, wareHouseTest *WareHouseTest, eventsMap EventsCountMap) {
	t.Helper()

	require.NotNil(t, eventsMap)
	require.NotEmpty(t, eventsMap)

	if count, exists := eventsMap["identifies"]; exists {
		t.Logf("Sending %d modified identifies events", count)

		for i := 0; i < count; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(ModifiedIdentifyPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadIdentify, "identify", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["tracks"]; exists {
		t.Logf("Sending %d modified tracks events", count)

		for i := 0; i < count; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(ModifiedTrackPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadTrack, "track", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["pages"]; exists {
		t.Logf("Sending %d modified pages events", count)

		for i := 0; i < count; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(ModifiedPagePayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadPage, "page", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["screens"]; exists {
		t.Logf("Sending %d modified screens events", count)

		for i := 0; i < count; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ModifiedScreenPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadScreen, "screen", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["aliases"]; exists {
		t.Logf("Sending %d modified aliases events", count)

		for i := 0; i < count; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(ModifiedAliasPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadAlias, "alias", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["groups"]; exists {
		t.Logf("Sending %d modified groups events", count)

		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(ModifiedGroupPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadGroup, "group", wareHouseTest.WriteKey)
		}
	}
}

func SendIntegratedEvents(t testing.TB, wareHouseTest *WareHouseTest, eventsMap EventsCountMap) {
	t.Helper()

	require.NotNil(t, eventsMap)
	require.NotEmpty(t, eventsMap)

	if count, exists := eventsMap["identifies"]; exists {
		t.Logf("Sending %d integrated identifies events", count)

		for i := 0; i < count; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(ReservedKeywordsIdentifyPayload, wareHouseTest.UserId, wareHouseTest.MsgId(), wareHouseTest.Provider))
			send(t, payloadIdentify, "identify", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["tracks"]; exists {
		t.Logf("Sending %d integrated tracks events", count)

		for i := 0; i < count; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(ReservedKeywordsTrackPayload, wareHouseTest.UserId, wareHouseTest.MsgId(), wareHouseTest.Provider))
			send(t, payloadTrack, "track", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["pages"]; exists {
		t.Logf("Sending %d integrated pages events", count)

		for i := 0; i < count; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(ReservedKeywordsPagePayload, wareHouseTest.UserId, wareHouseTest.MsgId(), wareHouseTest.Provider))
			send(t, payloadPage, "page", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["screens"]; exists {
		t.Logf("Sending %d integrated screens events", count)

		for i := 0; i < count; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ReservedKeywordsScreenPayload, wareHouseTest.UserId, wareHouseTest.MsgId(), wareHouseTest.Provider))
			send(t, payloadScreen, "screen", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["aliases"]; exists {
		t.Logf("Sending %d integrated aliases events", count)

		for i := 0; i < count; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(AliasPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadAlias, "alias", wareHouseTest.WriteKey)
		}
	}

	if count, exists := eventsMap["groups"]; exists {
		t.Logf("Sending %d integrated groups events", count)

		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(ReservedKeywordsGroupPayload, wareHouseTest.UserId, wareHouseTest.MsgId(), wareHouseTest.Provider))
			send(t, payloadGroup, "group", wareHouseTest.WriteKey)
		}
	}
}

func send(t testing.TB, payload *strings.Reader, eventType, writeKey string) {
	t.Helper()

	t.Logf("Sending event: %s for writeKey: %s", eventType, writeKey)

	url := fmt.Sprintf("http://localhost:%s/v1/%s", "8080", eventType)
	method := "POST"
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
	defer func() { _ = res.Body.Close() }()

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
