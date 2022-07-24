package testhelper

import (
	b64 "encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
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
)

func SendEvents(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()

	if count, exists := wareHouseTest.EventsCountMap["identifies"]; exists {
		t.Logf("Sending identifies events")
		for i := 0; i < count; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(IdentifyPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadIdentify, "identify", wareHouseTest.WriteKey)
		}
	}

	if count, exists := wareHouseTest.EventsCountMap["tracks"]; exists {
		t.Logf("Sending tracks events")
		for i := 0; i < count; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(TrackPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadTrack, "track", wareHouseTest.WriteKey)
		}
	}

	if count, exists := wareHouseTest.EventsCountMap["pages"]; exists {
		t.Logf("Sending pages events")
		for i := 0; i < count; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(PagePayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadPage, "page", wareHouseTest.WriteKey)
		}
	}

	if count, exists := wareHouseTest.EventsCountMap["screens"]; exists {
		t.Logf("Sending screens events")
		for i := 0; i < count; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ScreenPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadScreen, "screen", wareHouseTest.WriteKey)
		}
	}

	if count, exists := wareHouseTest.EventsCountMap["aliases"]; exists {
		t.Logf("Sending aliases events")
		for i := 0; i < count; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(AliasPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadAlias, "alias", wareHouseTest.WriteKey)
		}
	}

	if count, exists := wareHouseTest.EventsCountMap["groups"]; exists {
		t.Logf("Sending groups events")
		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(GroupPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadGroup, "group", wareHouseTest.WriteKey)
		}
	}
}

func SendModifiedEvents(t testing.TB, wareHouseTest *WareHouseTest) {
	t.Helper()

	if count, exists := wareHouseTest.EventsCountMap["identifies"]; exists {
		t.Logf("Sending modified identifies events")
		for i := 0; i < count; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(ModifiedIdentifyPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadIdentify, "identify", wareHouseTest.WriteKey)
		}
	}

	if count, exists := wareHouseTest.EventsCountMap["tracks"]; exists {
		t.Logf("Sending modified tracks events")
		for i := 0; i < count; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(ModifiedTrackPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadTrack, "track", wareHouseTest.WriteKey)
		}
	}

	if count, exists := wareHouseTest.EventsCountMap["pages"]; exists {
		t.Logf("Sending modified pages events")
		for i := 0; i < count; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(ModifiedPagePayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadPage, "page", wareHouseTest.WriteKey)
		}
	}

	if count, exists := wareHouseTest.EventsCountMap["screens"]; exists {
		t.Logf("Sending modified screens events")
		for i := 0; i < count; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ModifiedScreenPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadScreen, "screen", wareHouseTest.WriteKey)
		}
	}

	if count, exists := wareHouseTest.EventsCountMap["aliases"]; exists {
		t.Logf("Sending modified aliases events")
		for i := 0; i < count; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(ModifiedAliasPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
			send(t, payloadAlias, "alias", wareHouseTest.WriteKey)
		}
	}

	if count, exists := wareHouseTest.EventsCountMap["groups"]; exists {
		t.Logf("Sending modified groups events")
		for i := 0; i < count; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(ModifiedGroupPayload, wareHouseTest.UserId, wareHouseTest.MsgId()))
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
