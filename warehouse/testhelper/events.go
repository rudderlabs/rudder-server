package testhelper

import (
	b64 "encoding/base64"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/gofrs/uuid"
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
	  "event": "%s",
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
		"url": "http://www.rudderstack.com"
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
	  "event": "%s",
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
		"url": "http://www.rudderstack.com"
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

func SendEvents(t testing.TB, wdt *WareHouseDestinationTest) {
	if identify, exists := wdt.EventsCountMap["identifies"]; exists {
		t.Logf("Sending identifies events")
		for i := 0; i < identify; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(IdentifyPayload, wdt.UserId, wdt.MsgId()))
			send(payloadIdentify, "identify", wdt.WriteKey)
		}
	}

	if track, exists := wdt.EventsCountMap["tracks"]; exists {
		t.Logf("Sending tracks events")
		for i := 0; i < track; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(TrackPayload, wdt.UserId, wdt.MsgId(), wdt.Event))
			send(payloadTrack, "track", wdt.WriteKey)
		}
	}

	if page, exists := wdt.EventsCountMap["pages"]; exists {
		t.Logf("Sending pages events")
		for i := 0; i < page; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(PagePayload, wdt.UserId, wdt.MsgId()))
			send(payloadPage, "page", wdt.WriteKey)
		}
	}

	if screen, exists := wdt.EventsCountMap["screens"]; exists {
		t.Logf("Sending screens events")
		for i := 0; i < screen; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ScreenPayload, wdt.UserId, wdt.MsgId()))
			send(payloadScreen, "screen", wdt.WriteKey)
		}
	}

	if alias, exists := wdt.EventsCountMap["aliases"]; exists {
		t.Logf("Sending aliases events")
		for i := 0; i < alias; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(AliasPayload, wdt.UserId, wdt.MsgId()))
			send(payloadAlias, "alias", wdt.WriteKey)
		}
	}

	if group, exists := wdt.EventsCountMap["groups"]; exists {
		t.Logf("Sending groups events")
		for i := 0; i < group; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(GroupPayload, wdt.UserId, wdt.MsgId()))
			send(payloadGroup, "group", wdt.WriteKey)
		}
	}
}

func SendModifiedEvents(t testing.TB, wdt *WareHouseDestinationTest) {
	if identify, exists := wdt.EventsCountMap["identifies"]; exists {
		t.Logf("Sending modified identifies events")
		for i := 0; i < identify; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(ModifiedIdentifyPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			send(payloadIdentify, "identify", wdt.WriteKey)
		}
	}

	if track, exists := wdt.EventsCountMap["tracks"]; exists {
		t.Logf("Sending modified tracks events")
		for i := 0; i < track; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(ModifiedTrackPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String(), wdt.Event))
			send(payloadTrack, "track", wdt.WriteKey)
		}
	}

	if page, exists := wdt.EventsCountMap["pages"]; exists {
		t.Logf("Sending modified pages events")
		for i := 0; i < page; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(ModifiedPagePayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			send(payloadPage, "page", wdt.WriteKey)
		}
	}

	if screen, exists := wdt.EventsCountMap["screens"]; exists {
		t.Logf("Sending modified screens events")
		for i := 0; i < screen; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ModifiedScreenPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			send(payloadScreen, "screen", wdt.WriteKey)
		}
	}

	if alias, exists := wdt.EventsCountMap["aliases"]; exists {
		t.Logf("Sending modified aliases events")
		for i := 0; i < alias; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(ModifiedAliasPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			send(payloadAlias, "alias", wdt.WriteKey)
		}
	}

	if group, exists := wdt.EventsCountMap["groups"]; exists {
		t.Logf("Sending modified groups events")
		for i := 0; i < group; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(ModifiedGroupPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			send(payloadGroup, "group", wdt.WriteKey)
		}
	}
}

func send(payload *strings.Reader, eventType, writeKey string) {
	log.Printf("Sending event: %s for writeKey: %s", eventType, writeKey)

	url := fmt.Sprintf("http://localhost:%s/v1/%s", "8080", eventType)
	method := "POST"
	httpClient := &http.Client{}

	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		log.Printf("Error occurred while creating new http request for sending event with error: %s", err.Error())
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
		log.Printf("Error occurred while making http request for sending event with error: %s", err.Error())
		return
	}
	defer func() { _ = res.Body.Close() }()

	_, err = io.ReadAll(res.Body)
	if err != nil {
		log.Printf("Error occurred while reading http response for sending event with error: %s", err.Error())
		return
	}
	if res.Status != "200 OK" {
		return
	}

	log.Printf("Send successfully for event: %s and writeKey: %s", eventType, writeKey)
}
