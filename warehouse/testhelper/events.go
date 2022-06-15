package testhelper

import (
	b64 "encoding/base64"
	"fmt"
	"github.com/gofrs/uuid"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
)

func sendEvent(payload *strings.Reader, eventType, writeKey string) {
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

func SendEvents(t testing.TB, wdt *WareHouseDestinationTest) {
	if identify, exists := wdt.EventsCountMap["identifies"]; exists {
		t.Logf("Sending identifies events")
		for i := 0; i < identify; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(IdentifyPayload, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadIdentify, "identify", wdt.WriteKey)
		}
	}

	if track, exists := wdt.EventsCountMap["tracks"]; exists {
		t.Logf("Sending tracks events")
		for i := 0; i < track; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(TrackPayload, wdt.UserId, wdt.MsgId(), wdt.Event))
			sendEvent(payloadTrack, "track", wdt.WriteKey)
		}
	}

	if page, exists := wdt.EventsCountMap["pages"]; exists {
		t.Logf("Sending pages events")
		for i := 0; i < page; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(PagePayload, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadPage, "page", wdt.WriteKey)
		}
	}

	if screen, exists := wdt.EventsCountMap["screens"]; exists {
		t.Logf("Sending screens events")
		for i := 0; i < screen; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ScreenPayload, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadScreen, "screen", wdt.WriteKey)
		}
	}

	if alias, exists := wdt.EventsCountMap["aliases"]; exists {
		t.Logf("Sending aliases events")
		for i := 0; i < alias; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(AliasPayload, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadAlias, "alias", wdt.WriteKey)
		}
	}

	if group, exists := wdt.EventsCountMap["groups"]; exists {
		t.Logf("Sending groups events")
		for i := 0; i < group; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(GroupPayload, wdt.UserId, wdt.MsgId()))
			sendEvent(payloadGroup, "group", wdt.WriteKey)
		}
	}
}

func SendModifiedEvents(t testing.TB, wdt *WareHouseDestinationTest) {
	if identify, exists := wdt.EventsCountMap["identifies"]; exists {
		t.Logf("Sending modified identifies events")
		for i := 0; i < identify; i++ {
			payloadIdentify := strings.NewReader(fmt.Sprintf(ModifiedIdentifyPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			sendEvent(payloadIdentify, "identify", wdt.WriteKey)
		}
	}

	if track, exists := wdt.EventsCountMap["tracks"]; exists {
		t.Logf("Sending modified tracks events")
		for i := 0; i < track; i++ {
			payloadTrack := strings.NewReader(fmt.Sprintf(ModifiedTrackPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String(), wdt.Event))
			sendEvent(payloadTrack, "track", wdt.WriteKey)
		}
	}

	if page, exists := wdt.EventsCountMap["pages"]; exists {
		t.Logf("Sending modified pages events")
		for i := 0; i < page; i++ {
			payloadPage := strings.NewReader(fmt.Sprintf(ModifiedPagePayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			sendEvent(payloadPage, "page", wdt.WriteKey)
		}
	}

	if screen, exists := wdt.EventsCountMap["screens"]; exists {
		t.Logf("Sending modified screens events")
		for i := 0; i < screen; i++ {
			payloadScreen := strings.NewReader(fmt.Sprintf(ModifiedScreenPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			sendEvent(payloadScreen, "screen", wdt.WriteKey)
		}
	}

	if alias, exists := wdt.EventsCountMap["aliases"]; exists {
		t.Logf("Sending modified aliases events")
		for i := 0; i < alias; i++ {
			payloadAlias := strings.NewReader(fmt.Sprintf(ModifiedAliasPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			sendEvent(payloadAlias, "alias", wdt.WriteKey)
		}
	}

	if group, exists := wdt.EventsCountMap["groups"]; exists {
		t.Logf("Sending modified groups events")
		for i := 0; i < group; i++ {
			payloadGroup := strings.NewReader(fmt.Sprintf(ModifiedGroupPayload, wdt.UserId, uuid.Must(uuid.NewV4()).String()))
			sendEvent(payloadGroup, "group", wdt.WriteKey)
		}
	}

}
