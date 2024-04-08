package gateway

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	kituuid "github.com/rudderlabs/rudder-go-kit/uuid"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/response"
)

// ImportRequestHandler is an empty struct to capture import specific request handling functionality
type ImportRequestHandler struct {
	*Handle
}

// ProcessRequest on ImportRequestHandler splits payload by user and throws them into the webrequestQ and waits for all their responses before returning
func (irh *ImportRequestHandler) ProcessRequest(w *http.ResponseWriter, r *http.Request, _ string, payload []byte, arctx *gwtypes.AuthRequestContext) string {
	usersPayload, payloadError := getUsersPayload(payload)
	if payloadError != nil {
		return payloadError.Error()
	}
	count := len(usersPayload)
	done := make(chan string, count)
	for key := range usersPayload {
		irh.addToWebRequestQ(w, r, done, "batch", usersPayload[key], arctx)
	}

	var interimMsgs []string
	for index := 0; index < count; index++ {
		interimErrorMessage := <-done
		interimMsgs = append(interimMsgs, interimErrorMessage)
	}
	return strings.Join(interimMsgs, "")
}

// getPayloadFromRequest reads the request body and returns event payloads grouped by user id
// for performance see: https://github.com/rudderlabs/rudder-server/pull/2040
func getUsersPayload(requestPayload []byte) (map[string][]byte, error) {
	if !gjson.ValidBytes(requestPayload) {
		return make(map[string][]byte), errors.New(response.InvalidJSON)
	}

	result := gjson.GetBytes(requestPayload, "batch")

	var (
		userCnt = make(map[string]int)
		userMap = make(map[string][]byte)
	)
	result.ForEach(func(_, value gjson.Result) bool {
		anonIDFromReq := value.Get("anonymousId").String()
		userIDFromReq := value.Get("userId").String()
		rudderID, err := kituuid.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
		if err != nil {
			return false
		}

		uuidStr := rudderID.String()
		tempValue, ok := userMap[uuidStr]
		if !ok {
			userCnt[uuidStr] = 0
			userMap[uuidStr] = append([]byte(`{"batch":[`), append([]byte(value.Raw), ']', '}')...)
		} else {
			path := "batch." + strconv.Itoa(userCnt[uuidStr]+1)
			raw, err := sjson.SetRaw(string(tempValue), path, value.Raw)
			if err != nil {
				return false
			}
			userCnt[uuidStr]++
			userMap[uuidStr] = []byte(raw)
		}

		return true
	})
	return userMap, nil
}
