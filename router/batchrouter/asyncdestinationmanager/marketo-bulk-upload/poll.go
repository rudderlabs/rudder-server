package marketobulkupload

import (
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"io/ioutil"
)

const (
	JSONMimeType = "application/json"
)

type PollStatusResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type PollResponse struct {
	Result []PollStatusResponse `json:"result"`
}

func handlePollResponse(pollStatus *PollResponse) (*common.PollStatusResponse, error) {
	if len(pollStatus.Result) == 0 {
		return nil, fmt.Errorf("no poll response result")
	}

	result := pollStatus.Result[0]
	switch result.Status {
	case "Complete":
		return &common.PollStatusResponse{
			Complete:   true,
			StatusCode: 200,
			HasFailed:  false,
			InProgress: false,
			HasWarning: false,
		}, nil
	case "Importing", "Queued":
		return &common.PollStatusResponse{
			Complete:   false,
			StatusCode: 500,
			HasFailed:  false,
			InProgress: true,
			HasWarning: false,
		}, nil
	case "Failed":
		return &common.PollStatusResponse{
			Complete:   false,
			StatusCode: 500,
			HasFailed:  false,
			InProgress: false,
			HasWarning: false,
			Error:      result.Message,
		}, nil
	default:
		return &common.PollStatusResponse{
			Complete:   false,
			StatusCode: 500,
			HasFailed:  false,
			InProgress: false,
			HasWarning: false,
			Error:      "Unknown status",
		}, nil
	}
}

func getPollStatus(event marketoPollInputStruct) (*PollResponse, error) {
	accessToken, err := getAccessToken(event.DestConfig)
	if err != nil {
		return nil, err
	}

	munchkinId := event.DestConfig["MunchkinId"]
	pollUrl := fmt.Sprintf("https://%s.mktorest.com/bulk/v1/leads/batch/%s.json", munchkinId, event.ImportId)

	headers := map[string]string{
		"Content-Type":  JSONMimeType,
		"Authorization": fmt.Sprintf("Bearer %s", accessToken),
	}

	resp, err := handleHttpRequest("GET", pollUrl, headers)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if !isHttpStatusSuccess(resp.StatusCode) {
		return nil, fmt.Errorf("could not poll status: received HTTP status %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var pollResponse PollResponse
	if err := json.Unmarshal(body, &pollResponse); err != nil {
		return nil, err
	}

	return &pollResponse, nil
}

func pollHandler(event marketoPollInputStruct) (*common.PollStatusResponse, error) {
	pollResp, err := getPollStatus(event)
	if err != nil {
		return nil, err
	}

	if pollResp == nil {
		return &common.PollStatusResponse{
			Complete:   false,
			StatusCode: 500,
			HasFailed:  false,
			InProgress: false,
			HasWarning: false,
			Error:      "No poll response received from Marketo",
		}, nil
	}

	return handlePollResponse(pollResp)
}
