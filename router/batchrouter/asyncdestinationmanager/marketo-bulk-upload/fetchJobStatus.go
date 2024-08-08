package marketobulkupload

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

type Job struct {
	Metadata struct {
		JobID int `json:"job_id"`
	} `json:"metadata"`
	Data map[string]interface{} `json:"data"`
}

type FieldSchemaMapping struct{}

type FetchJobStatusResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type Response struct {
	StatusCode int              `json:"statusCode"`
	Metadata   ResponseMetadata `json:"metadata"`
}

type ResponseMetadata struct {
	FailedKeys    []int64           `json:"FailedKeys"`
	FailedReasons map[string]string `json:"FailedReasons"`
	SucceededKeys []int64           `json:"SucceededKeys"`
}

type PlatformError struct {
	Message string
}

func (e *PlatformError) Error() string {
	return e.Message
}

func getJobsStatus(event MarketoAsyncFailedPayload, jobType string, accessToken string) (string, error) {
	config := event.Config
	munchkinId := config["MunchkinId"]
	importId := event.ImportId
	var url string

	headers := map[string]string{
		"Content-Type":  JSONMimeType,
		"Authorization": fmt.Sprintf("Bearer %s", accessToken),
	}

	if jobType == "fail" {
		url = fmt.Sprintf("https://%s.mktorest.com/bulk/v1/leads/batch/%s/failures.json", munchkinId, importId)
	} else {
		url = fmt.Sprintf("https://%s.mktorest.com/bulk/v1/leads/batch/%s/warnings.json", munchkinId, importId)
	}

	startTime := time.Now()
	resp, err := handleHttpRequest("GET", url, headers)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	endTime := time.Now()
	requestTime := endTime.Sub(startTime).Milliseconds()

	fmt.Printf("Request time: %d ms\n", requestTime)

	if !isHttpStatusSuccess(resp.StatusCode) {
		return "", fmt.Errorf("could not fetch job status: received HTTP status %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func handleFetchJobStatusResponse(resp string, jobType string) (string, error) {
	// Handle the response as needed, this function is a placeholder
	return resp, nil
}

func responseHandler(event MarketoAsyncFailedPayload, jobType string) (*Response, error) {
	config := event.Config
	accessToken, err := getAccessToken(config)
	if err != nil {
		return nil, err
	}

	jobStatus, err := getJobsStatus(event, jobType, accessToken)
	if err != nil {
		return nil, err
	}

	jobStatusArr := strings.Split(jobStatus, "\n")
	headerArr := strings.Split(event.MetaData.CSVHeaders, ",")

	if len(headerArr) == 0 {
		return nil, &PlatformError{"No csvHeader in metadata"}
	}

	data := make(map[int64]string)

	fieldSchemaMapping, err := getFieldSchemaMap(accessToken, config["MunchkinId"])
	if err != nil {
		return nil, err
	}

	unsuccessfulJobInfo := checkEventStatusViaSchemaMatching(event, fieldSchemaMapping)
	mismatchJobIdArray := make([]int64, 0, len(unsuccessfulJobInfo))
	for k := range unsuccessfulJobInfo {
		if jobID, err := strconv.ParseInt(k, 10, 64); err == nil {
			mismatchJobIdArray = append(mismatchJobIdArray, jobID)
		}
	}

	reasons := unsuccessfulJobInfo

	filteredInputs := make([]MarketoAsyncFailedInput, 0, len(event.Input))
	for _, input := range event.Input {
		if !contains(mismatchJobIdArray, input.Metadata.JobID) {
			filteredInputs = append(filteredInputs, input)
		}
	}

	for _, input := range filteredInputs {
		responses := make([]string, 0, len(headerArr))
		for _, fieldName := range headerArr {
			if val, ok := input.Message[fieldName]; ok {
				responses = append(responses, fmt.Sprintf("%v", val))
			}
		}
		data[input.Metadata.JobID] = strings.Join(responses, ",")
	}

	unsuccessfulJobIdsArr := make([]int64, 0)
	successfulJobIdsArr := make([]int64, 0)

	for _, element := range jobStatusArr {
		elemArr := parseCSVLine(element)
		reasonMessage := elemArr[len(elemArr)-1]
		elemArr = elemArr[:len(elemArr)-1]

		for key, val := range data {
			if val == strings.Join(elemArr, ",") {
				if !contains(unsuccessfulJobIdsArr, key) {
					unsuccessfulJobIdsArr = append(unsuccessfulJobIdsArr, key)
				}
				reasons[strconv.FormatInt(key, 10)] = reasonMessage
			}
		}
	}

	FailedKeys := append(mismatchJobIdArray, unsuccessfulJobIdsArr...)
	for key := range data {
		if !contains(unsuccessfulJobIdsArr, key) {
			successfulJobIdsArr = append(successfulJobIdsArr, key)
		}
	}
	SucceededKeys := successfulJobIdsArr

	response := &Response{
		StatusCode: 200,
		Metadata: ResponseMetadata{
			FailedKeys:    FailedKeys,
			FailedReasons: reasons,
			SucceededKeys: SucceededKeys,
		},
	}

	return response, nil
}

func contains(arr []int64, val int64) bool {
	for _, item := range arr {
		if item == val {
			return true
		}
	}
	return false
}

func parseCSVLine(line string) []string {
	reader := csv.NewReader(strings.NewReader(line))
	records, _ := reader.Read()
	return records
}

func getFieldSchemaMap(accessToken, munchkinId interface{}) (FieldSchemaMapping, error) {
	// Implement the field schema map fetching logic
	return FieldSchemaMapping{}, nil
}

func checkEventStatusViaSchemaMatching(event MarketoAsyncFailedPayload, fieldSchemaMapping FieldSchemaMapping) map[string]string {
	// Implement the event status checking logic
	return make(map[string]string)
}
