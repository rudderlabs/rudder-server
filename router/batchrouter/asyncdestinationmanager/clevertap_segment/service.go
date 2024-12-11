package clevertapSegment

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

type ClevertapServiceImpl struct {
	BulkApi          string
	NotifyApi        string
	ConnectionConfig *ConnectionConfig
}

func (u *ClevertapServiceImpl) MakeHTTPRequest(data *HttpRequestData) ([]byte, int, error) {
	req, err := http.NewRequest(data.Method, data.Endpoint, data.Body)
	if err != nil {
		return nil, 500, err
	}
	req.Header.Add("X-CleverTap-Account-Id", data.appKey)
	req.Header.Add("X-CleverTap-Passcode", data.accessToken)
	req.Header.Add("content-type", data.ContentType)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, 500, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, 500, err
	}
	return body, res.StatusCode, err
}

func (u *ClevertapServiceImpl) UploadBulkFile(filePath, presignedURL string) error {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get the file information
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := fileInfo.Size()

	// Create the PUT request
	req, err := http.NewRequest("PUT", presignedURL, file)
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %w", err)
	}
	req.ContentLength = fileSize
	// Execute the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed, status: %s, response: %s", resp.Status, string(body))
	}
	return nil
}
