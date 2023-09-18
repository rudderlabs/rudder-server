package bingads

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

type BulkService struct {
	Endpoint string
	Session  *Session
}

func NewBulkService(session *Session) *BulkService {
	return &BulkService{
		Endpoint: "https://bulk.api.bingads.microsoft.com/Api/Advertiser/CampaignManagement/v13/BulkService.svc",
		Session:  session,
	}
}

type GetBulkUploadUrlRequest struct {
	XMLName      xml.Name `xml:"GetBulkUploadUrlRequest"`
	NS           string   `xml:"xmlns,attr"`
	AccountId    int64    `xml:"AccountId"`
	ResponseMode string   `xml:"ResponseMode"`
}

type GetBulkUploadUrlResponse struct {
	UploadUrl string `xml:"UploadUrl"`
	RequestId string `xml:"RequestId"`
}

type GetBulkUploadStatusRequest struct {
	XMLName   xml.Name `xml:"GetBulkUploadStatusRequest"`
	NS        string   `xml:"xmlns,attr"`
	RequestId string   `xml:"RequestId"`
}

type GetBulkUploadStatusResponse struct {
	XMLName         xml.Name   `xml:"GetBulkUploadStatusResponse"`
	NS              string     `xml:"xmlns,attr"`
	PercentComplete int64      `xml:"PercentComplete"`
	RequestStatus   string     `xml:"RequestStatus"`
	ResultFileUrl   string     `xml:"ResultFileUrl"`
	Errors          ErrorsType `xml:"ErrorsType"`
}

// GetBulkUploadUrl
func (c *BulkService) GetBulkUploadUrl() (*GetBulkUploadUrlResponse, error) {
	accountId, _ := strconv.ParseInt(c.Session.AccountId, 10, 64)
	req := GetBulkUploadUrlRequest{
		NS:           BingNamespace,
		AccountId:    accountId,
		ResponseMode: "ErrorsOnly",
	}

	resp, err := c.Session.SendRequest(req, c.Endpoint, "GetBulkUploadUrl")
	if err != nil {
		return nil, err
	}

	var getBulkUploadUrlResponse GetBulkUploadUrlResponse
	if err = xml.Unmarshal(resp, &getBulkUploadUrlResponse); err != nil {
		return nil, err
	}

	if getBulkUploadUrlResponse.RequestId == "" || getBulkUploadUrlResponse.UploadUrl == "" {
		return nil, fmt.Errorf("unable to get bulk upload url, check your credentials")
	}
	return &getBulkUploadUrlResponse, nil
}

// GetBulkUploadUrl
func (c *BulkService) GetBulkUploadStatus(requestId string) (*GetBulkUploadStatusResponse, error) {
	req := GetBulkUploadStatusRequest{
		NS:        BingNamespace,
		RequestId: requestId,
	}

	resp, err := c.Session.SendRequest(req, c.Endpoint, "GetBulkUploadStatus")
	if err != nil {
		return nil, err
	}

	var getBulkUploadStatusResponse GetBulkUploadStatusResponse
	if err = xml.Unmarshal(resp, &getBulkUploadStatusResponse); err != nil {
		return nil, err
	}
	return &getBulkUploadStatusResponse, nil
}

type UploadBulkFileResponse struct {
	TrackingId string `json:"TrackingId"`
	RequestId  string `json:"RequestId"`
}

func (c *BulkService) UploadBulkFile(url, filename string) (*UploadBulkFileResponse, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	part1, err := writer.CreateFormFile("", filepath.Base(filename))
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(part1, file)
	if err != nil {
		return nil, err
	}

	if err = writer.Close(); err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		return nil, err
	}

	token, err := c.Session.TokenSource.Token()
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("AuthenticationToken", token.AccessToken)
	req.Header.Add("DeveloperToken", c.Session.DeveloperToken)
	req.Header.Add("CustomerId", c.Session.CustomerId)
	req.Header.Add("AccountId", c.Session.AccountId)

	resp, err := c.Session.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var uploadBulkFileResponse UploadBulkFileResponse

	if err = json.Unmarshal(body, &uploadBulkFileResponse); err != nil {
		return nil, err
	}

	if uploadBulkFileResponse.RequestId == "" || uploadBulkFileResponse.TrackingId == "" {
		return nil, fmt.Errorf("unable to upload bulk file, check your credentials")
	}
	return &uploadBulkFileResponse, nil
}
