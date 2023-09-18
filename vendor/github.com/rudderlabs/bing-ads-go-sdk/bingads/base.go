package bingads

import (
	"encoding/xml"
	"net/http"

	"golang.org/x/oauth2"
)

var (
	EnvelopeNamespace = "http://schemas.xmlsoap.org/soap/envelope/"
	BingNamespace     = "https://bingads.microsoft.com/CampaignManagement/v13"
	AuthEndpoint      = "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
	TokenEndpoint     = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
)

type RequestEnvelope struct {
	XMLName xml.Name `xml:"s:Envelope"`
	EnvNS   string   `xml:"xmlns:i,attr"`
	EnvSS   string   `xml:"xmlns:s,attr"`
	Header  RequestHeader
	Body    RequestBody
}

type SoapResponseBody struct {
	OperationResponse []byte `xml:",innerxml"`
}

type SoapResponseEnvelope struct {
	XMLName xml.Name         `xml:"http://schemas.xmlsoap.org/soap/envelope/ Envelope"`
	Header  TrackingId       `xml:"http://schemas.xmlsoap.org/soap/envelope/ Header"`
	Body    SoapResponseBody `xml:"http://schemas.xmlsoap.org/soap/envelope/ Body"`
}

type TrackingId struct {
	Nil        bool   `xml:"http://www.w3.org/2001/XMLSchema-instance nil,attr"`
	TrackingId string `xml:"https://adcenter.microsoft.com/v8 TrackingId"`
}

type RequestBody struct {
	XMLName xml.Name `xml:"s:Body"`
	Body    interface{}
}

type RequestHeader struct {
	XMLName             xml.Name `xml:"s:Header"`
	BingNS              string   `xml:"xmlns,attr"`
	Action              string
	AuthenticationToken string `xml:"AuthenticationToken,omitempty"`
	CustomerAccountId   string `xml:"CustomerAccountId"`
	CustomerId          string `xml:"CustomerId,omitempty"`
	DeveloperToken      string `xml:"DeveloperToken"`
	Password            string `xml:"Password,omitempty"`
	Username            string `xml:"UserName,omitempty"`
}

type Session struct {
	AccountId      string
	CustomerId     string
	DeveloperToken string
	Username       string
	Password       string
	HTTPClient     HttpClient
	TokenSource    oauth2.TokenSource
}

type HttpClient interface {
	Do(*http.Request) (*http.Response, error)
}

type BulkServiceI interface {
	GetBulkUploadUrl() (*GetBulkUploadUrlResponse, error)
	GetBulkUploadStatus(requestId string) (*GetBulkUploadStatusResponse, error)
	UploadBulkFile(url, filename string) (*UploadBulkFileResponse, error)
}
