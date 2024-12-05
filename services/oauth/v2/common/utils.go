package common

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

var contentTypePattern = regexp.MustCompile(`text|application/json|application/xml`)

// ParseAccountIDFromTokenURL extracts the account ID from a token endpoint URL
// URL format: /destination/workspaces/{workspaceID}/accounts/{accountID}/token
func ParseAccountIDFromTokenURL(urlStr string) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}

	// Split path into components
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")

	// Look for accounts/{accountID}/token pattern from the end
	length := len(parts)
	if length < 3 || parts[length-1] != "token" || parts[length-3] != "accounts" {
		return "", fmt.Errorf("URL must follow pattern /accounts/{accountID}/token")
	}

	return parts[length-2], nil
}

// ProcessResponse is a helper function to process the response from the control plane
func ProcessResponse(resp *http.Response) (statusCode int, respBody string) {
	var respData []byte
	var ioUtilReadErr error
	if resp != nil && resp.Body != nil {
		respData, ioUtilReadErr = io.ReadAll(resp.Body)
		if ioUtilReadErr != nil {
			return http.StatusInternalServerError, ioUtilReadErr.Error()
		}
	}
	// Detecting content type of the respData
	contentTypeHeader := strings.ToLower(http.DetectContentType(respData))
	// If content type is not of type "*text*", overriding it with empty string
	if !contentTypePattern.MatchString(contentTypeHeader) {
		respData = []byte("")
	}

	return resp.StatusCode, string(respData)
}
