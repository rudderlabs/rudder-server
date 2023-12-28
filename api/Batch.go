package api

import (
	"net/http"

	"github.com/labstack/echo"
)

// BatchRequest represents the body of a Batch request.
type BatchRequest struct {
	Batch []BatchItem `json:"batch"`
}

// BatchItem represents an item in the Batch request.
type BatchItem struct {
	UserId      string                 `json:"userId,omitempty"`
	AnonymousId string                 `json:"anonymousId,omitempty"`
	Type        string                 `json:"type"` // Required field
	Context     map[string]interface{} `json:"context,omitempty"`
	Timestamp   string                 `json:"timestamp"`
	Event       string                 `json:"event,omitempty"`
	Properties  map[string]interface{} `json:"properties,omitempty"`
	Name        string                 `json:"name,omitempty"`
	GroupID     string                 `json:"groupId,omitempty"`
	Traits      map[string]interface{} `json:"traits,omitempty"`
	PreviousId  string                 `json:"previousId,omitempty"`
}

// BatchResponse represents the body of a Batch response.
type BatchResponse struct {
	// Add any fields you want to return in the response here
}

// BatchHandler handles incoming Batch requests.
func BatchHandler(ctx echo.Context) error {
	req := BatchRequest{}
	if err := ctx.Bind(&req); err != nil {
		return echo.ErrBadRequest
	}

	// Validate the batch items (you may want to add more validation logic)
	if len(req.Batch) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "Batch must contain at least one item")
	}

	// Process the batch items and generate a response if needed
	resp := BatchResponse{} // Fill in with appropriate response data
	return ctx.JSON(http.StatusOK, resp)
}
