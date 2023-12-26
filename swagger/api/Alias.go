package api

import (
	"net/http"

	"github.com/labstack/echo"
)

// AliasRequest represents the body of an Alias request.
type AliasRequest struct {
	UserId       string                 `json:"userId"`
	PreviousId   string                 `json:"previousId"` // Required field
	Context      map[string]interface{} `json:"context"`
	Integrations map[string]interface{} `json:"integrations"`
	Timestamp    string                 `json:"timestamp"`
	Traits       map[string]interface{} `json:"traits"`
}

// AliasResponse represents the body of an Alias response.
type AliasResponse struct {
	// Add any fields you want to return in the response here
}

// AliasHandler handles incoming Alias requests.
func AliasHandler(ctx echo.Context) error {
	req := AliasRequest{}
	if err := ctx.Bind(&req); err != nil {
		return echo.ErrBadRequest
	}

	// Check for required fields (previousId)
	if req.PreviousId == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Missing required field: previousId")
	}

	resp := AliasResponse{} // Fill in with appropriate response data
	return ctx.JSON(http.StatusOK, resp)
}
