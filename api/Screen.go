package api

import (
	"net/http"

	"github.com/labstack/echo"
)

// ScreenRequest represents the body of a Screen request.
type ScreenRequest struct {
	UserId       string                 `json:"userId"`
	AnonymousId  string                 `json:"anonymousId"`
	Context      map[string]interface{} `json:"context"`
	Integrations map[string]interface{} `json:"integrations"`
	Name         string                 `json:"name"` // Required field
	Properties   map[string]interface{} `json:"properties"`
	Timestamp    string                 `json:"timestamp"`
}

// ScreenResponse represents the body of a Screen response.
type ScreenResponse struct {
	// Add any fields you want to return in the response here
}

// ScreenHandler handles incoming Screen requests.
func ScreenHandler(ctx echo.Context) error {
	req := ScreenRequest{}
	if err := ctx.Bind(&req); err != nil {
		return echo.ErrBadRequest
	}

	// Check for required fields (userId or anonymousId, and name)
	if req.UserId == "" && req.AnonymousId == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Missing required field: userId or anonymousId")
	}
	if req.Name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Missing required field: name")
	}

	resp := ScreenResponse{} // Fill in with appropriate response data
	return ctx.JSON(http.StatusOK, resp)
}
