package clickhouse

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

// Various errors the driver might return. Can change between driver versions.
var (
	ErrPlaceholderCount  = errors.New("clickhouse: wrong placeholder count")
	ErrNameParams        = errors.New("clickhouse: driver does not support the use of Named Parameters")
	ErrMalformed         = errors.New("clickhouse: response is malformed")
	ErrTransportNil      = errors.New("clickhouse: transport must be set")
	ErrIncorrectResponse = errors.New("clickhouse: response must contain 'Ok.'")
	ErrNoLastInsertID    = errors.New("no LastInsertId available")
	ErrNoRowsAffected    = errors.New("no RowsAffected available")
)

var errorRe = regexp.MustCompile(`(?s)Code: (\d+),.+DB::Exception: (.+),.*`)

// Error contains parsed information about server error
type Error struct {
	Code    int
	Message string
}

// Error implements the interface error
func (e *Error) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

func newError(resp string) error {
	tokens := errorRe.FindStringSubmatch(resp)
	if len(tokens) < 3 {
		return fmt.Errorf("clickhouse: %s", resp)
	}
	code, _ := strconv.ParseInt(tokens[1], 10, 64)
	return &Error{Code: int(code), Message: tokens[2]}
}
