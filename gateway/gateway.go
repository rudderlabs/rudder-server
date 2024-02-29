package gateway

import (
	_ "embed"
	"errors"
	"regexp"
)

/*
 * The gateway module handles incoming requests from client devices.
 * It batches web requests and writes to DB in bulk to improve I/O.
 * Only after the request payload is persisted, an ACK is sent to
 * the client.
 */

const (
	delimiter                 = "<<>>"
	eventStreamSourceCategory = "eventStream"
	extractEvent              = "extract"
	rETLEvent                 = "record"
	customVal                 = "GW"
)

var (
	semverRegexp = regexp.MustCompile(`^v?([0-9]+)(\.[0-9]+)?(\.[0-9]+)?(-([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?(\+([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?$`)
	batchEvent   = []byte(`
	{
		"batch": [
		]
	}
`)
)

var (
	errRequestDropped    = errors.New("request dropped")
	errRequestSuppressed = errors.New("request suppressed")
	errEventSuppressed   = errors.New("event suppressed")
)

//go:embed openapi/index.html
var openApiSpec []byte
