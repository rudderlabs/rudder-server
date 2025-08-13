package static

import "github.com/rudderlabs/rudder-go-kit/logger"

type Logger interface {
	Warnn(msg string, fields ...logger.Field)
}
