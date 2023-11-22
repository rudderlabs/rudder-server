package testhelper

import "github.com/rudderlabs/rudder-go-kit/logger"

//go:generate mockgen -destination=../mocks/utils/logger/mock_logger.go -package mock_logger github.com/rudderlabs/rudder-go-kit/logger Logger
type Logger = logger.Logger
