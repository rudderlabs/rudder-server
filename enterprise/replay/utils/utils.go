package utils

import (
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func GetStartAndEndTime(config *config.Config) (time.Time, time.Time, error) {
	var startTime, endTime time.Time
	parse, err := time.Parse(misc.RFC3339Milli, strings.TrimSpace(config.GetString("START_TIME", "2000-10-02T15:04:05.000Z")))
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	startTime = parse
	endTimeStr := strings.TrimSpace(config.GetString("END_TIME", ""))
	if endTimeStr == "" {
		endTime = time.Now()
	} else {
		endTime, err = time.Parse(misc.RFC3339Milli, endTimeStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid END_TIME. Err: %w", err)
		}
	}
	return startTime, endTime, nil
}
