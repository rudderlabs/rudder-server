package utils

import (
	"fmt"
	"strconv"
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

func GetMinMaxCreatedAt(key string) (int64, int64, error) {
	var err error
	var minJobCreatedAt, maxJobCreatedAt int64
	keyTokens := strings.Split(key, "_")
	if len(keyTokens) != 3 {
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("%s 's parse with _ gave tokens more than 3. Expected 3", key)
	}
	keyTokens = strings.Split(keyTokens[2], ".")
	if len(keyTokens) > 7 {
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("%s 's parse with . gave tokens more than 7. Expected 6 or 7", keyTokens[2])
	}

	if len(keyTokens) < 6 { // for backward compatibility TODO: remove this check after some time
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("%s 's parse with . gave tokens less than 6. Expected 6 or 7", keyTokens[2])
	}
	minJobCreatedAt, err = strconv.ParseInt(keyTokens[3], 10, 64)
	if err != nil {
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("ParseInt of %s failed with err: %w", keyTokens[3], err)
	}

	maxJobCreatedAt, err = strconv.ParseInt(keyTokens[4], 10, 64)
	if err != nil {
		return minJobCreatedAt, maxJobCreatedAt, fmt.Errorf("ParseInt of %s failed with err: %w", keyTokens[4], err)
	}

	return minJobCreatedAt, maxJobCreatedAt, nil
}
