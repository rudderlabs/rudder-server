package timeutil

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// MinsOfDay returns minutes since start of day for a timestamp in format `15:30`
// eg. MinsOfDay("02:30") -> 150
// returns 0 for a timestamp not in format HH:MM
func MinsOfDay(str string) int {
	matched, err := regexp.MatchString("^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$", str)
	if err != nil || !matched {
		return 0
	}
	x := strings.Split(str, ":")
	hrs, _ := strconv.Atoi(x[0])
	mins, _ := strconv.Atoi(x[1])
	return (hrs * 60) + mins
}

// StartOfDay returns start of the day
func StartOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

// Now returns the current time in UTC.
func Now() time.Time {
	return time.Now().UTC()
}

// GetElapsedMinsInThisDay() returns no of minutes elapsed in this day for the time provided
func GetElapsedMinsInThisDay(currentTime time.Time) int {
	hour := currentTime.Hour()
	minute := currentTime.Minute()
	return MinsOfDay(fmt.Sprintf("%02d:%02d", hour, minute))
}
