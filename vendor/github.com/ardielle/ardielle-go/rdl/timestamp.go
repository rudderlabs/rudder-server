// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

//
// Timestamp - a wrapper for time.Time that marshals according to the RDL Timestamp spec
//
type Timestamp struct {
	time.Time
}

//
// Validate - return an error if the timestamp is not valid
//
func (ts Timestamp) Validate() error {
	if ts.IsZero() {
		return fmt.Errorf("Invalid timestamp")
	}
	return nil
}

//
// String - Show the Timestamp in RFC3339 format, UTC, millisecond resolution
//
func (ts Timestamp) String() string {
	if ts.IsZero() {
		return ""
	}
	return fmt.Sprintf("%d-%02d-%02dT%02d:%02d:%02d.%03dZ",
		ts.Year(), ts.Month(), ts.Day(),
		ts.Hour(), ts.Minute(), ts.Second(), ts.Nanosecond()/1000000)
}

//
// MarshalJSON - marshal the Timestamp in RDL format (RFC3339, UTC, millisecond resolution)
//
func (ts Timestamp) MarshalJSON() ([]byte, error) {
	return []byte("\"" + ts.String() + "\""), nil
}

//
// UnmarshalJSON - parse a Timestamp from a JSON string
//
func (ts *Timestamp) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err == nil {
		tsp, err := TimestampParse(string(j))
		if err == nil {
			*ts = tsp
		}
	}
	return err
}

//
// Millis - returns the number of milliseconds, as an int64, since the
// epoch (Jan 1, 1970). This is compatible with Java timestamps.
//
func (ts Timestamp) Millis() int64 {
	nsec := ts.UnixNano()
	return nsec / 1000000
}

//
// Micros - returns the number of microseconds, as an int64, since the
// epoch (Jan 1, 1970).
//
func (ts Timestamp) Micros() int64 {
	nsec := ts.UnixNano()
	return nsec / 1000
}

//
// SecondsSinceEpoch - returns the number of seconds, as a float64, since the
// epoch (Jan 1, 1970). This is compatible with Python timestamps.
//
func (ts Timestamp) SecondsSinceEpoch() float64 {
	return float64(ts.Micros()) / 1000000.0
}

//
// Equal - returns true if two timestamps have the same value, to millisecond accuracy
// The JSON representation of dates is only millisecond resolution.
//
func (ts Timestamp) Equal(ts2 Timestamp) bool {
	return ts.Millis() == ts2.Millis()
}

//
// TimestampNow - returns a Timestamp representing the current time
//
func TimestampNow() Timestamp {
	ts := time.Now().UTC()
	return Timestamp{ts}
}

// TimestampParse - parses the string and returns a Timestamp, or error
//
func TimestampParse(s string) (Timestamp, error) {
	layout := "2006-01-02T15:04:05.999Z"
	t, e := time.Parse(layout, s)
	if e != nil {
		if strings.HasSuffix(s, "+00:00") || strings.HasSuffix(s, "-00:00") {
			t, e = time.Parse(layout, s[:len(s)-6]+"Z")
		} else if strings.HasSuffix(s, "+0000") || strings.HasSuffix(s, "-0000") {
			t, e = time.Parse(layout, s[:len(s)-5]+"Z")
		}
		if e != nil {
			var ts Timestamp
			return ts, e
		}
	}
	return Timestamp{t}, nil
}

//
// NewTimestamp - create a new Timestamp from the specified time.Time
//
func NewTimestamp(t time.Time) Timestamp {
	return Timestamp{t.UTC()}
}

//
// TimestampFromEpoch creates a new Timestamp from the specified time.Time
//
func TimestampFromEpoch(secondsSinceEpoch float64) Timestamp {
	sec := int64(secondsSinceEpoch)
	nsec := int64((secondsSinceEpoch - float64(sec) + 0.0000005) * 1000000000.0)
	return Timestamp{time.Unix(sec, nsec).UTC()}
}
