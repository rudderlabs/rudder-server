package testhelper

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
)

// SampleTestRecordsTemplate returns a set of records for testing default loading scenarios.
// It uses testdata/load.template as the source of data.
func SampleTestRecordsTemplate(recordSetIndex int) [][]string {
	return [][]string{
		{fmt.Sprint(100*recordSetIndex + 1), "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "125.75", "125", "hello-world"},
		{fmt.Sprint(100*recordSetIndex + 2), "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "126.75", "126", "hello-world"},
		{fmt.Sprint(100*recordSetIndex + 3), "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{fmt.Sprint(100*recordSetIndex + 4), "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{fmt.Sprint(100*recordSetIndex + 5), "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{fmt.Sprint(100*recordSetIndex + 6), "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{fmt.Sprint(100*recordSetIndex + 7), "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{fmt.Sprint(100*recordSetIndex + 8), "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{fmt.Sprint(100*recordSetIndex + 9), "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{fmt.Sprint(100*recordSetIndex + 10), "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{fmt.Sprint(100*recordSetIndex + 11), "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{fmt.Sprint(100*recordSetIndex + 12), "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{fmt.Sprint(100*recordSetIndex + 13), "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{fmt.Sprint(100*recordSetIndex + 14), "2022-12-15T06:53:49Z", "", "", "", "", ""},
	}
}

// SampleTestRecords returns a set of records for testing default loading scenarios.
// It uses testdata/load.* as the source of data.
func SampleTestRecords() [][]string {
	return [][]string{
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "126.75", "126", "hello-world"},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "125.75", "125", "hello-world"},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
	}
}

// AppendTestRecords returns a set of records for testing append scenarios.
// It uses testdata/load.* twice as the source of data.
func AppendTestRecords() [][]string {
	return [][]string{
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "126.75", "126", "hello-world"},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "126.75", "126", "hello-world"},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "125.75", "125", "hello-world"},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "125.75", "125", "hello-world"},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
	}
}

// DiscardTestRecords returns a set of records for testing rudder discards.
// It uses testdata/discards.* as the source of data.
func DiscardTestRecords() [][]string {
	return [][]string{
		{"context_screen_density", "125.75", "dummy reason", "2022-12-15T06:53:49Z", "1", "test_table", "2022-12-15T06:53:49Z"},
		{"context_screen_density", "125", "dummy reason", "2022-12-15T06:53:49Z", "2", "test_table", "2022-12-15T06:53:49Z"},
		{"context_screen_density", "true", "dummy reason", "2022-12-15T06:53:49Z", "3", "test_table", "2022-12-15T06:53:49Z"},
		{"context_screen_density", "7274e5db-f918-4efe-1212-872f66e235c5", "dummy reason", "2022-12-15T06:53:49Z", "4", "test_table", "2022-12-15T06:53:49Z"},
		{"context_screen_density", "hello-world", "dummy reason", "2022-12-15T06:53:49Z", "5", "test_table", "2022-12-15T06:53:49Z"},
		{"context_screen_density", "2022-12-15T06:53:49Z", "dummy reason", "2022-12-15T06:53:49Z", "6", "test_table", "2022-12-15T06:53:49Z"},
	}
}

// DedupTestRecords returns a set of records for testing deduplication scenarios.
// It uses testdata/dedup.* as the source of data.
func DedupTestRecords() [][]string {
	return [][]string{
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
	}
}

// DedupTwiceTestRecords returns a set of records for testing deduplication scenarios.
// It uses testdata/dedup.* twice as the source of data.
func DedupTwiceTestRecords() [][]string {
	return [][]string{
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
	}
}

// MismatchSchemaTestRecords returns a set of records for testing schema mismatch scenarios.
// It uses testdata/mismatch-schema.* as the source of data.
func MismatchSchemaTestRecords() [][]string {
	return [][]string{
		{"6734e5db-f918-4efe-1421-872f66e235c5", "", "", "", "", "", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "126.75", "126", "hello-world"},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "125.75", "125", "hello-world"},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
	}
}

// UploadJobIdentifiesRecords returns a set of records for testing upload job identifies scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
func UploadJobIdentifiesRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+7@example.com", "2023-05-12T04:44:48Z", "[::1]", "", "2023-05-12T04:44:48Z", "2023-05-12T04:44:48Z", destType, "2023-05-12T04:44:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+7@example.com", destID, "13bf264c-1b82-48a9-9718-d2d111aba2e2"},
		{userIDFormat, uuidTS, "2", "non escaped column", "Richard Hendricks", "2", "rhedricks+8@example.com", "2023-05-12T04:50:48Z", "[::1]", "non escaped column", "2023-05-12T04:50:48Z", "2023-05-12T04:50:48Z", destType, "2023-05-12T04:50:48Z", "HTTP", "non escaped column", sourceID, "Richard Hendricks", "[::1]", "non escaped column", "rhedricks+8@example.com", destID, "3a79a351-dc6c-43f3-b761-2aae5bccf80a"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+5@example.com", "2023-05-12T04:32:48Z", "[::1]", "", "2023-05-12T04:32:48Z", "2023-05-12T04:32:48Z", destType, "2023-05-12T04:32:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+5@example.com", destID, "5519f8c0-429e-41b1-a314-b1297f43b61c"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+6@example.com", "2023-05-12T04:38:48Z", "[::1]", "", "2023-05-12T04:38:48Z", "2023-05-12T04:38:48Z", destType, "2023-05-12T04:38:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+6@example.com", destID, "89fbe252-a3ed-41c8-b9f3-f19f7e9ec863"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+3@example.com", "2023-05-12T04:20:48Z", "[::1]", "", "2023-05-12T04:20:48Z", "2023-05-12T04:20:48Z", destType, "2023-05-12T04:20:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+3@example.com", destID, "4e0977c5-5d53-4c34-bb4b-a418ad308e6f"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+2@example.com", "2023-05-12T04:14:48Z", "[::1]", "", "2023-05-12T04:14:48Z", "2023-05-12T04:14:48Z", destType, "2023-05-12T04:14:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+2@example.com", destID, "56519cd6-5e53-4cec-aa1d-a00dc9a940e6"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+1@example.com", "2023-05-12T04:08:48Z", "[::1]", "", "2023-05-12T04:08:48Z", "2023-05-12T04:08:48Z", destType, "2023-05-12T04:08:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+1@example.com", destID, "c7c64e80-7da1-4a56-ab61-72778fb66d8c"},
		{userIDFormat, uuidTS, "2", "non escaped column", "Richard Hendricks", "2", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", "[::1]", "non escaped column", "2023-05-12T04:26:48Z", "2023-05-12T04:26:48Z", destType, "2023-05-12T04:26:48Z", "HTTP", "non escaped column", sourceID, "Richard Hendricks", "[::1]", "non escaped column", "rhedricks+4@example.com", destID, "d0e00bef-94c0-4cc2-b681-2b0579021229"},
	}
}

// UploadJobIdentifiesAppendRecords returns a set of records for testing upload job identifies scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobIdentifiesAppendRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+3@example.com", "2023-05-12T04:20:48Z", "[::1]", "", "2023-05-12T04:20:48Z", "2023-05-12T04:20:48Z", destType, "2023-05-12T04:20:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+3@example.com", destID, "4e0977c5-5d53-4c34-bb4b-a418ad308e6f"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+3@example.com", "2023-05-12T04:20:48Z", "[::1]", "", "2023-05-12T04:20:48Z", "2023-05-12T04:20:48Z", destType, "2023-05-12T04:20:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+3@example.com", destID, "4e0977c5-5d53-4c34-bb4b-a418ad308e6f"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+2@example.com", "2023-05-12T04:14:48Z", "[::1]", "", "2023-05-12T04:14:48Z", "2023-05-12T04:14:48Z", destType, "2023-05-12T04:14:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+2@example.com", destID, "56519cd6-5e53-4cec-aa1d-a00dc9a940e6"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+2@example.com", "2023-05-12T04:14:48Z", "[::1]", "", "2023-05-12T04:14:48Z", "2023-05-12T04:14:48Z", destType, "2023-05-12T04:14:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+2@example.com", destID, "56519cd6-5e53-4cec-aa1d-a00dc9a940e6"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+1@example.com", "2023-05-12T04:08:48Z", "[::1]", "", "2023-05-12T04:08:48Z", "2023-05-12T04:08:48Z", destType, "2023-05-12T04:08:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+1@example.com", destID, "c7c64e80-7da1-4a56-ab61-72778fb66d8c"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+1@example.com", "2023-05-12T04:08:48Z", "[::1]", "", "2023-05-12T04:08:48Z", "2023-05-12T04:08:48Z", destType, "2023-05-12T04:08:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+1@example.com", destID, "c7c64e80-7da1-4a56-ab61-72778fb66d8c"},
		{userIDFormat, uuidTS, "2", "non escaped column", "Richard Hendricks", "2", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", "[::1]", "non escaped column", "2023-05-12T04:26:48Z", "2023-05-12T04:26:48Z", destType, "2023-05-12T04:26:48Z", "HTTP", "non escaped column", sourceID, "Richard Hendricks", "[::1]", "non escaped column", "rhedricks+4@example.com", destID, "d0e00bef-94c0-4cc2-b681-2b0579021229"},
		{userIDFormat, uuidTS, "2", "non escaped column", "Richard Hendricks", "2", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", "[::1]", "non escaped column", "2023-05-12T04:26:48Z", "2023-05-12T04:26:48Z", destType, "2023-05-12T04:26:48Z", "HTTP", "non escaped column", sourceID, "Richard Hendricks", "[::1]", "non escaped column", "rhedricks+4@example.com", destID, "d0e00bef-94c0-4cc2-b681-2b0579021229"},
	}
}

// UploadJobIdentifiesMergeRecords returns a set of records for testing upload job identifies scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobIdentifiesMergeRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+3@example.com", "2023-05-12T04:20:48Z", "[::1]", "", "2023-05-12T04:20:48Z", "2023-05-12T04:20:48Z", destType, "2023-05-12T04:20:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+3@example.com", destID, "4e0977c5-5d53-4c34-bb4b-a418ad308e6f"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+2@example.com", "2023-05-12T04:14:48Z", "[::1]", "", "2023-05-12T04:14:48Z", "2023-05-12T04:14:48Z", destType, "2023-05-12T04:14:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+2@example.com", destID, "56519cd6-5e53-4cec-aa1d-a00dc9a940e6"},
		{userIDFormat, uuidTS, "2", "", "Richard Hendricks", "2", "rhedricks+1@example.com", "2023-05-12T04:08:48Z", "[::1]", "", "2023-05-12T04:08:48Z", "2023-05-12T04:08:48Z", destType, "2023-05-12T04:08:48Z", "HTTP", "", sourceID, "Richard Hendricks", "[::1]", "", "rhedricks+1@example.com", destID, "c7c64e80-7da1-4a56-ab61-72778fb66d8c"},
		{userIDFormat, uuidTS, "2", "non escaped column", "Richard Hendricks", "2", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", "[::1]", "non escaped column", "2023-05-12T04:26:48Z", "2023-05-12T04:26:48Z", destType, "2023-05-12T04:26:48Z", "HTTP", "non escaped column", sourceID, "Richard Hendricks", "[::1]", "non escaped column", "rhedricks+4@example.com", destID, "d0e00bef-94c0-4cc2-b681-2b0579021229"},
	}
}

// UploadJobUsersRecords returns a set of records for testing upload job users scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
func UploadJobUsersRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "2023-05-12T04:26:48Z", "2", "[::1]", "non escaped column", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", destID, "rhedricks+4@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:26:48Z", "Richard Hendricks", "2023-05-12T04:26:48Z"},
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "2023-05-12T04:50:48Z", "2", "[::1]", "non escaped column", "rhedricks+8@example.com", "2023-05-12T04:50:48Z", destID, "rhedricks+8@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:50:48Z", "Richard Hendricks", "2023-05-12T04:50:48Z"},
	}
}

// UploadJobUsersRecordsForDatalake returns a set of records for testing upload job users scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
// sent_at, timestamp, original_timestamp will not be present in the records.
func UploadJobUsersRecordsForDatalake(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{sourceID, destType, "[::1]", "Richard Hendricks", "", "", "2", "", "2", "[::1]", "", "rhedricks+1@example.com", "", destID, "rhedricks+1@example.com", "", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:08:48Z", "Richard Hendricks", ""},
		{sourceID, destType, "[::1]", "Richard Hendricks", "", "", "2", "", "2", "[::1]", "", "rhedricks+2@example.com", "", destID, "rhedricks+2@example.com", "", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:14:48Z", "Richard Hendricks", ""},
		{sourceID, destType, "[::1]", "Richard Hendricks", "", "", "2", "", "2", "[::1]", "", "rhedricks+3@example.com", "", destID, "rhedricks+3@example.com", "", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:20:48Z", "Richard Hendricks", ""},
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "", "2", "[::1]", "non escaped column", "rhedricks+4@example.com", "", destID, "rhedricks+4@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:26:48Z", "Richard Hendricks", ""},
		{sourceID, destType, "[::1]", "Richard Hendricks", "", "", "2", "", "2", "[::1]", "", "rhedricks+5@example.com", "", destID, "rhedricks+5@example.com", "", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:32:48Z", "Richard Hendricks", ""},
		{sourceID, destType, "[::1]", "Richard Hendricks", "", "", "2", "", "2", "[::1]", "", "rhedricks+6@example.com", "", destID, "rhedricks+6@example.com", "", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:38:48Z", "Richard Hendricks", ""},
		{sourceID, destType, "[::1]", "Richard Hendricks", "", "", "2", "", "2", "[::1]", "", "rhedricks+7@example.com", "", destID, "rhedricks+7@example.com", "", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:44:48Z", "Richard Hendricks", ""},
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "", "2", "[::1]", "non escaped column", "rhedricks+8@example.com", "", destID, "rhedricks+8@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:50:48Z", "Richard Hendricks", ""},
	}
}

// UploadJobUsersAppendRecords returns a set of records for testing upload job users scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobUsersAppendRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "2023-05-12T04:26:48Z", "2", "[::1]", "non escaped column", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", destID, "rhedricks+4@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:26:48Z", "Richard Hendricks", "2023-05-12T04:26:48Z"},
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "2023-05-12T04:26:48Z", "2", "[::1]", "non escaped column", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", destID, "rhedricks+4@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:26:48Z", "Richard Hendricks", "2023-05-12T04:26:48Z"},
	}
}

// UploadJobUsersAppendRecordsUsingUsersLoadFiles returns a set of records for testing upload job users scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobUsersAppendRecordsUsingUsersLoadFiles(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "2023-05-12T04:26:48Z", "2", "[::1]", "non escaped column", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", destID, "rhedricks+4@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:26:48Z", "Richard Hendricks", "2023-05-12T04:26:48Z"},
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "2023-05-12T04:26:48Z", "2", "[::1]", "non escaped column", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", destID, "rhedricks+4@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:26:48Z", "Richard Hendricks", "2023-05-12T04:26:48Z"},
	}
}

// UploadJobUsersMergeRecord returns a set of records for testing upload job users scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobUsersMergeRecord(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "2023-05-12T04:26:48Z", "2", "[::1]", "non escaped column", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", destID, "rhedricks+4@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:26:48Z", "Richard Hendricks", "2023-05-12T04:26:48Z"},
	}
}

// UploadJobUsersRecordsUsingUsersLoadFilesForClickhouse returns a set of records for testing upload job users scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
// For AggregatingMergeTree ClickHouse replaces all rows with the same primary key (or more accurately, with the same sorting key) with a single row (within a one data part) that stores a combination of states of aggregate functions.
// So received_at will be record for the first record only.
func UploadJobUsersRecordsUsingUsersLoadFilesForClickhouse(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "2023-05-12T04:26:48Z", "2", "[::1]", "non escaped column", "rhedricks+4@example.com", "2023-05-12T04:26:48Z", destID, "rhedricks+4@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:08:48Z", "Richard Hendricks", "2023-05-12T04:26:48Z"},
		{sourceID, destType, "[::1]", "Richard Hendricks", "non escaped column", "non escaped column", "2", "2023-05-12T04:50:48Z", "2", "[::1]", "non escaped column", "rhedricks+8@example.com", "2023-05-12T04:50:48Z", destID, "rhedricks+8@example.com", "non escaped column", "HTTP", userIDFormat, uuidTS, "2023-05-12T04:32:48Z", "Richard Hendricks", "2023-05-12T04:50:48Z"},
	}
}

// UploadJobTracksRecords returns a set of records for testing upload job tracks scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
func UploadJobTracksRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{"2023-05-12T04:45:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:45:48Z", "0acd70af-2afe-4eb4-b092-cf3dda54c681", "product_track", "2023-05-12T04:45:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:45:48Z", userIDFormat},
		{"2023-05-12T04:33:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:33:48Z", "1b8f4e80-37a6-453c-9368-2ee090dd8fa4", "product_track", "2023-05-12T04:33:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:33:48Z", userIDFormat},
		{"2023-05-12T04:51:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:51:48Z", "9a072d06-c7de-43da-83c6-4afd700940d1", "product_track", "2023-05-12T04:51:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:51:48Z", userIDFormat},
		{"2023-05-12T04:09:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:09:48Z", "40f30073-9995-4574-8b11-9256f6276164", "product_track", "2023-05-12T04:09:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:09:48Z", userIDFormat},
		{"2023-05-12T04:39:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:39:48Z", "824f7653-3d75-4e78-8d9d-e936df497f13", "product_track", "2023-05-12T04:39:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:39:48Z", userIDFormat},
		{"2023-05-12T04:27:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:27:48Z", "aa9d12d9-69c8-4385-9348-dd80ab166bc7", "product_track", "2023-05-12T04:27:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:27:48Z", userIDFormat},
		{"2023-05-12T04:15:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:15:48Z", "c7556798-10fe-4135-9f36-c9d51613ee3b", "product_track", "2023-05-12T04:15:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:15:48Z", userIDFormat},
		{"2023-05-12T04:21:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:21:48Z", "faa2440d-7d5f-4922-957b-d61a65c8a268", "product_track", "2023-05-12T04:21:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:21:48Z", userIDFormat},
	}
}

// UploadJobTracksAppendRecords returns a set of records for testing upload job tracks scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobTracksAppendRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{"2023-05-12T04:09:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:09:48Z", "40f30073-9995-4574-8b11-9256f6276164", "product_track", "2023-05-12T04:09:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:09:48Z", userIDFormat},
		{"2023-05-12T04:09:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:09:48Z", "40f30073-9995-4574-8b11-9256f6276164", "product_track", "2023-05-12T04:09:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:09:48Z", userIDFormat},
		{"2023-05-12T04:27:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:27:48Z", "aa9d12d9-69c8-4385-9348-dd80ab166bc7", "product_track", "2023-05-12T04:27:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:27:48Z", userIDFormat},
		{"2023-05-12T04:27:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:27:48Z", "aa9d12d9-69c8-4385-9348-dd80ab166bc7", "product_track", "2023-05-12T04:27:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:27:48Z", userIDFormat},
		{"2023-05-12T04:15:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:15:48Z", "c7556798-10fe-4135-9f36-c9d51613ee3b", "product_track", "2023-05-12T04:15:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:15:48Z", userIDFormat},
		{"2023-05-12T04:15:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:15:48Z", "c7556798-10fe-4135-9f36-c9d51613ee3b", "product_track", "2023-05-12T04:15:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:15:48Z", userIDFormat},
		{"2023-05-12T04:21:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:21:48Z", "faa2440d-7d5f-4922-957b-d61a65c8a268", "product_track", "2023-05-12T04:21:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:21:48Z", userIDFormat},
		{"2023-05-12T04:21:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:21:48Z", "faa2440d-7d5f-4922-957b-d61a65c8a268", "product_track", "2023-05-12T04:21:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:21:48Z", userIDFormat},
	}
}

// UploadJobTracksMergeRecords returns a set of records for testing upload job tracks scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobTracksMergeRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{"2023-05-12T04:09:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:09:48Z", "40f30073-9995-4574-8b11-9256f6276164", "product_track", "2023-05-12T04:09:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:09:48Z", userIDFormat},
		{"2023-05-12T04:27:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:27:48Z", "aa9d12d9-69c8-4385-9348-dd80ab166bc7", "product_track", "2023-05-12T04:27:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:27:48Z", userIDFormat},
		{"2023-05-12T04:15:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:15:48Z", "c7556798-10fe-4135-9f36-c9d51613ee3b", "product_track", "2023-05-12T04:15:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:15:48Z", userIDFormat},
		{"2023-05-12T04:21:48Z", destID, destType, uuidTS, "HTTP", "2023-05-12T04:21:48Z", "faa2440d-7d5f-4922-957b-d61a65c8a268", "product_track", "2023-05-12T04:21:48Z", "[::1]", "Product Track", sourceID, "[::1]", "2023-05-12T04:21:48Z", userIDFormat},
	}
}

// UploadJobProductTrackRecords returns a set of records for testing upload job product track scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
func UploadJobProductTrackRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{"2023-05-12T04:45:48Z", userIDFormat, "7", "2023-05-12T04:45:48Z", sourceID, "2023-05-12T04:45:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:45:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "0acd70af-2afe-4eb4-b092-cf3dda54c681", "product_track", "3"},
		{"2023-05-12T04:33:48Z", userIDFormat, "5", "2023-05-12T04:33:48Z", sourceID, "2023-05-12T04:33:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:33:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "1b8f4e80-37a6-453c-9368-2ee090dd8fa4", "product_track", "3"},
		{"2023-05-12T04:51:48Z", userIDFormat, "8", "2023-05-12T04:51:48Z", sourceID, "2023-05-12T04:51:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:51:48Z", "[::1]", destID, uuidTS, "non escaped column", "OK for the price. It works but the material feels flimsy.", "non escaped column", "86ac1cd43", "Product Track", "9a072d06-c7de-43da-83c6-4afd700940d1", "product_track", "3"},
		{"2023-05-12T04:09:48Z", userIDFormat, "1", "2023-05-12T04:09:48Z", sourceID, "2023-05-12T04:09:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:09:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "40f30073-9995-4574-8b11-9256f6276164", "product_track", "3"},
		{"2023-05-12T04:39:48Z", userIDFormat, "6", "2023-05-12T04:39:48Z", sourceID, "2023-05-12T04:39:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:39:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "824f7653-3d75-4e78-8d9d-e936df497f13", "product_track", "3"},
		{"2023-05-12T04:27:48Z", userIDFormat, "4", "2023-05-12T04:27:48Z", sourceID, "2023-05-12T04:27:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:27:48Z", "[::1]", destID, uuidTS, "non escaped column", "OK for the price. It works but the material feels flimsy.", "non escaped column", "86ac1cd43", "Product Track", "aa9d12d9-69c8-4385-9348-dd80ab166bc7", "product_track", "3"},
		{"2023-05-12T04:15:48Z", userIDFormat, "2", "2023-05-12T04:15:48Z", sourceID, "2023-05-12T04:15:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:15:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "c7556798-10fe-4135-9f36-c9d51613ee3b", "product_track", "3"},
		{"2023-05-12T04:21:48Z", userIDFormat, "3", "2023-05-12T04:21:48Z", sourceID, "2023-05-12T04:21:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:21:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "faa2440d-7d5f-4922-957b-d61a65c8a268", "product_track", "3"},
	}
}

// UploadJobProductTrackAppendRecords returns a set of records for testing upload job product track scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobProductTrackAppendRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{"2023-05-12T04:09:48Z", userIDFormat, "1", "2023-05-12T04:09:48Z", sourceID, "2023-05-12T04:09:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:09:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "40f30073-9995-4574-8b11-9256f6276164", "product_track", "3"},
		{"2023-05-12T04:09:48Z", userIDFormat, "1", "2023-05-12T04:09:48Z", sourceID, "2023-05-12T04:09:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:09:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "40f30073-9995-4574-8b11-9256f6276164", "product_track", "3"},
		{"2023-05-12T04:27:48Z", userIDFormat, "4", "2023-05-12T04:27:48Z", sourceID, "2023-05-12T04:27:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:27:48Z", "[::1]", destID, uuidTS, "non escaped column", "OK for the price. It works but the material feels flimsy.", "non escaped column", "86ac1cd43", "Product Track", "aa9d12d9-69c8-4385-9348-dd80ab166bc7", "product_track", "3"},
		{"2023-05-12T04:27:48Z", userIDFormat, "4", "2023-05-12T04:27:48Z", sourceID, "2023-05-12T04:27:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:27:48Z", "[::1]", destID, uuidTS, "non escaped column", "OK for the price. It works but the material feels flimsy.", "non escaped column", "86ac1cd43", "Product Track", "aa9d12d9-69c8-4385-9348-dd80ab166bc7", "product_track", "3"},
		{"2023-05-12T04:15:48Z", userIDFormat, "2", "2023-05-12T04:15:48Z", sourceID, "2023-05-12T04:15:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:15:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "c7556798-10fe-4135-9f36-c9d51613ee3b", "product_track", "3"},
		{"2023-05-12T04:15:48Z", userIDFormat, "2", "2023-05-12T04:15:48Z", sourceID, "2023-05-12T04:15:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:15:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "c7556798-10fe-4135-9f36-c9d51613ee3b", "product_track", "3"},
		{"2023-05-12T04:21:48Z", userIDFormat, "3", "2023-05-12T04:21:48Z", sourceID, "2023-05-12T04:21:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:21:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "faa2440d-7d5f-4922-957b-d61a65c8a268", "product_track", "3"},
		{"2023-05-12T04:21:48Z", userIDFormat, "3", "2023-05-12T04:21:48Z", sourceID, "2023-05-12T04:21:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:21:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "faa2440d-7d5f-4922-957b-d61a65c8a268", "product_track", "3"},
	}
}

// UploadJobProductTrackMergeRecords returns a set of records for testing upload job product track scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobProductTrackMergeRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{"2023-05-12T04:09:48Z", userIDFormat, "1", "2023-05-12T04:09:48Z", sourceID, "2023-05-12T04:09:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:09:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "40f30073-9995-4574-8b11-9256f6276164", "product_track", "3"},
		{"2023-05-12T04:27:48Z", userIDFormat, "4", "2023-05-12T04:27:48Z", sourceID, "2023-05-12T04:27:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:27:48Z", "[::1]", destID, uuidTS, "non escaped column", "OK for the price. It works but the material feels flimsy.", "non escaped column", "86ac1cd43", "Product Track", "aa9d12d9-69c8-4385-9348-dd80ab166bc7", "product_track", "3"},
		{"2023-05-12T04:15:48Z", userIDFormat, "2", "2023-05-12T04:15:48Z", sourceID, "2023-05-12T04:15:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:15:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "c7556798-10fe-4135-9f36-c9d51613ee3b", "product_track", "3"},
		{"2023-05-12T04:21:48Z", userIDFormat, "3", "2023-05-12T04:21:48Z", sourceID, "2023-05-12T04:21:48Z", "HTTP", "[::1]", destType, "2023-05-12T04:21:48Z", "[::1]", destID, uuidTS, "", "OK for the price. It works but the material feels flimsy.", "", "86ac1cd43", "Product Track", "faa2440d-7d5f-4922-957b-d61a65c8a268", "product_track", "3"},
	}
}

// UploadJobPagesRecords returns a set of records for testing upload job pages scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
func UploadJobPagesRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{userIDFormat, sourceID, "1f56e6fc-5be6-40d2-a0d3-14015b293919", "Home | RudderStack | 6", "2023-05-12T04:40:48Z", "HTTP", "", "2023-05-12T04:40:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:40:48Z", "", "[::1]", "2023-05-12T04:40:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "86b9cabc-edea-48ce-aee1-f23ea71e1940", "Home | RudderStack | 1", "2023-05-12T04:10:48Z", "HTTP", "", "2023-05-12T04:10:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:10:48Z", "", "[::1]", "2023-05-12T04:10:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "96088371-cbd7-46e5-932b-09ff3f74a147", "Home | RudderStack | 5", "2023-05-12T04:34:48Z", "HTTP", "", "2023-05-12T04:34:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:34:48Z", "", "[::1]", "2023-05-12T04:34:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "ca5a3529-a90e-4019-929c-422cdb453e30", "Home | RudderStack | 7", "2023-05-12T04:46:48Z", "HTTP", "", "2023-05-12T04:46:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:46:48Z", "", "[::1]", "2023-05-12T04:46:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "d1d643cc-524d-498d-a4a4-d7227753353c", "Home | RudderStack | 3", "2023-05-12T04:22:48Z", "HTTP", "", "2023-05-12T04:22:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:22:48Z", "", "[::1]", "2023-05-12T04:22:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "d952bb46-1342-4490-9a02-2b016d7bce3d", "Home | RudderStack | 4", "2023-05-12T04:28:48Z", "HTTP", "non escaped column", "2023-05-12T04:28:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:28:48Z", "non escaped column", "[::1]", "2023-05-12T04:28:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "fe41c062-73cb-4c05-aa3b-5e8a4e1575ff", "Home | RudderStack | 2", "2023-05-12T04:16:48Z", "HTTP", "", "2023-05-12T04:16:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:16:48Z", "", "[::1]", "2023-05-12T04:16:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "ff4c965e-f2b3-4f9a-b5a2-d540d72d4535", "Home | RudderStack | 8", "2023-05-12T04:52:48Z", "HTTP", "non escaped column", "2023-05-12T04:52:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:52:48Z", "non escaped column", "[::1]", "2023-05-12T04:52:48Z", "http://www.rudderstack.com", uuidTS},
	}
}

// UploadJobPagesAppendRecords returns a set of records for testing upload job pages scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobPagesAppendRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{userIDFormat, sourceID, "86b9cabc-edea-48ce-aee1-f23ea71e1940", "Home | RudderStack | 1", "2023-05-12T04:10:48Z", "HTTP", "", "2023-05-12T04:10:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:10:48Z", "", "[::1]", "2023-05-12T04:10:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "86b9cabc-edea-48ce-aee1-f23ea71e1940", "Home | RudderStack | 1", "2023-05-12T04:10:48Z", "HTTP", "", "2023-05-12T04:10:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:10:48Z", "", "[::1]", "2023-05-12T04:10:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "d1d643cc-524d-498d-a4a4-d7227753353c", "Home | RudderStack | 3", "2023-05-12T04:22:48Z", "HTTP", "", "2023-05-12T04:22:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:22:48Z", "", "[::1]", "2023-05-12T04:22:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "d1d643cc-524d-498d-a4a4-d7227753353c", "Home | RudderStack | 3", "2023-05-12T04:22:48Z", "HTTP", "", "2023-05-12T04:22:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:22:48Z", "", "[::1]", "2023-05-12T04:22:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "d952bb46-1342-4490-9a02-2b016d7bce3d", "Home | RudderStack | 4", "2023-05-12T04:28:48Z", "HTTP", "non escaped column", "2023-05-12T04:28:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:28:48Z", "non escaped column", "[::1]", "2023-05-12T04:28:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "d952bb46-1342-4490-9a02-2b016d7bce3d", "Home | RudderStack | 4", "2023-05-12T04:28:48Z", "HTTP", "non escaped column", "2023-05-12T04:28:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:28:48Z", "non escaped column", "[::1]", "2023-05-12T04:28:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "fe41c062-73cb-4c05-aa3b-5e8a4e1575ff", "Home | RudderStack | 2", "2023-05-12T04:16:48Z", "HTTP", "", "2023-05-12T04:16:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:16:48Z", "", "[::1]", "2023-05-12T04:16:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "fe41c062-73cb-4c05-aa3b-5e8a4e1575ff", "Home | RudderStack | 2", "2023-05-12T04:16:48Z", "HTTP", "", "2023-05-12T04:16:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:16:48Z", "", "[::1]", "2023-05-12T04:16:48Z", "http://www.rudderstack.com", uuidTS},
	}
}

// UploadJobPagesMergeRecords returns a set of records for testing upload job pages scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobPagesMergeRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{userIDFormat, sourceID, "86b9cabc-edea-48ce-aee1-f23ea71e1940", "Home | RudderStack | 1", "2023-05-12T04:10:48Z", "HTTP", "", "2023-05-12T04:10:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:10:48Z", "", "[::1]", "2023-05-12T04:10:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "d1d643cc-524d-498d-a4a4-d7227753353c", "Home | RudderStack | 3", "2023-05-12T04:22:48Z", "HTTP", "", "2023-05-12T04:22:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:22:48Z", "", "[::1]", "2023-05-12T04:22:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "d952bb46-1342-4490-9a02-2b016d7bce3d", "Home | RudderStack | 4", "2023-05-12T04:28:48Z", "HTTP", "non escaped column", "2023-05-12T04:28:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:28:48Z", "non escaped column", "[::1]", "2023-05-12T04:28:48Z", "http://www.rudderstack.com", uuidTS},
		{userIDFormat, sourceID, "fe41c062-73cb-4c05-aa3b-5e8a4e1575ff", "Home | RudderStack | 2", "2023-05-12T04:16:48Z", "HTTP", "", "2023-05-12T04:16:48Z", destID, "[::1]", destType, "Home", "2023-05-12T04:16:48Z", "", "[::1]", "2023-05-12T04:16:48Z", "http://www.rudderstack.com", uuidTS},
	}
}

// UploadJobScreensRecords returns a set of records for testing upload job screens scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
func UploadJobScreensRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 3", "2023-05-12T04:23:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "088860cf-64d2-41c4-8347-19e6b216a91f", "2023-05-12T04:23:48Z", destID, "2023-05-12T04:23:48Z", "2023-05-12T04:23:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 8", "2023-05-12T04:53:48Z", userIDFormat, "non escaped column", "[::1]", "Main", "[::1]", uuidTS, sourceID, "16d73325-0eea-4ae7-bc0c-7c9913c1d71a", "2023-05-12T04:53:48Z", destID, "2023-05-12T04:53:48Z", "2023-05-12T04:53:48Z", "non escaped column"},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 2", "2023-05-12T04:17:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "28c7a962-02ea-431e-9313-4c30801d048a", "2023-05-12T04:17:48Z", destID, "2023-05-12T04:17:48Z", "2023-05-12T04:17:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 5", "2023-05-12T04:35:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "6c5d48a9-116d-460a-baa4-d42a861c8966", "2023-05-12T04:35:48Z", destID, "2023-05-12T04:35:48Z", "2023-05-12T04:35:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 7", "2023-05-12T04:47:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "788d7056-a605-448f-8877-c6972c1316d8", "2023-05-12T04:47:48Z", destID, "2023-05-12T04:47:48Z", "2023-05-12T04:47:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 4", "2023-05-12T04:29:48Z", userIDFormat, "non escaped column", "[::1]", "Main", "[::1]", uuidTS, sourceID, "7a94a3a8-e343-449d-9a57-3cce7b4a8386", "2023-05-12T04:29:48Z", destID, "2023-05-12T04:29:48Z", "2023-05-12T04:29:48Z", "non escaped column"},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 1", "2023-05-12T04:11:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "98c2e2a8-1304-45b1-a1e1-07d9d3249b84", "2023-05-12T04:11:48Z", destID, "2023-05-12T04:11:48Z", "2023-05-12T04:11:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 6", "2023-05-12T04:41:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "e463cb40-9326-4b22-bd76-b01dab49ef0e", "2023-05-12T04:41:48Z", destID, "2023-05-12T04:41:48Z", "2023-05-12T04:41:48Z", ""},
	}
}

// UploadJobScreensAppendRecords returns a set of records for testing upload job screens scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobScreensAppendRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 3", "2023-05-12T04:23:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "088860cf-64d2-41c4-8347-19e6b216a91f", "2023-05-12T04:23:48Z", destID, "2023-05-12T04:23:48Z", "2023-05-12T04:23:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 3", "2023-05-12T04:23:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "088860cf-64d2-41c4-8347-19e6b216a91f", "2023-05-12T04:23:48Z", destID, "2023-05-12T04:23:48Z", "2023-05-12T04:23:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 2", "2023-05-12T04:17:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "28c7a962-02ea-431e-9313-4c30801d048a", "2023-05-12T04:17:48Z", destID, "2023-05-12T04:17:48Z", "2023-05-12T04:17:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 2", "2023-05-12T04:17:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "28c7a962-02ea-431e-9313-4c30801d048a", "2023-05-12T04:17:48Z", destID, "2023-05-12T04:17:48Z", "2023-05-12T04:17:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 4", "2023-05-12T04:29:48Z", userIDFormat, "non escaped column", "[::1]", "Main", "[::1]", uuidTS, sourceID, "7a94a3a8-e343-449d-9a57-3cce7b4a8386", "2023-05-12T04:29:48Z", destID, "2023-05-12T04:29:48Z", "2023-05-12T04:29:48Z", "non escaped column"},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 4", "2023-05-12T04:29:48Z", userIDFormat, "non escaped column", "[::1]", "Main", "[::1]", uuidTS, sourceID, "7a94a3a8-e343-449d-9a57-3cce7b4a8386", "2023-05-12T04:29:48Z", destID, "2023-05-12T04:29:48Z", "2023-05-12T04:29:48Z", "non escaped column"},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 1", "2023-05-12T04:11:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "98c2e2a8-1304-45b1-a1e1-07d9d3249b84", "2023-05-12T04:11:48Z", destID, "2023-05-12T04:11:48Z", "2023-05-12T04:11:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 1", "2023-05-12T04:11:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "98c2e2a8-1304-45b1-a1e1-07d9d3249b84", "2023-05-12T04:11:48Z", destID, "2023-05-12T04:11:48Z", "2023-05-12T04:11:48Z", ""},
	}
}

// UploadJobScreensMergeRecords returns a set of records for testing upload job screens scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobScreensMergeRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 3", "2023-05-12T04:23:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "088860cf-64d2-41c4-8347-19e6b216a91f", "2023-05-12T04:23:48Z", destID, "2023-05-12T04:23:48Z", "2023-05-12T04:23:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 2", "2023-05-12T04:17:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "28c7a962-02ea-431e-9313-4c30801d048a", "2023-05-12T04:17:48Z", destID, "2023-05-12T04:17:48Z", "2023-05-12T04:17:48Z", ""},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 4", "2023-05-12T04:29:48Z", userIDFormat, "non escaped column", "[::1]", "Main", "[::1]", uuidTS, sourceID, "7a94a3a8-e343-449d-9a57-3cce7b4a8386", "2023-05-12T04:29:48Z", destID, "2023-05-12T04:29:48Z", "2023-05-12T04:29:48Z", "non escaped column"},
		{destType, "http://www.rudderstack.com", "HTTP", "Home | RudderStack | 1", "2023-05-12T04:11:48Z", userIDFormat, "", "[::1]", "Main", "[::1]", uuidTS, sourceID, "98c2e2a8-1304-45b1-a1e1-07d9d3249b84", "2023-05-12T04:11:48Z", destID, "2023-05-12T04:11:48Z", "2023-05-12T04:11:48Z", ""},
	}
}

// UploadJobAliasesRecords returns a set of records for testing upload job aliases scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
func UploadJobAliasesRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{sourceID, destID, "[::1]", "2023-05-12T04:25:48Z", "190db5b9-b672-4384-b983-c83bc15a3c5d", userIDFormat, uuidTS, "gbelson+3@example.com", "2023-05-12T04:25:48Z", "HTTP", "2023-05-12T04:25:48Z", destType, "[::1]", "2023-05-12T04:25:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:31:48Z", "7106b916-c6aa-41e5-87b1-24bc984ecbc6", userIDFormat, uuidTS, "gbelson+4@example.com", "2023-05-12T04:31:48Z", "HTTP", "2023-05-12T04:31:48Z", destType, "[::1]", "2023-05-12T04:31:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:37:48Z", "85b39ba2-d840-4ecc-9519-d6bb534e5472", userIDFormat, uuidTS, "gbelson+5@example.com", "2023-05-12T04:37:48Z", "HTTP", "2023-05-12T04:37:48Z", destType, "[::1]", "2023-05-12T04:37:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:43:48Z", "a6eb36dc-b3a6-4c16-a311-fdd9354dfb5e", userIDFormat, uuidTS, "gbelson+6@example.com", "2023-05-12T04:43:48Z", "HTTP", "2023-05-12T04:43:48Z", destType, "[::1]", "2023-05-12T04:43:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:55:48Z", "b16ac193-6ee8-4258-8fef-57d674cb75cf", userIDFormat, uuidTS, "gbelson+8@example.com", "2023-05-12T04:55:48Z", "HTTP", "2023-05-12T04:55:48Z", destType, "[::1]", "2023-05-12T04:55:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:19:48Z", "e4642f0a-9c2e-4300-8da8-b53dc51df6f5", userIDFormat, uuidTS, "gbelson+2@example.com", "2023-05-12T04:19:48Z", "HTTP", "2023-05-12T04:19:48Z", destType, "[::1]", "2023-05-12T04:19:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:49:48Z", "efed87b9-dbf3-43bf-989b-e7ac71649743", userIDFormat, uuidTS, "gbelson+7@example.com", "2023-05-12T04:49:48Z", "HTTP", "2023-05-12T04:49:48Z", destType, "[::1]", "2023-05-12T04:49:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:13:48Z", "fe9dd44d-6b3a-4491-adf1-50d87af80000", userIDFormat, uuidTS, "gbelson+1@example.com", "2023-05-12T04:13:48Z", "HTTP", "2023-05-12T04:13:48Z", destType, "[::1]", "2023-05-12T04:13:48Z"},
	}
}

// UploadJobAliasesAppendRecords returns a set of records for testing upload job aliases scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobAliasesAppendRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{sourceID, destID, "[::1]", "2023-05-12T04:25:48Z", "190db5b9-b672-4384-b983-c83bc15a3c5d", userIDFormat, uuidTS, "gbelson+3@example.com", "2023-05-12T04:25:48Z", "HTTP", "2023-05-12T04:25:48Z", destType, "[::1]", "2023-05-12T04:25:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:25:48Z", "190db5b9-b672-4384-b983-c83bc15a3c5d", userIDFormat, uuidTS, "gbelson+3@example.com", "2023-05-12T04:25:48Z", "HTTP", "2023-05-12T04:25:48Z", destType, "[::1]", "2023-05-12T04:25:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:31:48Z", "7106b916-c6aa-41e5-87b1-24bc984ecbc6", userIDFormat, uuidTS, "gbelson+4@example.com", "2023-05-12T04:31:48Z", "HTTP", "2023-05-12T04:31:48Z", destType, "[::1]", "2023-05-12T04:31:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:31:48Z", "7106b916-c6aa-41e5-87b1-24bc984ecbc6", userIDFormat, uuidTS, "gbelson+4@example.com", "2023-05-12T04:31:48Z", "HTTP", "2023-05-12T04:31:48Z", destType, "[::1]", "2023-05-12T04:31:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:19:48Z", "e4642f0a-9c2e-4300-8da8-b53dc51df6f5", userIDFormat, uuidTS, "gbelson+2@example.com", "2023-05-12T04:19:48Z", "HTTP", "2023-05-12T04:19:48Z", destType, "[::1]", "2023-05-12T04:19:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:19:48Z", "e4642f0a-9c2e-4300-8da8-b53dc51df6f5", userIDFormat, uuidTS, "gbelson+2@example.com", "2023-05-12T04:19:48Z", "HTTP", "2023-05-12T04:19:48Z", destType, "[::1]", "2023-05-12T04:19:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:13:48Z", "fe9dd44d-6b3a-4491-adf1-50d87af80000", userIDFormat, uuidTS, "gbelson+1@example.com", "2023-05-12T04:13:48Z", "HTTP", "2023-05-12T04:13:48Z", destType, "[::1]", "2023-05-12T04:13:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:13:48Z", "fe9dd44d-6b3a-4491-adf1-50d87af80000", userIDFormat, uuidTS, "gbelson+1@example.com", "2023-05-12T04:13:48Z", "HTTP", "2023-05-12T04:13:48Z", destType, "[::1]", "2023-05-12T04:13:48Z"},
	}
}

// UploadJobAliasesMergeRecords returns a set of records for testing upload job aliases scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobAliasesMergeRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{sourceID, destID, "[::1]", "2023-05-12T04:25:48Z", "190db5b9-b672-4384-b983-c83bc15a3c5d", userIDFormat, uuidTS, "gbelson+3@example.com", "2023-05-12T04:25:48Z", "HTTP", "2023-05-12T04:25:48Z", destType, "[::1]", "2023-05-12T04:25:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:31:48Z", "7106b916-c6aa-41e5-87b1-24bc984ecbc6", userIDFormat, uuidTS, "gbelson+4@example.com", "2023-05-12T04:31:48Z", "HTTP", "2023-05-12T04:31:48Z", destType, "[::1]", "2023-05-12T04:31:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:19:48Z", "e4642f0a-9c2e-4300-8da8-b53dc51df6f5", userIDFormat, uuidTS, "gbelson+2@example.com", "2023-05-12T04:19:48Z", "HTTP", "2023-05-12T04:19:48Z", destType, "[::1]", "2023-05-12T04:19:48Z"},
		{sourceID, destID, "[::1]", "2023-05-12T04:13:48Z", "fe9dd44d-6b3a-4491-adf1-50d87af80000", userIDFormat, uuidTS, "gbelson+1@example.com", "2023-05-12T04:13:48Z", "HTTP", "2023-05-12T04:13:48Z", destType, "[::1]", "2023-05-12T04:13:48Z"},
	}
}

// UploadJobGroupsRecords returns a set of records for testing upload job groups scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
func UploadJobGroupsRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{destType, "0d089014-bc87-4543-9c69-c30e86ff6867", "", "basic", "2023-05-12T04:42:48Z", uuidTS, sourceID, "2023-05-12T04:42:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 6", "[::1]", "HTTP", "2023-05-12T04:42:48Z", "4500", "", destID, "2023-05-12T04:42:48Z", "Hooli", "[::1]"},
		{destType, "41d34264-afea-49b8-a466-f91ba655bc6e", "", "basic", "2023-05-12T04:12:48Z", uuidTS, sourceID, "2023-05-12T04:12:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 1", "[::1]", "HTTP", "2023-05-12T04:12:48Z", "4500", "", destID, "2023-05-12T04:12:48Z", "Hooli", "[::1]"},
		{destType, "56eb070b-b4ef-4ec0-86b4-390f531bad9c", "", "basic", "2023-05-12T04:24:48Z", uuidTS, sourceID, "2023-05-12T04:24:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 3", "[::1]", "HTTP", "2023-05-12T04:24:48Z", "4500", "", destID, "2023-05-12T04:24:48Z", "Hooli", "[::1]"},
		{destType, "6c062c97-eb80-427a-8877-2ace53782280", "non escaped column", "basic", "2023-05-12T04:54:48Z", uuidTS, sourceID, "2023-05-12T04:54:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 8", "[::1]", "HTTP", "2023-05-12T04:54:48Z", "4500", "non escaped column", destID, "2023-05-12T04:54:48Z", "Hooli", "[::1]"},
		{destType, "7dbbf719-320c-427a-bc13-590196b92484", "", "basic", "2023-05-12T04:36:48Z", uuidTS, sourceID, "2023-05-12T04:36:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 5", "[::1]", "HTTP", "2023-05-12T04:36:48Z", "4500", "", destID, "2023-05-12T04:36:48Z", "Hooli", "[::1]"},
		{destType, "a3788fd5-95ba-4b04-88af-cd59fbad7559", "non escaped column", "basic", "2023-05-12T04:30:48Z", uuidTS, sourceID, "2023-05-12T04:30:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 4", "[::1]", "HTTP", "2023-05-12T04:30:48Z", "4500", "non escaped column", destID, "2023-05-12T04:30:48Z", "Hooli", "[::1]"},
		{destType, "c11f1b76-2433-4b6c-857f-cb70bbdddfca", "", "basic", "2023-05-12T04:48:48Z", uuidTS, sourceID, "2023-05-12T04:48:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 7", "[::1]", "HTTP", "2023-05-12T04:48:48Z", "4500", "", destID, "2023-05-12T04:48:48Z", "Hooli", "[::1]"},
		{destType, "d4531c22-856e-43a3-9bb5-b532811a108f", "", "basic", "2023-05-12T04:18:48Z", uuidTS, sourceID, "2023-05-12T04:18:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 2", "[::1]", "HTTP", "2023-05-12T04:18:48Z", "4500", "", destID, "2023-05-12T04:18:48Z", "Hooli", "[::1]"},
	}
}

// UploadJobGroupsAppendRecords returns a set of records for testing upload job groups scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobGroupsAppendRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{destType, "41d34264-afea-49b8-a466-f91ba655bc6e", "", "basic", "2023-05-12T04:12:48Z", uuidTS, sourceID, "2023-05-12T04:12:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 1", "[::1]", "HTTP", "2023-05-12T04:12:48Z", "4500", "", destID, "2023-05-12T04:12:48Z", "Hooli", "[::1]"},
		{destType, "41d34264-afea-49b8-a466-f91ba655bc6e", "", "basic", "2023-05-12T04:12:48Z", uuidTS, sourceID, "2023-05-12T04:12:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 1", "[::1]", "HTTP", "2023-05-12T04:12:48Z", "4500", "", destID, "2023-05-12T04:12:48Z", "Hooli", "[::1]"},
		{destType, "56eb070b-b4ef-4ec0-86b4-390f531bad9c", "", "basic", "2023-05-12T04:24:48Z", uuidTS, sourceID, "2023-05-12T04:24:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 3", "[::1]", "HTTP", "2023-05-12T04:24:48Z", "4500", "", destID, "2023-05-12T04:24:48Z", "Hooli", "[::1]"},
		{destType, "56eb070b-b4ef-4ec0-86b4-390f531bad9c", "", "basic", "2023-05-12T04:24:48Z", uuidTS, sourceID, "2023-05-12T04:24:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 3", "[::1]", "HTTP", "2023-05-12T04:24:48Z", "4500", "", destID, "2023-05-12T04:24:48Z", "Hooli", "[::1]"},
		{destType, "a3788fd5-95ba-4b04-88af-cd59fbad7559", "non escaped column", "basic", "2023-05-12T04:30:48Z", uuidTS, sourceID, "2023-05-12T04:30:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 4", "[::1]", "HTTP", "2023-05-12T04:30:48Z", "4500", "non escaped column", destID, "2023-05-12T04:30:48Z", "Hooli", "[::1]"},
		{destType, "a3788fd5-95ba-4b04-88af-cd59fbad7559", "non escaped column", "basic", "2023-05-12T04:30:48Z", uuidTS, sourceID, "2023-05-12T04:30:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 4", "[::1]", "HTTP", "2023-05-12T04:30:48Z", "4500", "non escaped column", destID, "2023-05-12T04:30:48Z", "Hooli", "[::1]"},
		{destType, "d4531c22-856e-43a3-9bb5-b532811a108f", "", "basic", "2023-05-12T04:18:48Z", uuidTS, sourceID, "2023-05-12T04:18:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 2", "[::1]", "HTTP", "2023-05-12T04:18:48Z", "4500", "", destID, "2023-05-12T04:18:48Z", "Hooli", "[::1]"},
		{destType, "d4531c22-856e-43a3-9bb5-b532811a108f", "", "basic", "2023-05-12T04:18:48Z", uuidTS, sourceID, "2023-05-12T04:18:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 2", "[::1]", "HTTP", "2023-05-12T04:18:48Z", "4500", "", destID, "2023-05-12T04:18:48Z", "Hooli", "[::1]"},
	}
}

// UploadJobGroupsMergeRecords returns a set of records for testing upload job groups scenarios.
// It uses twice upload-job.events-1.json as the source of data.
func UploadJobGroupsMergeRecords(userIDFormat, sourceID, destID, destType string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{destType, "41d34264-afea-49b8-a466-f91ba655bc6e", "", "basic", "2023-05-12T04:12:48Z", uuidTS, sourceID, "2023-05-12T04:12:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 1", "[::1]", "HTTP", "2023-05-12T04:12:48Z", "4500", "", destID, "2023-05-12T04:12:48Z", "Hooli", "[::1]"},
		{destType, "56eb070b-b4ef-4ec0-86b4-390f531bad9c", "", "basic", "2023-05-12T04:24:48Z", uuidTS, sourceID, "2023-05-12T04:24:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 3", "[::1]", "HTTP", "2023-05-12T04:24:48Z", "4500", "", destID, "2023-05-12T04:24:48Z", "Hooli", "[::1]"},
		{destType, "a3788fd5-95ba-4b04-88af-cd59fbad7559", "non escaped column", "basic", "2023-05-12T04:30:48Z", uuidTS, sourceID, "2023-05-12T04:30:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 4", "[::1]", "HTTP", "2023-05-12T04:30:48Z", "4500", "non escaped column", destID, "2023-05-12T04:30:48Z", "Hooli", "[::1]"},
		{destType, "d4531c22-856e-43a3-9bb5-b532811a108f", "", "basic", "2023-05-12T04:18:48Z", uuidTS, sourceID, "2023-05-12T04:18:48Z", userIDFormat, "5e8a78ba9d32d3b1898a6247", "Technology 2", "[::1]", "HTTP", "2023-05-12T04:18:48Z", "4500", "", destID, "2023-05-12T04:18:48Z", "Hooli", "[::1]"},
	}
}

// SourceJobTracksRecords returns a set of records for testing source job tracks scenarios.
// It uses source-job.events-1.json, source-job.events-2.json as the source of data.
func SourceJobTracksRecords(userIDFormat, sourceID, destID, destType, jobRunID, taskRunID string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{"sources", "2DkCpUr0xfiGBPJxIwqyqfyHdq4", "2023-05-12T04:15:48Z", "v2.1.1", uuidTS, "2023-05-12T04:15:48Z", "[::1]", "google_sheet", "google_sheet", userIDFormat, destID, "664cac3b-ff37-4a4d-9992-ad25da52f5c2", "[::1]", "singer-google-sheets", "2023-05-12T04:15:48Z", jobRunID, taskRunID, sourceID, destType, "2023-05-12T04:15:48Z"},
		{"sources", "2DkCpUr0xfiGBPJxIwqyqfyHdq4", "2023-05-12T04:12:48Z", "v2.1.1", uuidTS, "2023-05-12T04:12:48Z", "[::1]", "google_sheet", "google_sheet", userIDFormat, destID, "9b9b5940-ab21-4f07-8de6-cf8e70cb9c06", "[::1]", "singer-google-sheets", "2023-05-12T04:12:48Z", jobRunID, taskRunID, sourceID, destType, "2023-05-12T04:12:48Z"},
		{"sources", "2DkCpUr0xfiGBPJxIwqyqfyHdq4", "2023-05-12T04:14:48Z", "v2.1.1", uuidTS, "2023-05-12T04:14:48Z", "[::1]", "google_sheet", "google_sheet", userIDFormat, destID, "c50d846f-4add-40bd-8697-f94d63b1aa64", "[::1]", "singer-google-sheets", "2023-05-12T04:14:48Z", jobRunID, taskRunID, sourceID, destType, "2023-05-12T04:14:48Z"},
		{"sources", "2DkCpUr0xfiGBPJxIwqyqfyHdq4", "2023-05-12T04:13:48Z", "v2.1.1", uuidTS, "2023-05-12T04:13:48Z", "[::1]", "google_sheet", "google_sheet", userIDFormat, destID, "f623fe31-6ef3-4835-ad2c-c252be290f3b", "[::1]", "singer-google-sheets", "2023-05-12T04:13:48Z", jobRunID, taskRunID, sourceID, destType, "2023-05-12T04:13:48Z"},
	}
}

// SourceJobGoogleSheetRecords returns a set of records for testing source job google sheet scenarios.
// It uses upload-job.events-1.json, upload-job.events-2.json as the source of data.
func SourceJobGoogleSheetRecords(userIDFormat, sourceID, destID, destType, jobRunID, taskRunID string) [][]string {
	uuidTS := timeutil.Now().Format("2006-01-02")
	return [][]string{
		{"8", "2023-05-12T04:15:48Z", "non escaped column", "[::1]", jobRunID, "sources", "OK for the price. It works but the material feels flimsy.", sourceID, "2023-05-12T04:15:48Z", destID, "2DkCpUr0xfiGBPJxIwqyqfyHdq4", "google_sheet", taskRunID, "singer-google-sheets", userIDFormat, "[::1]", "2023-05-12T04:15:48Z", "664cac3b-ff37-4a4d-9992-ad25da52f5c2", "2023-05-12T04:15:48Z", "86ac1cd43", uuidTS, "v2.1.1", destType, "google_sheet", "non escaped column", "3"},
		{"5", "2023-05-12T04:12:48Z", "", "[::1]", jobRunID, "sources", "OK for the price. It works but the material feels flimsy.", sourceID, "2023-05-12T04:12:48Z", destID, "2DkCpUr0xfiGBPJxIwqyqfyHdq4", "google_sheet", taskRunID, "singer-google-sheets", userIDFormat, "[::1]", "2023-05-12T04:12:48Z", "9b9b5940-ab21-4f07-8de6-cf8e70cb9c06", "2023-05-12T04:12:48Z", "86ac1cd43", uuidTS, "v2.1.1", destType, "google_sheet", "", "3"},
		{"7", "2023-05-12T04:14:48Z", "non escaped column", "[::1]", jobRunID, "sources", "OK for the price. It works but the material feels flimsy.", sourceID, "2023-05-12T04:14:48Z", destID, "2DkCpUr0xfiGBPJxIwqyqfyHdq4", "google_sheet", taskRunID, "singer-google-sheets", userIDFormat, "[::1]", "2023-05-12T04:14:48Z", "c50d846f-4add-40bd-8697-f94d63b1aa64", "2023-05-12T04:14:48Z", "86ac1cd43", uuidTS, "v2.1.1", destType, "google_sheet", "non escaped column", "3"},
		{"6", "2023-05-12T04:13:48Z", "", "[::1]", jobRunID, "sources", "OK for the price. It works but the material feels flimsy.", sourceID, "2023-05-12T04:13:48Z", destID, "2DkCpUr0xfiGBPJxIwqyqfyHdq4", "google_sheet", taskRunID, "singer-google-sheets", userIDFormat, "[::1]", "2023-05-12T04:13:48Z", "f623fe31-6ef3-4835-ad2c-c252be290f3b", "2023-05-12T04:13:48Z", "86ac1cd43", uuidTS, "v2.1.1", destType, "google_sheet", "", "3"},
	}
}
