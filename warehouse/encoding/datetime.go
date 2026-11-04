package encoding

import "time"

// NormalizeMaxComputeDateTime converts source timestamps and already normalized
// load-file values to UTC. Retrying a load does not alter a valid datetime.
func NormalizeMaxComputeDateTime(value any) (string, bool) {
	text, ok := value.(string)
	if !ok {
		return "", false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700",
		MaxComputeTimeFormat,
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed.UTC().Format(MaxComputeTimeFormat), true
		}
	}
	return "", false
}
