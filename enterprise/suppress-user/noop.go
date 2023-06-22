package suppression

import "time"

type NOOP struct{}

func (*NOOP) IsSuppressedUser(_, _, _ string) bool {
	return false
}

func (*NOOP) GetCreatedAt(_, _, _ string) time.Time {
	return time.Time{}
}
