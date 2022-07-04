package suppression

type NOOP struct{}

func (*NOOP) IsSuppressedUser(userID, sourceID, writeKey string) bool {
	return false
}
