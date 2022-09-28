package suppression

type NOOP struct{}

func (*NOOP) IsSuppressedUser(_, _ string) bool {
	return false
}
