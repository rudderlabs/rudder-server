package testutils

func GetClosedEmptyChannel() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
