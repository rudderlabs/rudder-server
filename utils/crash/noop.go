package crash

type NOOP struct{}

func (n *NOOP) Notify(team string) func() {
	return func() {}
}
