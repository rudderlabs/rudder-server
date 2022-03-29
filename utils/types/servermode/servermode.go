package servermode

type Mode string

const (
	NormalMode   Mode = "NORMAL"
	DegradedMode Mode = "DEGRADED"
)

type Ack struct {
	ack        func()
	mode       Mode
	workspaces string
}

func (m Ack) Ack() {
	m.ack()
}

func (m Ack) Mode() Mode {
	return m.mode
}

func (m Ack) Workspaces() string {
	return m.workspaces
}

func WithACK(mode Mode, workspaces string, ack func()) Ack {
	return Ack{
		mode:       mode,
		ack:        ack,
		workspaces: workspaces,
	}
}
