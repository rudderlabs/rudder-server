package servermode

type Mode string

const (
	NormalMode    Mode = "NORMAL"
	DegradedMode  Mode = "DEGRADED"
)

type Ack struct {
	ack  func()
	mode Mode
}

func (m Ack) Ack() {
	m.ack()
}

func (m Ack) Mode() Mode {
	return m.mode
}

func WithACK(mode Mode, ack func()) Ack {
	return Ack{
		mode: mode,
		ack:  ack,
	}
}

func (mode Mode) Valid() bool {
	if mode == NormalMode || mode == DegradedMode {
		return true
	}
	return false
}
