package servermode

type Mode string

const (
	NormalMode   Mode = "NORMAL"
	DegradedMode Mode = "DEGRADED"
)

type ChangeEvent struct {
	err  error
	ack  func() error
	mode Mode
}

func NewChangeEvent(mode Mode, ack func() error) ChangeEvent {
	return ChangeEvent{
		mode: mode,
		ack:  ack,
	}
}

func ChangeEventError(err error) ChangeEvent {
	return ChangeEvent{
		err: err,
	}
}

func (m ChangeEvent) Ack() error {
	return m.ack()
}

func (m ChangeEvent) Mode() Mode {
	return m.mode
}

func (m ChangeEvent) Err() error {
	return m.err
}

func (mode Mode) Valid() bool {
	if mode == NormalMode || mode == DegradedMode {
		return true
	}
	return false
}
