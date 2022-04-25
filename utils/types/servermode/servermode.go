package servermode

import "context"

type Mode string

const (
	NormalMode   Mode = "NORMAL"
	DegradedMode Mode = "DEGRADED"

	ETCDClusterManager = "ETCD"
	StaticClusterManager = "Static"
)

type ChangeEvent struct {
	err  error
	ack  func(context.Context) error
	mode Mode
}

func NewChangeEvent(mode Mode, ack func(context.Context) error) ChangeEvent {
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

func (m ChangeEvent) Ack(ctx context.Context) error {
	return m.ack(ctx)
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
