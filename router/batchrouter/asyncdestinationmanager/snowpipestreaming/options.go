package snowpipestreaming

type Opt func(*Manager)

func WithRequestDoer(requestDoer requestDoer) Opt {
	return func(s *Manager) {
		s.requestDoer = requestDoer
	}
}
