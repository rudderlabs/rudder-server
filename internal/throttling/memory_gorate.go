package throttling

import (
	"sync"
	"time"

	gorate "golang.org/x/time/rate"
)

// TODO add expiration mechanism? if we don't touch a key anymore it will stay in memory forever
type goRate struct {
	mu sync.Mutex
	m  map[string]*gorate.Limiter
}

func (r *goRate) limit(key string, cost, rate, periodInSecs int64) *goRateReservation {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.m == nil {
		r.m = make(map[string]*gorate.Limiter)
	}

	window := time.Duration(periodInSecs) * time.Second
	l, ok := r.m[key]
	if !ok {
		l = gorate.NewLimiter(gorate.Every(window), int(rate))
		r.m[key] = l
	}

	resWindow := time.Now().Add(window)
	res := l.ReserveN(resWindow, int(cost))

	return &goRateReservation{
		reservation: res,
		window:      resWindow,
	}
}

type goRateReservation struct {
	reservation *gorate.Reservation
	window      time.Time
}

func (r *goRateReservation) Cancel()       { r.reservation.Cancel() }
func (r *goRateReservation) CancelFuture() { r.reservation.CancelAt(r.window) }
func (r *goRateReservation) Allowed() bool {
	return r.reservation.OK() && r.reservation.DelayFrom(r.window) == 0
}
