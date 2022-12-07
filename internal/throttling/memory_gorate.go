package throttling

import (
	"sync"
	"time"

	gorate "golang.org/x/time/rate"

	"github.com/rudderlabs/rudder-server/internal/throttling/cachettl"
)

type goRate struct {
	mu    sync.Mutex
	store *cachettl.Cache[string, *gorate.Limiter]
}

func (r *goRate) limit(key string, cost, rate, periodInSecs int64) *goRateReservation {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.store == nil {
		r.store = cachettl.New[string, *gorate.Limiter]()
	}

	window := time.Duration(periodInSecs) * time.Second
	l := r.store.Get(key)
	if l == nil {
		l = gorate.NewLimiter(gorate.Every(window), int(rate))
		r.store.Put(key, l, window)
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
