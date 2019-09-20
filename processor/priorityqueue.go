package processor

import (
	"container/heap"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

//We keep a priority queue of user_id to last event
//timestamp from that user
type pqItemT struct {
	userID string    //userID
	lastTS time.Time //last timestamp
	index  int       //index in priority queue
}

type pqT []*pqItemT

func (pq pqT) Len() int { return len(pq) }

func (pq pqT) Less(i, j int) bool {
	return pq[i].lastTS.Before(pq[j].lastTS)
}

func (pq pqT) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *pqT) Push(x interface{}) {
	n := len(*pq)
	item := x.(*pqItemT)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *pqT) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *pqT) Top() *pqItemT {
	item := (*pq)[len(*pq)-1]
	return item
}

func (pq *pqT) Remove(item *pqItemT) {
	heap.Remove(pq, item.index)
}

func (pq *pqT) Update(item *pqItemT, newTS time.Time) {
	item.lastTS = newTS
	heap.Fix(pq, item.index)
}

func (pq *pqT) Add(item *pqItemT) {
	heap.Push(pq, item)
}

func (pq *pqT) Print() {
	for _, v := range *pq {
		logger.Debug(*v)
	}
}
