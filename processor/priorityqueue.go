package processor

import (
	"container/heap"
	"time"

	"github.com/rudderlabs/rudder-server/processor/transformer"
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
	item := (*pq)[0]
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
		pkgLogger.Debug(*v)
	}
}

type TransformRequestT struct {
	Event          []transformer.TransformerEventT
	Stage          string
	ProcessingTime float64
	Index          int
}

type transformRequestPQ []*TransformRequestT

func (pq transformRequestPQ) Len() int {
	return len(pq)
}

func (pq transformRequestPQ) Less(i, j int) bool {
	return pq[i].ProcessingTime < pq[j].ProcessingTime
}

func (pq transformRequestPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *transformRequestPQ) Push(x interface{}) {
	n := len(*pq)
	item := x.(*TransformRequestT)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *transformRequestPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.Index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *transformRequestPQ) Top() *TransformRequestT {
	item := (*pq)[0]
	return item
}

func (pq *transformRequestPQ) Remove(item *TransformRequestT) {
	heap.Remove(pq, item.Index)
}

func (pq *transformRequestPQ) Update(item *TransformRequestT, nextItem *TransformRequestT) {
	index := item.Index
	item = nextItem
	item.Index = index
	heap.Fix(pq, item.Index)
}

func (pq *transformRequestPQ) Add(item *TransformRequestT) {
	heap.Push(pq, item)
}

func (pq *transformRequestPQ) RemoveTop() {
	heap.Pop(pq)
}

func (pq *transformRequestPQ) Print() {
	for _, v := range *pq {
		pkgLogger.Debug(*v)
	}
}
