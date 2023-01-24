package processor

import (
	"container/heap"

	"github.com/rudderlabs/rudder-server/processor/transformer"
)

type TransformRequestT struct {
	Event          []transformer.TransformerEventT
	Stage          string
	ProcessingTime float64
	Index          int
}

type transformRequestPQ []*TransformRequestT

func (pq transformRequestPQ) Len() int { // skipcq: GO-W1029
	return len(pq)
}

func (pq transformRequestPQ) Less(i, j int) bool { // skipcq: GO-W1029
	return pq[i].ProcessingTime < pq[j].ProcessingTime
}

func (pq transformRequestPQ) Swap(i, j int) { // skipcq: GO-W1029
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *transformRequestPQ) Push(x interface{}) { // skipcq: GO-W1029
	n := len(*pq)
	item := x.(*TransformRequestT)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *transformRequestPQ) Pop() interface{} { // skipcq: GO-W1029
	old := *pq
	n := len(old)
	item := old[n-1]
	item.Index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *transformRequestPQ) Top() *TransformRequestT { // skipcq: GO-W1029
	item := (*pq)[0]
	return item
}

func (pq *transformRequestPQ) Remove(item *TransformRequestT) { // skipcq: GO-W1029
	heap.Remove(pq, item.Index)
}

func (pq *transformRequestPQ) Update(item, nextItem *TransformRequestT) { // skipcq: GO-W1029
	index := item.Index
	item = nextItem
	item.Index = index
	heap.Fix(pq, item.Index)
}

func (pq *transformRequestPQ) Add(item *TransformRequestT) { // skipcq: GO-W1029
	heap.Push(pq, item)
}

func (pq *transformRequestPQ) RemoveTop() { // skipcq: GO-W1029
	heap.Pop(pq)
}
