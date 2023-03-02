package queue

import (
	"container/heap"
	"time"
)

// Item stores the attributes which will be pushed to the priority queue..
type Item[T any] struct {
	Value     T
	Priority  int
	timeStamp int64
	index     int
}

// PriorityQueue provides a heap.Interface compatible priority queue for the Item type.
// The actual Item.index in the queue is controlled by the Item.Priority and Item.timeStamp.
type PriorityQueue[T any] []*Item[T]

// Len: Size of the priority queue . Used to satisfy the heap interface...
func (pq PriorityQueue[T]) Len() int { return len(pq) }

// Less is used to compare elements and store them in the proper order in
// priority queue.
func (pq PriorityQueue[T]) Less(i, j int) bool {
	if pq[i].Priority == pq[j].Priority {
		return pq[i].timeStamp <= pq[j].timeStamp
	}
	return pq[i].Priority > pq[j].Priority
}

// Swap is used to swap the values in the priority queue.
func (pq PriorityQueue[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push adds elements to the priority queue
func (pq *PriorityQueue[T]) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item[T])
	item.index = n
	item.timeStamp = makeTimestamp()
	*pq = append(*pq, item)
}

// Pop removes elements from the priority queue
func (pq *PriorityQueue[T]) Pop() interface{} {
	old := *pq
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// Top returns the topmost element in the priority queue
func (pq *PriorityQueue[T]) Top() interface{} {
	if len(*pq) == 0 {
		return nil
	}
	ol := *pq
	return *ol[0]
}

// GetIndex returns the index of the corresponding element.
func (pq *PriorityQueue[T]) GetIndex(x interface{}) int {
	item := x.(*Item[T])
	return item.index
}

// Update updates the attributes of an element in the priority queue.
func (pq *PriorityQueue[T]) Update(item *Item[T], priority int) {
	if item.index == -1 {
		return
	}
	item.Priority = priority
	heap.Fix(pq, item.index)
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
