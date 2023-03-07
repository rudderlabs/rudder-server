package queue

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPriorityQueue(t *testing.T) {
	t.Run("different priorities", func(t *testing.T) {
		pq := make(PriorityQueue[any], 0)
		heap.Init(&pq)
		for i := 0; i < 3; i++ {
			item := &Item[any]{
				Priority: i * 2,
			}
			heap.Push(&pq, item)
		}
		expectedVals := []int{4, 2, 0}
		actualVals := make([]int, 0)
		for len(pq) > 0 {
			topEle := pq.Top().(Item[any])
			_ = pq.GetIndex(&topEle)
			_ = heap.Pop(&pq).(*Item[any])
			actualVals = append(actualVals, topEle.Priority)
		}
		require.Equal(t, expectedVals, actualVals)
	})

	t.Run("same priorities", func(t *testing.T) {
		pq := make(PriorityQueue[any], 3)

		for i := 0; i < 3; i++ {
			pq[i] = &Item[any]{
				Priority:  1,
				timeStamp: int64(i),
			}
		}

		pq.Update(pq[2], 3)
		expectedVals := []int64{2, 0, 1}
		actualVals := make([]int64, 0)
		for pq.Len() > 0 {
			item := heap.Pop(&pq).(*Item[any])
			actualVals = append(actualVals, item.timeStamp)
		}
		require.Equal(t, expectedVals, actualVals)
	})

	t.Run("nil operations", func(t *testing.T) {
		var pq PriorityQueue[any]
		require.Nil(t, pq.Top())
		require.Nil(t, pq.Pop())
		require.Equal(t, 0, pq.Len())
	})

	t.Run("pop then try to update", func(t *testing.T) {
		pq := make(PriorityQueue[any], 3)

		for i := 0; i < 3; i++ {
			pq[i] = &Item[any]{
				Priority:  1,
				timeStamp: int64(i),
			}
		}
		i1 := pq.Pop().((*Item[any])) // remove the item
		require.Len(t, pq, 2, "pq should have 2 elements after pop")
		pq.Update(i1, i1.Priority+1) // try to update the removed item
		require.Len(t, pq, 2, "pq should still have 2 elements after updating the popped item")
	})
}
