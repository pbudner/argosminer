package transforms

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	utcLocation, _ := time.LoadLocation("UTC")
	pq := make(TimeOrderedQueue, 0)
	heap.Init(&pq)
	heap.Push(&pq, &BufferItem{
		value: []byte("Apple"),
		time:  time.Date(2022, 02, 23, 12, 00, 00, 00, utcLocation).UnixNano(),
	})
	heap.Push(&pq, &BufferItem{
		value: []byte("Banana"),
		time:  time.Date(2022, 02, 23, 11, 00, 00, 00, utcLocation).UnixNano(),
	})
	heap.Push(&pq, &BufferItem{
		value: []byte("Avocado"),
		time:  time.Date(2022, 02, 23, 13, 00, 00, 00, utcLocation).UnixNano(),
	})
	require.Equal(t, []byte("Banana"), heap.Pop(&pq).(*BufferItem).value)
	heap.Push(&pq, &BufferItem{
		value: []byte("Pineapple"),
		time:  time.Date(2022, 02, 22, 13, 00, 00, 00, utcLocation).UnixNano(),
	})
	heap.Push(&pq, &BufferItem{
		value: []byte("Orange"),
		time:  time.Date(2022, 02, 24, 13, 00, 00, 00, utcLocation).UnixNano(),
	})
	require.Equal(t, []byte("Pineapple"), pq[0].value)
	require.Equal(t, []byte("Pineapple"), heap.Pop(&pq).(*BufferItem).value)
	require.Equal(t, []byte("Apple"), pq[0].value)
	require.Equal(t, []byte("Apple"), heap.Pop(&pq).(*BufferItem).value)
	require.Equal(t, []byte("Avocado"), heap.Pop(&pq).(*BufferItem).value)
	require.Equal(t, []byte("Orange"), heap.Pop(&pq).(*BufferItem).value)

}
