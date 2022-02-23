package transforms

type BufferItem struct {
	value []byte
	time  int64
	index int
}

type TimeOrderedQueue []*BufferItem

func (pq TimeOrderedQueue) Len() int { return len(pq) }

func (pq TimeOrderedQueue) Less(i, j int) bool {
	return pq[i].time < pq[j].time
}

func (pq TimeOrderedQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *TimeOrderedQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*BufferItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *TimeOrderedQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
