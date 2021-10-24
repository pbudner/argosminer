package backends

type StoreBackend interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Increment(key []byte) (uint64, error)
	Contains(key []byte) bool
	GetLast(count int) ([][]byte, error)
	GetFirst(count int) ([][]byte, error)
	GetRange(from []byte, to []byte) ([][]byte, error)
	TotalCount() (uint64, error)
	CountRange(from []byte, to []byte) (uint64, error)
	Find(prefix []byte) ([]KeyValue, error)
	Close()
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

type StoreBackendGenerator func(string) StoreBackend
