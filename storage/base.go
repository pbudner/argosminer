package storage

type Storage interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Increment(key []byte) (uint64, error)
	SetBatch(batch []KeyValue) error
	Contains(key []byte) bool
	IterateReverse(prefix []byte, f func(KeyValue) (bool, error)) error
	GetFirst(count int) ([][]byte, error)
	GetRange(from []byte, to []byte) ([][]byte, error)
	TotalCount() (uint64, error)
	CountRange(from []byte, to []byte) (uint64, error)
	Find(prefix []byte, f func(KeyValue) (bool, error)) error
	CountPrefix(prefix []byte) (uint64, error)
	Close()
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

type StorageGenerator func(string) Storage
