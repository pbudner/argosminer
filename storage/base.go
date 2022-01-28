package storage

import "errors"

var (

	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("key not found")

	// ErrEmptyKey is returned when an empty is provided.
	ErrEmptyKey = errors.New("empty key")
)

type Storage interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Increment(key []byte) (uint64, error)
	SetBatch(batch []KeyValue) error
	Contains(key []byte) bool
	GetFirst(count int) ([][]byte, error)
	GetRange(from []byte, to []byte) ([][]byte, error)
	TotalCount() (uint64, error)
	CountRange(from []byte, to []byte) (uint64, error)
	Iterate(prefix []byte, f func([]byte, func() ([]byte, error)) (bool, error)) error
	IterateReverse(prefix []byte, f func([]byte, func() ([]byte, error)) (bool, error)) error
	Seek(key []byte) (KeyValue, error)
	CountPrefix(prefix []byte) (uint64, error)
	Close()
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

type StorageGenerator func(string) Storage
