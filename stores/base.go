package stores

import "time"

type Store interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Increment(key []byte, timestamp time.Time) (uint64, error)
	Contains(key []byte) bool
	EncodeDirectlyFollowsRelation(from []byte, to []byte) []byte
	EncodeActivity(activity []byte) []byte
	Close()
}

type StoreGenerator func(string) Store
