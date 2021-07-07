package stores

import "time"

type Store interface {
	Set(key string, value interface{}) error
	Get(key string) (interface{}, error)
	Increment(key string, timestamp time.Time) (uint64, error)
	Contains(key string) bool
	EncodeDirectlyFollowsRelation(from string, to string) string
	EncodeActivity(activity string) string
	Close()
}

type StoreGenerator func(interface{}) Store
