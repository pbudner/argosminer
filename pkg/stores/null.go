package stores

import (
	"sync"
	"time"
)

type nullStore struct {
	mu    *sync.Mutex
	store map[string]interface{}
}

func NewNullStoreGenerator() StoreGenerator {
	return func(_ interface{}) Store {
		return NewNullStore()
	}
}

func NewNullStore() *nullStore {
	store := nullStore{
		mu:    &sync.Mutex{},
		store: make(map[string]interface{}),
	}
	return &store
}

func (s *nullStore) Set(key string, value interface{}) error {
	return nil
}

func (s *nullStore) Get(key string) (interface{}, error) {
	return 0, nil
}

func (s *nullStore) Increment(key string, timestamp time.Time) (uint64, error) {
	return 0, nil
}

func (s *nullStore) Contains(key string) bool {
	return true
}

func (s *nullStore) EncodeDirectlyFollowsRelation(from string, to string) string {
	return ""
}

func (s *nullStore) EncodeActivity(activity string) string {
	return activity
}

func (s *nullStore) Close() {
	// do nothing
}
