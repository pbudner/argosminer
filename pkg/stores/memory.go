package stores

import (
	"fmt"
	"sync"
	"time"
)

type memoryStore struct {
	mu    *sync.Mutex
	store map[string]interface{}
}

func NewMemoryStore() *memoryStore {
	store := memoryStore{
		mu:    &sync.Mutex{},
		store: make(map[string]interface{}),
	}
	return &store
}

func (s *memoryStore) Set(key string, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[key] = value
	return nil
}

func (s *memoryStore) Get(key string) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.store[key]
	if !ok {
		return nil, fmt.Errorf("key %s does not exist in memory store", key)
	}

	return value, nil
}

func (s *memoryStore) Increment(key string, timestamp time.Time) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rawValue, ok := s.store[key]
	if !ok {
		s.store[key] = uint64(1)
		return 1, nil
	}

	value, ok := rawValue.(uint64)
	if !ok {
		return 0, fmt.Errorf("value is not a uint64")
	}

	value++
	s.store[key] = value
	return value, nil
}

func (s *memoryStore) Contains(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.store[key]
	return ok
}

func (s *memoryStore) EncodeDirectlyFollowsRelation(from string, to string) string {
	if from == "" {
		return to
	}
	return fmt.Sprintf("%s -> %s", from, to)
}

func (s *memoryStore) EncodeActivity(activity string) string {
	return activity
}

func (s *memoryStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// do nothing
}
