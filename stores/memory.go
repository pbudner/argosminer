package stores

import (
	"fmt"
	"sync"
	"time"
)

type memoryStore struct {
	mu    *sync.Mutex
	store map[string][]byte
}

func NewMemoryStoreGenerator() StoreGenerator {
	return func(_ string) Store {
		return NewMemoryStore()
	}
}

func NewMemoryStore() *memoryStore {
	store := memoryStore{
		mu:    &sync.Mutex{},
		store: make(map[string][]byte),
	}
	return &store
}

func (s *memoryStore) Set(key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[string(key)] = value
	return nil
}

func (s *memoryStore) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.store[string(key)]
	if !ok {
		return nil, fmt.Errorf("key %s does not exist in memory store", key)
	}

	return value, nil
}

func (s *memoryStore) Increment(key []byte, timestamp time.Time) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	serializedKey := string(key)
	rawValue, ok := s.store[serializedKey]
	if !ok {
		s.store[serializedKey] = uint64ToBytes(1)
		return 1, nil
	}

	value := bytesToUint64(rawValue)
	value++
	s.store[serializedKey] = uint64ToBytes(value)
	return value, nil
}

func (s *memoryStore) Contains(key []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.store[string(key)]
	return ok
}

func (s *memoryStore) EncodeDirectlyFollowsRelation(from []byte, to []byte) []byte {
	if len(from) == 0 {
		return to
	}
	return append(append(from, byte(0)), to[:]...)
}

func (s *memoryStore) EncodeActivity(activity []byte) []byte {
	return activity
}

func (s *memoryStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// do nothing
}
