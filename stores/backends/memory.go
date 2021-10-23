package backends

/*
type memoryStore struct {
	mu    *sync.Mutex
	store map[string][]byte
}

func NewMemoryStoreGenerator() StoreBackendGenerator {
	return func(_ string) StoreBackend {
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

func (s *memoryStore) Increment(key []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	serializedKey := string(key)
	rawValue, ok := s.store[serializedKey]
	if !ok {
		s.store[serializedKey] = utils.Uint64ToBytes(1)
		return 1, nil
	}

	value := 1 + utils.BytesToUint64(rawValue) // increment
	s.store[serializedKey] = utils.Uint64ToBytes(value)
	return value, nil
}

func (s *memoryStore) Contains(key []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.store[string(key)]
	return ok
}

func (s *memoryStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// do nothing
}
*/
