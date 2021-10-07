package stores

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nakabonne/tstorage"
	log "github.com/sirupsen/logrus"
)

type tstorageStore struct {
	mu         *sync.Mutex
	localStore Store
	store      tstorage.Storage
	ctx        context.Context
}

func NewTstorageStoreGenerator() StoreGenerator {
	return func(_ interface{}) Store {
		return NewTstorageStore()
	}
}

func NewTstorageStore() *tstorageStore {
	localStore := NewMemoryStore()
	storage, _ := tstorage.NewStorage(
		tstorage.WithTimestampPrecision(tstorage.Seconds),
		tstorage.WithDataPath("./data"),
	)

	store := tstorageStore{
		mu:         &sync.Mutex{},
		store:      storage,
		localStore: localStore,
		ctx:        context.TODO(),
	}
	return &store
}

func (s *tstorageStore) Set(key string, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localStore.Set(key, value)
}

func (s *tstorageStore) Get(key string) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localStore.Get(key)
}

func (s *tstorageStore) Increment(key string, timestamp time.Time) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	incr, err := s.localStore.Increment(key, timestamp)
	line := fmt.Sprintf("%s count=%du %d", key, incr, timestamp.Unix())
	log.Debug(line)
	_ = s.store.InsertRows([]tstorage.Row{
		{
			Metric:    key,
			DataPoint: tstorage.DataPoint{Timestamp: timestamp.Unix(), Value: float64(incr)},
		},
	})
	return incr, err
}

func (s *tstorageStore) Contains(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localStore.Contains(key)
}

func (s *tstorageStore) EncodeDirectlyFollowsRelation(from string, to string) string {
	if from == "" {
		return fmt.Sprintf("dfrelation,to=%s", escape(to))
	}
	return fmt.Sprintf("dfrelation,from=%s,to=%s", escape(from), escape(to))
}

func (s *tstorageStore) EncodeActivity(activity string) string {
	return fmt.Sprintf("activity,name=%s", escape(activity))
}

func (s *tstorageStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store.Close()
}
