package stores

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxapi "github.com/influxdata/influxdb-client-go/v2/api"
)

type influxStore struct {
	mu             *sync.Mutex
	redisStore     redisStore
	influxClient   influxdb2.Client
	influxWriteAPI influxapi.WriteAPI
	ctx            context.Context
}

func NewInfluxStoreGenerator(serverURL string, token string, bucket string, organization string, redisOptions redis.Options) StoreGenerator {
	return func(_ interface{}) Store {
		return NewInfluxStore(serverURL, token, bucket, organization, redisOptions)
	}
}

func NewInfluxStore(serverURL string, token string, bucket string, organization string, redisOptions redis.Options) *influxStore {
	redisStore := NewRedisStore(redisOptions)
	client := influxdb2.NewClientWithOptions(serverURL, token, influxdb2.DefaultOptions())
	writeAPI := client.WriteAPI(organization, bucket)

	store := influxStore{
		mu:             &sync.Mutex{},
		redisStore:     *redisStore,
		influxClient:   client,
		influxWriteAPI: writeAPI,
		ctx:            context.TODO(),
	}
	return &store
}

func (s *influxStore) Set(key string, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.redisStore.Set(key, value)
}

func (s *influxStore) Get(key string) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.redisStore.Get(key)
}

func (s *influxStore) Increment(key string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	incr, err := s.redisStore.Increment(key)
	// line := fmt.Sprintf("my_counter,app=go counter=%d", counter)
	// writeAPI.WriteRecord(line)
	return incr, err
}

func (s *influxStore) Contains(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.redisStore.Contains(key)
}

func (s *influxStore) EncodeDirectlyFollowsRelation(from string, to string) string {
	if from == "" {
		return fmt.Sprintf("to=%s", to)
	}
	return fmt.Sprintf("from=%s,to=%s", from, to)
}

func (s *influxStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.redisStore.Close()
	s.influxWriteAPI.Flush()
	s.influxClient.Close()
}
