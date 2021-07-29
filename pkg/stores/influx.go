package stores

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxapi "github.com/influxdata/influxdb-client-go/v2/api"
	log "github.com/sirupsen/logrus"
)

type influxStore struct {
	mu             *sync.Mutex
	localStore     Store
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
	localStore := NewMemoryStore()
	client := influxdb2.NewClientWithOptions(serverURL, token, influxdb2.DefaultOptions().SetPrecision(time.Second))
	writeAPI := client.WriteAPI(organization, bucket)

	store := influxStore{
		mu:             &sync.Mutex{},
		localStore:     localStore,
		influxClient:   client,
		influxWriteAPI: writeAPI,
		ctx:            context.TODO(),
	}
	return &store
}

func (s *influxStore) Set(key string, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localStore.Set(key, value)
}

func (s *influxStore) Get(key string) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.localStore.Get(key)
}

func (s *influxStore) Increment(key string, timestamp time.Time) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	incr, err := s.localStore.Increment(key, timestamp)
	line := fmt.Sprintf("%s count=%du %d", key, incr, timestamp.Unix())
	log.Debug(line)
	s.influxWriteAPI.WriteRecord(line)
	return incr, err
}

func (s *influxStore) Contains(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.localStore.Contains(key)
}

func (s *influxStore) EncodeDirectlyFollowsRelation(from string, to string) string {
	if from == "" {
		return fmt.Sprintf("dfrelation,to=%s", escape(to))
	}
	return fmt.Sprintf("dfrelation,from=%s,to=%s", escape(from), escape(to))
}

func (s *influxStore) EncodeActivity(activity string) string {
	return fmt.Sprintf("activity,name=%s", escape(activity))
}

func (s *influxStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.localStore.Close()
	s.influxWriteAPI.Flush()
	s.influxClient.Close()
}

func escape(str string) string {
	str = strings.Replace(str, " ", "\\ ", -1)
	str = strings.Replace(str, ",", "\\,", -1)
	return str
}
