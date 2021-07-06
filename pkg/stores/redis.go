package stores

import (
	"context"
	"sync"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type redisStore struct {
	mu          sync.Mutex
	redisClient *redis.Client
	ctx         context.Context
}

func NewRedisStore(redisClient *redis.Client) *redisStore {
	store := redisStore{
		redisClient: redisClient,
		ctx:         context.TODO(),
	}
	return &store
}

func (s *redisStore) Set(key string, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.redisClient.Set(s.ctx, key, value, 0)
	if status.Err() != nil {
		return status.Err()
	}

	return nil
}

func (s *redisStore) Get(key string) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret := s.redisClient.Get(s.ctx, key)
	if ret.Err() != nil {
		return nil, ret.Err()
	}

	return ret.Val(), nil
}

func (s *redisStore) Increment(key string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := s.redisClient.Incr(s.ctx, key)

	if ret.Err() != nil {
		return 0, ret.Err()
	}

	return uint64(ret.Val()), nil
}

func (s *redisStore) Contains(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := s.redisClient.Exists(s.ctx, key)
	if ret.Err() != nil {
		log.Error(ret.Err())
		return false
	}
	return ret.Val() > 0
}

func (s *redisStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.redisClient.Close()
}
