package storage

import (
	"context"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

type cachedStorage[K any, V any] struct {
	config CachedStorageConfig
	cache  *ttlcache.Cache[int, KeyValue[K, V]]
}

type CachedStorageConfig struct {
	StoragePrefix []byte
	TTL           time.Duration
	MaxItems      uint64
}

func NewCachedStorage[K any, V any](storage Storage, config CachedStorageConfig) *cachedStorage[K, V] {
	loader := ttlcache.LoaderFunc[int, KeyValue[K, V]](
		func(c *ttlcache.Cache[int, KeyValue[K, V]], key int) *ttlcache.Item[int, KeyValue[K, V]] {
			// ToDo: Get from storage and save in cache
			// item := c.Set(key, nil, ttlcache.DefaultTTL)
			return nil
		},
	)

	cache := ttlcache.New(
		ttlcache.WithTTL[int, KeyValue[K, V]](config.TTL),
		ttlcache.WithCapacity[int, KeyValue[K, V]](config.MaxItems),
		ttlcache.WithLoader[int, KeyValue[K, V]](loader),
	)

	cache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[int, KeyValue[K, V]]) {
		// ToDo: Save item on disk
	})

	// start clean up process to free up memory
	go cache.Start()

	return &cachedStorage[K, V]{
		config: config,
		cache:  cache,
	}
}
