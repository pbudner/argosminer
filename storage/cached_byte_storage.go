package storage

import (
	"context"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/jellydator/ttlcache/v3"
)

type CachedByteStorage struct {
	config CachedByteStorageConfig
	store  Storage
	cache  *ttlcache.Cache[uint64, KeyValue[[]byte, []byte]]
}

type CachedByteStorageConfig struct {
	StoragePrefix []byte
	TTL           time.Duration
	MaxItems      uint64
}

func NewCachedByteStorage(storage Storage, config CachedByteStorageConfig) *CachedByteStorage {
	cache := ttlcache.New(
		ttlcache.WithTTL[uint64, KeyValue[[]byte, []byte]](config.TTL),
		ttlcache.WithCapacity[uint64, KeyValue[[]byte, []byte]](config.MaxItems),
	)

	cache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[uint64, KeyValue[[]byte, []byte]]) {
		key := append(config.StoragePrefix, item.Value().Key...)
		storage.Set(key, item.Value().Value)
	})

	// start cleanup process to free up memory
	go cache.Start()
	return &CachedByteStorage{
		config: config,
		cache:  cache,
		store:  storage,
	}
}

func (c CachedByteStorage) Get(key []byte) ([]byte, bool) {
	var result []byte
	hashedKey := xxhash.Sum64(key)
	item := c.cache.Get(hashedKey)
	if item == nil {
		// try to get the item from storage
		pKey := append(c.config.StoragePrefix, key...)
		value, err := c.store.Get(pKey)
		if err != nil {
			return result, false
		}

		// put item into cache again
		c.Set(key, value)
		return value, true
	}

	return item.Value().Value, true
}

func (c CachedByteStorage) Set(key []byte, value []byte) {
	hashedKey := xxhash.Sum64(key)
	c.cache.Set(hashedKey, KeyValue[[]byte, []byte]{
		Key:   key,
		Value: value,
	}, ttlcache.DefaultTTL)
}

func (c CachedByteStorage) Close() {
	c.cache.Stop()

	// iterate through all items and save them on disk
	for _, kv := range c.cache.Items() {
		key := append(c.config.StoragePrefix, kv.Value().Key...)
		c.store.Set(key, kv.Value().Value)
	}

	c.cache = nil
}
