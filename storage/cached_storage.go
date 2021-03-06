package storage

import (
	"context"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/jellydator/ttlcache/v3"
)

type Serializable interface {
	Marshal() []byte
	Unmarshal([]byte) Serializable
}

type CachedStorage[T Serializable] struct {
	config CachedStorageConfig
	store  Storage
	cache  *ttlcache.Cache[uint64, KeyValue[[]byte, Serializable]]
}

type CachedStorageConfig struct {
	StoragePrefix []byte
	TTL           time.Duration
	MaxItems      uint64
}

func NewCachedByteStorage[T Serializable](storage Storage, config CachedStorageConfig) *CachedStorage[T] {
	cache := ttlcache.New(
		ttlcache.WithTTL[uint64, KeyValue[[]byte, Serializable]](config.TTL),
		ttlcache.WithCapacity[uint64, KeyValue[[]byte, Serializable]](config.MaxItems),
	)

	cache.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[uint64, KeyValue[[]byte, Serializable]]) {
		key := append(config.StoragePrefix, item.Value().Key...)
		storage.Set(key, item.Value().Value.Marshal())
	})

	// start cleanup process to free up memory
	go cache.Start()
	return &CachedStorage[T]{
		config: config,
		cache:  cache,
		store:  storage,
	}
}

func (c CachedStorage[T]) Get(key []byte) (Serializable, bool) {
	var result T
	hashedKey := xxhash.Sum64(key)
	item := c.cache.Get(hashedKey)
	if item == nil {
		// try to get the item from storage
		pKey := append(c.config.StoragePrefix, key...)
		value, err := c.store.Get(pKey)
		if err != nil {
			return result, false
		}
		umarshalledValue := result.Unmarshal(value)
		c.Set(key, umarshalledValue) // put item into cache again
		return umarshalledValue, true
	}
	return item.Value().Value, true
}

func (c CachedStorage[T]) Set(key []byte, value Serializable) {
	hashedKey := xxhash.Sum64(key)
	c.cache.Set(hashedKey, KeyValue[[]byte, Serializable]{
		Key:   key,
		Value: value,
	}, ttlcache.DefaultTTL)
}

func (c CachedStorage[T]) Close() {
	c.cache.Stop()

	// iterate through all items and save them on disk
	for _, kv := range c.cache.Items() {
		key := append(c.config.StoragePrefix, kv.Value().Key...)
		c.store.Set(key, kv.Value().Value.Marshal())
	}

	c.cache = nil
}
