package stores

import (
	"sync"

	"github.com/pbudner/argosminer/storage"
)

var (
	kvStoreSingletonOnce sync.Once
	kvStoreSingleton     *KvStore
	kvPrefix             = []byte{0x01}
)

type KvStore struct {
	sync.RWMutex
}

func NewKvStore() *KvStore {
	kvStoreSingletonOnce.Do(func() {
		kvStoreSingleton = &KvStore{}
	})
	return kvStoreSingleton
}

func (kv *KvStore) Set(key []byte, value []byte) error {
	kv.Lock()
	defer kv.Unlock()
	pKey := append(kvPrefix, key...)
	return storage.DefaultStorage.Set(pKey, value)
}

func (kv *KvStore) Get(key []byte) ([]byte, error) {
	kv.RLock()
	defer kv.RUnlock()
	pKey := append(kvPrefix, key...)
	return storage.DefaultStorage.Get(pKey)
}

func (kv *KvStore) Increment(key []byte) (uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	pKey := append(kvPrefix, key...)
	return storage.DefaultStorage.Increment(pKey)
}

func (kv *KvStore) Close() {
	kv.Lock()
	defer kv.Unlock()
}
