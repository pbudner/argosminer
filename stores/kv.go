package stores

import (
	"sync"

	"github.com/pbudner/argosminer/storage"
)

var (
	kvPrefix = []byte{0x01}
)

type KvStore struct {
	sync.RWMutex
	storage storage.Storage
}

func NewKvStore(storage storage.Storage) *KvStore {
	return &KvStore{
		storage: storage,
	}
}

func (kv *KvStore) Set(key []byte, value []byte) error {
	kv.Lock()
	defer kv.Unlock()
	pKey := append(kvPrefix, key...)
	return kv.storage.Set(pKey, value)
}

func (kv *KvStore) Get(key []byte) ([]byte, error) {
	kv.RLock()
	defer kv.RUnlock()
	pKey := append(kvPrefix, key...)
	return kv.storage.Get(pKey)
}

func (kv *KvStore) Increment(key []byte) (uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	pKey := append(kvPrefix, key...)
	return kv.storage.Increment(pKey)
}

func (kv *KvStore) Close() {
	kv.Lock()
	defer kv.Unlock()
}
