package stores

import (
	"sync"

	"github.com/pbudner/argosminer/stores/backends"
)

type KvStore struct {
	sync.Mutex
	store backends.StoreBackend
}

func NewKvStore(storeGenerator backends.StoreBackendGenerator) *KvStore {
	return &KvStore{
		store: storeGenerator("kv"),
	}
}

func (kv *KvStore) Set(key []byte, value []byte) error {
	kv.Lock()
	defer kv.Unlock()
	return kv.store.Set(key, value)
}

func (kv *KvStore) Get(key []byte) ([]byte, error) {
	kv.Lock()
	defer kv.Unlock()
	return kv.store.Get(key)
}

func (kv *KvStore) Increment(key []byte) (uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	return kv.store.Increment(key)
}

func (kv *KvStore) Find(prefix []byte) ([]backends.KeyValue, error) {
	kv.Lock()
	defer kv.Unlock()
	return kv.store.Find(prefix)
}
