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

func (es *KvStore) Set(key []byte, value []byte) error {
	es.Lock()
	defer es.Unlock()
	return es.store.Set(key, value)
}

func (es *KvStore) Get(key []byte) ([]byte, error) {
	es.Lock()
	defer es.Unlock()
	return es.store.Get(key)
}
