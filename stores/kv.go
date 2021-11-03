package stores

import (
	"sync"

	"github.com/pbudner/argosminer/storage"
)

type KvStore struct {
	sync.Mutex
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
	return kv.storage.Set(key, value)
}

func (kv *KvStore) Get(key []byte) ([]byte, error) {
	kv.Lock()
	defer kv.Unlock()
	return kv.storage.Get(key)
}

func (kv *KvStore) Increment(key []byte) (uint64, error) {
	kv.Lock()
	defer kv.Unlock()
	return kv.storage.Increment(key)
}

func (kv *KvStore) Find(prefix []byte, f func(storage.KeyValue) (bool, error)) error {
	kv.Lock()
	defer kv.Unlock()
	return kv.storage.Find(prefix, f)
}

func (kv *KvStore) Close() {
	kv.Lock()
	defer kv.Unlock()
	kv.storage.Close()
}
