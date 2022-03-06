package stores

/*
import (
	"sync"
	"time"

	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/storage/key"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	processStoreSingletonOnce sync.Once
	processStoreSingleton     *ProcessStore
	processPrefix             = []byte{0x01, 0x01}
)

type ProcessStore struct {
	sync.RWMutex
}

func GetProcessStore() *ProcessStore {
	processStoreSingletonOnce.Do(func() {
		processStoreSingleton = &ProcessStore{}
	})
	return processStoreSingleton
}

func (kv *ProcessStore) Set(name string, activities []string) error {
	kv.Lock()
	defer kv.Unlock()
	key, err := key.New(append(processPrefix, []byte("process-name")...), time.Now().UTC())
	if err != nil {
		return err
	}

	b, err := msgpack.Marshal(&activities)
	if err != nil {
		return err
	}

	return storage.DefaultStorage.Set(key, b)
}

func (kv *ProcessStore) Update(name string, activities []string) error {
	kv.Lock()
	defer kv.Unlock()
	key, err := key.New(append(processPrefix, []byte("process-name")), time.Now().UTC())
	if err != nil {
		return err
	}

	b, err := msgpack.Marshal(&activities)
	if err != nil {
		return err
	}

	return storage.DefaultStorage.Set(key, b)
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
*/
