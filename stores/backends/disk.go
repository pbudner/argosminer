package backends

import (
	"fmt"
	"log"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/pbudner/argosminer/stores/utils"
)

type diskStore struct {
	store *badger.DB
}

func NewDiskStoreGenerator() StoreBackendGenerator {
	return func(storeId string) StoreBackend {
		return NewDiskStore(storeId)
	}
}

func NewDiskStore(storeId string) *diskStore {
	db, err := badger.Open(badger.DefaultOptions(fmt.Sprintf("/Volumes/PascalsSSD/ArgosMiner/badger-%s", storeId)))
	if err != nil {
		log.Fatal(err)
	}

	store := diskStore{
		store: db,
	}
	return &store
}

func (s *diskStore) Set(key []byte, value []byte) error {
	return s.store.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		return err
	})
}

func (s *diskStore) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.store.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		value, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (s *diskStore) Increment(key []byte, timestamp time.Time) (uint64, error) {
	var itemValue uint64
	err := s.store.Update(func(txn *badger.Txn) error {
		// retrieve stored value
		item, err := txn.Get(key)
		if err != badger.ErrKeyNotFound {
			return err
		}

		if err == badger.ErrKeyNotFound {
			itemValue = 0
		} else {
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			itemValue = utils.BytesToUint64(valCopy)
		}

		itemValue++

		// update value
		err = txn.Set(key, utils.Uint64ToBytes(itemValue))
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return itemValue, nil
}

func (s *diskStore) Contains(key []byte) bool {
	err := s.store.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			return err
		}
		return nil
	})

	return err == nil
}

func (s *diskStore) EncodeDirectlyFollowsRelation(from []byte, to []byte) []byte {
	if len(from) == 0 {
		return to
	}

	return append(append(from, byte(0)), to[:]...)
}

func (s *diskStore) EncodeActivity(activity []byte) []byte {
	return activity
}

func (s *diskStore) Close() {
	s.store.Close()
}
