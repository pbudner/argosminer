package backends

import (
	"bytes"
	"fmt"
	"log"

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
	db, err := badger.Open(badger.DefaultOptions(fmt.Sprintf("/Volumes/PascalsSSD/ArgosMiner/badger_%s", storeId)))
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

func (s *diskStore) Increment(key []byte) (uint64, error) {
	var itemValue uint64
	err := s.store.Update(func(txn *badger.Txn) error {
		// retrieve stored value
		item, err := txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		if err == badger.ErrKeyNotFound {
			itemValue = 0
		} else {
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			// TODO: We could increase performance here, if we increment on byte level...
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

func (s *diskStore) GetLast(count int) ([][]byte, error) {
	result := make([][]byte, count)
	err := s.store.View(func(txn *badger.Txn) error {
		var err error
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		i := 0
		for it.Rewind(); it.Valid(); it.Next() {
			if i == count {
				break
			}
			item := it.Item()
			result[i], err = item.ValueCopy(nil)
			if err != nil {
				return err
			}

			i++
		}

		return nil
	})

	return result, err
}

func (s *diskStore) GetFirst(count int) ([][]byte, error) {
	result := make([][]byte, count)
	err := s.store.View(func(txn *badger.Txn) error {
		var err error
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		i := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			result[i], err = item.ValueCopy(nil)
			if err != nil {
				return err
			}
			i++
		}

		return nil
	})

	return result, err
}

func (s *diskStore) GetRange(from []byte, to []byte) ([][]byte, error) {
	result := make([][]byte, 0)
	err := s.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		found := false
		it.Rewind()
		for it.Seek(from); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if !found {
				if bytes.Compare(key, from) < 0 {
					continue
				}
				found = true
			}

			if bytes.Compare(key, to) > 0 {
				break
			}

			itemBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result = append(result, itemBytes)
		}

		return nil
	})

	return result, err
}

func (s *diskStore) Find(prefix []byte) ([]KeyValue, error) {
	result := make([]KeyValue, 0)
	err := s.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		i := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			i++
			if i > 20000 {
				break
			}
			item := it.Item()
			key := item.KeyCopy(nil)
			itemBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result = append(result, KeyValue{Key: key, Value: itemBytes})
		}

		return nil
	})

	return result, err
}

func (s *diskStore) TotalCount() (uint64, error) {
	counter := uint64(0)
	err := s.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			counter++
		}

		return nil
	})

	return counter, err
}

func (s *diskStore) CountRange(from []byte, to []byte) (uint64, error) {
	counter := uint64(0)
	err := s.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		found := false
		it.Rewind()
		for it.Seek(from); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if !found {
				if bytes.Compare(key, from) < 0 {
					continue
				}
				found = true
			}

			if bytes.Compare(key, to) > 0 {
				break
			}

			counter++
		}

		return nil
	})
	return counter, err
}

func (s *diskStore) Close() {
	s.store.Close()
}
