package stores

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"

	badger "github.com/dgraph-io/badger/v3"
)

type badgerStore struct {
	store *badger.DB
}

func NewBadgerStoreGenerator() StoreGenerator {
	return func(storeId interface{}) Store {
		return NewBadgerStore(storeId.(int))
	}
}

func NewBadgerStore(storeId int) *badgerStore {
	db, err := badger.Open(badger.DefaultOptions(fmt.Sprintf("/Volumes/PascalsSSD/ArgosMiner/badger%d", storeId)))
	if err != nil {
		log.Fatal(err)
	}

	store := badgerStore{
		store: db,
	}
	return &store
}

func (s *badgerStore) Set(key string, value interface{}) error {
	iValue, ok := value.(uint64)
	if !ok {
		return fmt.Errorf("value is not a uint64")
	}

	return s.store.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), uint64ToBytes(iValue))
		return err
	})
}

func (s *badgerStore) Get(key string) (interface{}, error) {
	var value []byte
	err := s.store.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
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

	return bytesToUint64(value), nil
}

func (s *badgerStore) Increment(key string, timestamp time.Time) (uint64, error) {
	var itemValue uint64
	err := s.store.Update(func(txn *badger.Txn) error {
		// retrieve stored value
		item, err := txn.Get([]byte(key))
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

			itemValue = bytesToUint64(valCopy)
		}

		itemValue++

		// update value
		err = txn.Set([]byte(key), uint64ToBytes(itemValue))
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

func (s *badgerStore) Contains(key string) bool {
	err := s.store.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return nil
	})

	return err == nil
}

func (s *badgerStore) EncodeDirectlyFollowsRelation(from string, to string) string {
	if from == "" {
		return to
	}
	return fmt.Sprintf("%s -> %s", from, to)
}

func (s *badgerStore) EncodeActivity(activity string) string {
	return activity
}

func (s *badgerStore) Close() {
	s.store.Close()
}

func uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
