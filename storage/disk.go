package storage

import (
	"bytes"
	"errors"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	log "github.com/sirupsen/logrus"
)

const (
	valueLogGCDiscardRatio       = 0.5
	maintenanceIntervalInMinutes = 5
)

type diskStorage struct {
	store              *badger.DB
	maintenanceDone    chan bool
	maintenanceRunning bool
}

func NewDiskStorageGenerator() StorageGenerator {
	return func(storeId string) Storage {
		return NewDiskStorage(storeId)
	}
}

func NewDiskStorage(dataPath string) *diskStorage {
	opts := badger.DefaultOptions(dataPath)
	opts = opts.WithSyncWrites(true).WithLogger(log.StandardLogger()).WithDetectConflicts(false)

	// open the database
	db, err := badger.Open(opts)
	if err != nil {
		return nil
	}

	store := diskStorage{
		store:           db,
		maintenanceDone: make(chan bool),
	}

	go store.maintenance()
	return &store
}

func (s *diskStorage) Set(key []byte, value []byte) error {
	return s.store.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		return err
	})
}

func (s *diskStorage) SetBatch(batch []KeyValue) error {
	writeBatch := s.store.NewWriteBatch()
	defer writeBatch.Cancel()
	for _, kv := range batch {
		if len(kv.Key) == 0 {
			log.Error("Key has a length of 0. This should not happen!")
		}
		err := writeBatch.Set(kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}
	return writeBatch.Flush()
}

func (s *diskStorage) Get(key []byte) ([]byte, error) {
	var value []byte
	if len(key) == 0 {
		return nil, errors.New("empty key")
	}

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

func (s *diskStorage) Increment(key []byte) (uint64, error) {
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
			itemValue = BytesToUint64(valCopy)
		}

		itemValue++

		// update value
		err = txn.Set(key, Uint64ToBytes(itemValue))
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

func (s *diskStorage) Contains(key []byte) bool {
	err := s.store.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			return err
		}
		return nil
	})

	return err == nil
}

func (s *diskStorage) IterateReverse(prefix []byte, f func(KeyValue) (bool, error)) error {
	err := s.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		seekPrefix := append(prefix[:], 0xFF)
		for it.Seek(seekPrefix); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			itemBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			if ok, err := f(KeyValue{Key: key, Value: itemBytes}); !ok {
				return err
			}
		}

		return nil
	})

	return err
}

func (s *diskStorage) GetFirst(count int) ([][]byte, error) {
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

func (s *diskStorage) GetRange(from []byte, to []byte) ([][]byte, error) {
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

func (s *diskStorage) Iterate(prefix []byte, f func(KeyValue) (bool, error)) error {
	err := s.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			itemBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			if ok, err := f(KeyValue{Key: key, Value: itemBytes}); !ok {
				return err
			}
		}

		return nil
	})

	return err
}

func (s *diskStorage) CountPrefix(prefix []byte) (uint64, error) {
	counter := uint64(0)
	err := s.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			counter++
		}

		return nil
	})

	return counter, err
}

func (s *diskStorage) TotalCount() (uint64, error) {
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

func (s *diskStorage) CountRange(from []byte, to []byte) (uint64, error) {
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

func (s *diskStorage) maintenance() {
	maintenanceTicker := time.NewTicker(maintenanceIntervalInMinutes * time.Minute)
	maintenanceRunning := false
	defer maintenanceTicker.Stop()
	for {
		select {
		case <-s.maintenanceDone:
			return
		case <-maintenanceTicker.C:
			if maintenanceRunning {
				continue
			}

			var err error
			log.Info("Performing maintenance on database")
			for err == nil {
				select {
				case <-s.maintenanceDone:
					return
				default:
					log.Trace("RunValueLogGC")
					s.store.RunValueLogGC(valueLogGCDiscardRatio)
				}
			}

			maintenanceRunning = false
			if err == badger.ErrNoRewrite {
				log.Debug("Successfully finished ValueLogGC")
			} else {
				log.Error("Failed to run ValueLogGC:", err)
			}
		}
	}
}

func (s *diskStorage) Close() {
	close(s.maintenanceDone)
	err := s.store.Close()
	if err != nil {
		log.Error(err)
	}
}
