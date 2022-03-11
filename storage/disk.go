package storage

import (
	"bytes"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/prometheus/client_golang/prometheus"

	"go.uber.org/zap"
)

const (
	valueLogGCDiscardRatio       = 0.5
	maintenanceIntervalInMinutes = 5
	monitorIntervalInMiliseconds = 1000
)

var (
	diskStorageMaintenanceCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "argosminer_storage_disk",
		Name:      "maintenance_total",
		Help:      "Count of maintenance runs.",
	}, []string{"path"})

	diskStorageFinishedMaintenanceCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "argosminer_storage_disk",
		Name:      "maintenance_finished_total",
		Help:      "Count of finished maintenance runs.",
	}, []string{"path"})

	lsmSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "argosminer_storage_disk",
		Name:      "lsm_size",
		Help:      "Size of the log-structured merge tree in bytes.",
	}, []string{"path"})

	vlogSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "argosminer_storage_disk",
		Name:      "vlog_size",
		Help:      "Size of the value log in bytes.",
	}, []string{"path"})
)

func init() {
	prometheus.MustRegister(diskStorageMaintenanceCounter, diskStorageFinishedMaintenanceCounter, lsmSizeGauge, vlogSizeGauge)
}

type diskStorage struct {
	store            *badger.DB
	closeMaintenance chan bool
	closeMonitor     chan bool
	path             string
	log              *zap.SugaredLogger
}

func NewDiskStorage(config Config) *diskStorage {
	log := zap.L().Sugar()
	opts := badger.DefaultOptions(config.Path)
	opts = opts.WithSyncWrites(config.SyncWrites).WithLogger(defaultLogger(log.With("service", "badger-db"))).WithDetectConflicts(false)

	// open the database
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}

	store := diskStorage{
		store:            db,
		closeMaintenance: make(chan bool),
		closeMonitor:     make(chan bool),
		path:             config.Path,
		log:              log.With("service", "disk-storage"),
	}

	go store.maintenance()
	go store.monitor()
	return &store
}

func (s *diskStorage) Set(key []byte, value []byte) error {
	return s.store.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *diskStorage) SetBatch(batch []KeyValue) error {
	writeBatch := s.store.NewWriteBatch()
	defer writeBatch.Cancel()
	for _, kv := range batch {
		if len(kv.Key) == 0 {
			s.log.Error("Key has a length of 0. This should not happen!")
		}
		err := writeBatch.Set(kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}
	return writeBatch.Flush()
}

func (s *diskStorage) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	var value []byte
	err := s.store.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrKeyNotFound
			}
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
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

			// TODO: We could increase performance here, if we would increment on byte level...
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
			if err == badger.ErrKeyNotFound {
				return ErrKeyNotFound
			}
			return err
		}
		return nil
	})

	return err == nil
}

func (s *diskStorage) IterateReverse(prefix []byte, f func([]byte, func() ([]byte, error)) (bool, error)) error {
	err := s.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		seekPrefix := append(prefix[:], 0xFF)
		for it.Seek(seekPrefix); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)

			valueFunc := func() ([]byte, error) {
				itemBytes, err := item.ValueCopy(nil)
				if err != nil {
					return nil, err
				}

				return itemBytes, nil
			}

			if ok, err := f(key, valueFunc); !ok {
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

func (s *diskStorage) Iterate(prefix []byte, f func([]byte, func() ([]byte, error)) (bool, error)) error {
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

			valueFunc := func() ([]byte, error) {
				itemBytes, err := item.ValueCopy(nil)
				if err != nil {
					return nil, err
				}

				return itemBytes, nil
			}

			if ok, err := f(key, valueFunc); !ok {
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

func (s *diskStorage) Seek(key []byte) (KeyValue, error) {
	var result KeyValue
	err := s.store.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		opts.Prefix = key[:8]
		it := txn.NewIterator(opts)
		defer it.Close()
		seekKey := append(key[:], 0xFF)
		it.Seek(seekKey)
		if !it.ValidForPrefix(key[:8]) {
			return ErrNoResults
		}
		item := it.Item()
		itemKey := item.Key()
		itemBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		result = KeyValue{
			Key:   itemKey,
			Value: itemBytes,
		}
		return nil
	})

	return result, err
}

func (s *diskStorage) maintenance() {
	maintenanceTicker := time.NewTicker(maintenanceIntervalInMinutes * time.Minute)
	defer maintenanceTicker.Stop()
	for {
		select {
		case <-s.closeMaintenance:
			return
		case <-maintenanceTicker.C:
			var err error
			s.log.Debug("Performing maintenance on database")
			diskStorageMaintenanceCounter.WithLabelValues(s.path).Inc()
			for err == nil {
				select {
				case <-s.closeMaintenance:
					return
				default:
					s.log.Debug("Calling RunValueLogGC()")
					err = s.store.RunValueLogGC(valueLogGCDiscardRatio)
				}
			}

			if err == badger.ErrNoRewrite {
				diskStorageFinishedMaintenanceCounter.WithLabelValues(s.path).Inc()
				s.log.Debug("Successfully finished maintenance on database")
			} else {
				s.log.Error("Failed to run maintenance on database:", err)
			}

			// we reset the timer, because sometimes the maintenance intervall takes too much time
			maintenanceTicker.Reset(maintenanceIntervalInMinutes * time.Minute)
		}
	}
}

func (s *diskStorage) monitor() {
	monitorTicker := time.NewTicker(monitorIntervalInMiliseconds * time.Millisecond)
	defer monitorTicker.Stop()
	for {
		select {
		case <-s.closeMonitor:
			return
		case <-monitorTicker.C:
			s.log.Debug("Update monitoring metrics")
			lsm, vlog := s.store.Size()
			lsmSizeGauge.WithLabelValues(s.path).Set(float64(lsm))
			vlogSizeGauge.WithLabelValues(s.path).Set(float64(vlog))
		}
	}
}

func (s *diskStorage) Close() {
	close(s.closeMaintenance)
	close(s.closeMonitor)
	err := s.store.Close()
	if err != nil {
		s.log.Error(err)
	}
}
