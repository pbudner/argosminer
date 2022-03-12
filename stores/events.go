package stores

import (
	"bytes"
	"sync"
	"time"

	"github.com/pbudner/argosminer/pipeline"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/storage/key"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

var (
	eventStoreSingletonOnce sync.Once
	eventStoreSingleton     *EventStore
	eventPrefix             byte = 0x02
	counterKey                   = append([]byte{eventPrefix}, []byte("counter")...)
	binEventCounterKey           = append([]byte{eventPrefix}, []byte("bin_counter")...)
	binProcessCounterKey         = append([]byte{eventPrefix}, []byte("bin_counter_process_instances")...)
	eventKey                     = append([]byte{eventPrefix}, []byte("event")...)
	EventFlushCount              = 100000 // decreasing this reduces memory utilization, but also performance
)

func init() {
}

type EventStore struct {
	sync.RWMutex
	counter           uint64
	buffer            []pipeline.Event
	eventBinCounter   map[string]uint64 // stores a counter for each hour (key = yyyymmddHH)
	processBinCounter map[string]uint64 // stores a counter for each hour (key = yyyymmddHH)
	log               *zap.SugaredLogger
}

func GetEventStore() *EventStore {
	eventStoreSingletonOnce.Do(func() {
		eventStoreSingleton = &EventStore{
			log: zap.L().Sugar().With("service", "event-store"),
		}
		eventStoreSingleton.init()
	})

	return eventStoreSingleton
}

func (es *EventStore) init() {
	es.Lock()
	defer es.Unlock()

	// load counter
	v, err := storage.DefaultStorage.Get(counterKey)
	if err != nil && err != storage.ErrKeyNotFound {
		es.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		es.log.Info("Initialize event counter as 0")
		es.counter = 0
	} else {
		es.counter = storage.BytesToUint64(v)
	}

	// load event bin counter
	es.eventBinCounter = make(map[string]uint64)
	v2, err := storage.DefaultStorage.Get([]byte(binEventCounterKey))
	if err != nil && err != storage.ErrKeyNotFound {
		es.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		es.log.Info("Initialize event bin counters as 0")
	} else {
		if err := msgpack.Unmarshal(v2, &es.eventBinCounter); err != nil {
			es.log.Error(err)
		}
	}

	// load bin counter
	es.processBinCounter = make(map[string]uint64)
	v3, err := storage.DefaultStorage.Get([]byte(binProcessCounterKey))
	if err != nil && err != storage.ErrKeyNotFound {
		es.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		es.log.Info("Initialize process bin counters as 0")
	} else {
		if err := msgpack.Unmarshal(v3, &es.processBinCounter); err != nil {
			es.log.Error(err)
		}
	}
}

func (es *EventStore) Append(event pipeline.Event) error {
	es.Lock()
	defer es.Unlock()

	// increase event counter
	es.counter++

	binKey := event.Timestamp.Format("2006010215")
	_, ok := es.eventBinCounter[binKey]
	if !ok {
		es.eventBinCounter[binKey] = 1
	} else {
		es.eventBinCounter[binKey]++
	}

	es.buffer = append(es.buffer, event)
	if len(es.buffer) >= EventFlushCount {
		err := es.flush()
		if err != nil {
			return err
		}
	}

	return nil
}

func (es *EventStore) GetLast(count int) ([]pipeline.Event, error) {
	es.RLock()
	defer es.RUnlock()

	var event_arr []pipeline.Event
	if count > len(es.buffer) {
		event_arr = es.buffer[:]
	} else {
		index := len(es.buffer) - count
		event_arr = es.buffer[index:]
	}

	if len(event_arr) < count {
		prefix, err := key.New(eventKey, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		err = storage.DefaultStorage.IterateReverse(prefix[:8], func(key []byte, getValue func() ([]byte, error)) (bool, error) {
			if !bytes.Equal(key[:8], prefix[:8]) {
				es.log.Warn("Prefix search included wrong items. Abort search. This should not happen.")
				return false, nil
			}
			var evts []pipeline.Event
			value, err := getValue()
			if err != nil {
				return false, err
			}
			err = msgpack.Unmarshal(value, &evts)
			if err != nil {
				return false, err
			}
			es.log.Info(evts)
			idx := len(evts) - (count - len(event_arr))
			if idx < 0 {
				idx = 0
			}

			event_arr = append(event_arr, evts[idx:]...)
			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	return event_arr, nil
}

func (es *EventStore) GetBinCount() (map[string]uint64, error) {
	es.RLock()
	defer es.RUnlock()

	// we need to copy the map as concurrent read and write operations are not allowed (and we unlock the mutex after returning)
	copiedMap := make(map[string]uint64)
	for key, value := range es.eventBinCounter {
		copiedMap[key] = value
	}

	return copiedMap, nil
}

func (es *EventStore) GetCount() uint64 {
	return es.counter
}

func (es *EventStore) Close() {
	es.Lock()
	defer es.Unlock()
	if err := es.flush(); err != nil {
		es.log.Error(err)
	}
}

// flush flushes the current event buffer as a block to the indexed database
func (es *EventStore) flush() error {
	if es.buffer == nil || len(es.buffer) == 0 {
		return nil
	}

	prefix, err := key.New(eventKey, time.Now().UTC())
	if err != nil {
		return err
	}
	buffer, err := msgpack.Marshal(&es.buffer)
	if err != nil {
		return err
	}
	err = storage.DefaultStorage.Set(prefix, buffer)
	if err != nil {
		return err
	}

	// commit the event counter
	if err = storage.DefaultStorage.Set(counterKey, storage.Uint64ToBytes(es.counter)); err != nil {
		return err
	}

	// commit the bin counter
	b, err := msgpack.Marshal(&es.eventBinCounter)
	if err != nil {
		return err
	}

	if err = storage.DefaultStorage.Set(binEventCounterKey, b); err != nil {
		return err
	}

	es.buffer = nil // reset the buffer
	return nil
}
