package stores

import (
	"bytes"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/storage/key"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

var (
	eventPrefix     byte = 0x02
	counterKey           = append([]byte{eventPrefix}, []byte("counter")...)
	binCounterKey        = append([]byte{eventPrefix}, []byte("bin_counter")...)
	eventKey             = append([]byte{eventPrefix}, []byte("event")...)
	EventFlushCount      = 100000 // decreasing this reduces memory utilization, but also performance
)

func init() {
}

type EventStore struct {
	sync.Mutex
	storage    storage.Storage
	counter    uint64
	buffer     []events.Event
	binCounter map[string]uint64
	log        *zap.SugaredLogger
}

func NewEventStore(storage storage.Storage) *EventStore {
	eventStore := &EventStore{
		storage: storage,
		log:     zap.L().Sugar().With("service", "event-store"),
	}

	eventStore.init()
	return eventStore
}

func (es *EventStore) init() {
	// load counter
	v, err := es.storage.Get(counterKey)
	if err != nil && err != badger.ErrKeyNotFound {
		es.log.Error(err)
	} else if err == badger.ErrKeyNotFound {
		es.log.Info("Initialize event counter as 0")
		es.counter = 0
	} else {
		es.counter = storage.BytesToUint64(v)
	}

	// load bin counter
	es.binCounter = make(map[string]uint64)
	v2, err := es.storage.Get([]byte(binCounterKey))
	if err != nil && err != badger.ErrKeyNotFound {
		es.log.Error(err)
	} else if err == badger.ErrKeyNotFound {
		es.log.Info("Initialize event bin counters as 0")
	} else {
		if err := msgpack.Unmarshal(v2, &es.binCounter); err != nil {
			es.log.Error(err)
		}
	}
}

func (es *EventStore) Append(event *events.Event) error {
	es.Lock()
	defer es.Unlock()

	// increase event counter
	es.counter++

	binKey := event.Timestamp.Format("2006010215")
	_, ok := es.binCounter[binKey]
	if !ok {
		es.binCounter[binKey] = 1
	} else {
		es.binCounter[binKey]++
	}

	es.buffer = append(es.buffer, *event)
	if len(es.buffer) >= EventFlushCount {
		err := es.flush()
		if err != nil {
			return err
		}
	}

	return nil
}

func (es *EventStore) GetLast(count int) ([]events.Event, error) {
	es.Lock()
	defer es.Unlock()

	var event_arr []events.Event
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
		err = es.storage.IterateReverse(prefix[:8], func(key []byte, getValue func() ([]byte, error)) (bool, error) {
			if !bytes.Equal(key[:8], prefix[:8]) {
				es.log.Warn("Prefix search included wrong items. Abort search.")
				return false, nil
			}
			var evts []events.Event
			value, err := getValue()
			if err != nil {
				return false, err
			}
			err = msgpack.Unmarshal(value, &evts)
			if err != nil {
				return false, err
			}
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
	es.Lock()
	defer es.Unlock()

	// we need to copy the map as concurrent read and write operations are not allowed (and we unlock the mutex after returning)
	copiedMap := make(map[string]uint64)
	for key, value := range es.binCounter {
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

// flush flushes the current event buffer as a block to the indexed BadgerDB
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
	err = es.storage.Set(prefix, buffer)
	if err != nil {
		return err
	}

	// commit the event counter
	if err = es.storage.Set(counterKey, storage.Uint64ToBytes(es.counter)); err != nil {
		return err
	}

	// commit the bin counter
	b, err := msgpack.Marshal(&es.binCounter)
	if err != nil {
		return err
	}

	if err = es.storage.Set(binCounterKey, b); err != nil {
		return err
	}

	es.buffer = nil // reset the buffer
	return nil
}
