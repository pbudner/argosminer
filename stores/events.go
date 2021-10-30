package stores

import (
	"bytes"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/storage"
	"github.com/pbudner/argosminer/storage/key"
	log "github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

const event_flush_count = 100000 // decreasing this reduces memory utilization, but also performance
const counter_key = "counter"
const bin_counter_key = "bin_counter"

type EventStore struct {
	sync.Mutex
	storage      storage.Storage
	counter      uint64
	minTimestamp time.Time
	buffer       []storage.KeyValue
	binCounter   map[string]uint64
}

func NewEventStore(storageGenerator storage.StorageGenerator) *EventStore {
	eventStore := &EventStore{
		storage: storageGenerator("event_log"),
	}

	eventStore.init()
	return eventStore
}

func (es *EventStore) init() {
	// load counter
	v, err := es.storage.Get([]byte(counter_key))
	if err != nil && err != badger.ErrKeyNotFound {
		log.Error(err)
	} else if err == badger.ErrKeyNotFound {
		log.Info("Initialize event counter as 0")
		es.counter = 0
	} else {
		es.counter = storage.BytesToUint64(v)
	}

	// load bin counter
	es.binCounter = make(map[string]uint64)
	v2, err := es.storage.Get([]byte(bin_counter_key))
	if err != nil && err != badger.ErrKeyNotFound {
		log.Error(err)
	} else {
		if err := msgpack.Unmarshal(v2, &es.binCounter); err != nil {
			log.Error(err)
		}
	}
}

func (es *EventStore) Append(event *events.Event) error {
	es.Lock()
	defer es.Unlock()
	t := time.Now().UTC()

	if es.buffer == nil {
		es.minTimestamp = t
	}

	// increase event counter
	es.counter++

	binKey := event.Timestamp.Format("2006010215")
	_, ok := es.binCounter[binKey]
	if !ok {
		es.binCounter[binKey] = 1
	} else {
		es.binCounter[binKey]++
	}

	k, err := key.New([]byte("event"), event.Timestamp)
	if err != nil {
		return err
	}

	binEvent, err := event.Marshal()
	if err != nil {
		return err
	}

	es.buffer = append(es.buffer, storage.KeyValue{Key: k, Value: binEvent})
	if len(es.buffer) >= event_flush_count {
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

	var event_arr []storage.KeyValue
	if count > len(es.buffer) {
		event_arr = es.buffer[:]
	} else {
		index := len(es.buffer) - count
		event_arr = es.buffer[index:]
	}

	eventList := make([]events.Event, len(event_arr))
	for i, v := range event_arr {
		err := eventList[i].Unmarshal(v.Value)
		if err != nil {
			return nil, err
		}
	}

	if len(eventList) < count {
		prefix, err := key.New([]byte("event"), time.Now().UTC())
		if err != nil {
			return nil, err
		}
		err = es.storage.IterateReverse(func(kv storage.KeyValue) (bool, error) {
			if !bytes.Equal(kv.Key[:8], prefix[:8]) {
				return true, nil
			}

			var ev events.Event
			err := ev.Unmarshal(kv.Value)
			if err != nil {
				return false, err
			}

			eventList = append(eventList, ev)
			return len(eventList) < count, nil
		})

		if err != nil {
			return eventList, err
		}
	}

	return eventList, nil
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
		log.Error(err)
	}
	es.storage.Close()
}

// flush flushes the current event buffer as a block to the indexed BadgerDB
func (es *EventStore) flush() error {
	if es.buffer == nil || len(es.buffer) == 0 {
		return nil
	}

	err := es.storage.SetBatch(es.buffer)
	if err != nil {
		return err
	}

	// commit the event counter
	if err = es.storage.Set([]byte(counter_key), storage.Uint64ToBytes(es.counter)); err != nil {
		return err
	}

	// commit the bin counter
	b, err := msgpack.Marshal(es.binCounter)
	if err != nil {
		return err
	}

	if err = es.storage.Set([]byte(bin_counter_key), b); err != nil {
		return err
	}

	es.buffer = nil // reset the buffer
	return nil
}
