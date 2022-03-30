package stores

import (
	"container/list"
	"sync"
	"time"

	"github.com/pbudner/argosminer/encoding"
	"github.com/pbudner/argosminer/pipeline"
	"github.com/pbudner/argosminer/storage"
	"go.uber.org/zap"
)

const MAX_EVENTS_IN_LAST_EVENTS_BUFFER = 50

var (
	eventStoreSingletonOnce sync.Once
	eventStoreSingleton     *EventStore
	eventStorePrefix        byte = 0x02
	eventCounterKey              = append([]byte{eventStorePrefix}, []byte("event_counter")...)
	binEventCounterKey           = append([]byte{eventStorePrefix}, []byte("bin_counter")...)
	caseCounterKey               = append([]byte{eventStorePrefix}, []byte("case_counter")...)
	binCaseCounterKey            = append([]byte{eventStorePrefix}, []byte("bin_counter_process_instances")...)
	casePrefix                   = append([]byte{eventStorePrefix}, []byte("case")...)
	actionIndexPrefix            = append([]byte{eventStorePrefix}, []byte("action")...)
	lastEventsKey                = append([]byte{eventStorePrefix}, []byte("last_events")...)
	EventFlushCount              = 100000 // decreasing this reduces memory utilization, but also performance
)

func init() {
}

type Case struct {
	ID             string
	FirstTimestamp time.Time
	LastTimestamp  time.Time
	Events         []pipeline.Event
}

type EventStore struct {
	sync.RWMutex
	lastEventsBuffer *list.List
	eventCounter     uint64
	caseCounter      uint64
	caseBuffer       map[string]Case   // maps a case id to an instantiated case
	eventBinCounter  map[string]uint64 // maps a date to an event counter used for binning
	caseBinCounter   map[string]uint64 // maps a date to a case counter used for binning
	log              *zap.SugaredLogger
}

func GetEventStore() *EventStore {
	eventStoreSingletonOnce.Do(func() {
		eventStoreSingleton = &EventStore{
			log:        zap.L().Sugar().With("service", "event-store"),
			caseBuffer: make(map[string]Case),
		}
		eventStoreSingleton.init()
	})

	return eventStoreSingleton
}

func (es *EventStore) init() {
	es.Lock()
	defer es.Unlock()

	// load event counter
	v, err := storage.DefaultStorage.Get(eventCounterKey)
	if err != nil && err != storage.ErrKeyNotFound {
		es.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		es.log.Info("Initialize event counter as 0")
		es.eventCounter = 0
	} else {
		es.eventCounter = storage.BytesToUint64(v)
	}

	// load event bin counter
	es.eventBinCounter = make(map[string]uint64)
	v2, err := storage.DefaultStorage.Get([]byte(binEventCounterKey))
	if err != nil && err != storage.ErrKeyNotFound {
		es.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		es.log.Info("Initialize event bin counters as 0")
	} else {
		if err := encoding.Gob.Unmarshal(v2, &es.eventBinCounter); err != nil {
			es.log.Error(err)
		}
	}

	// load case counter
	v3, err := storage.DefaultStorage.Get(caseCounterKey)
	if err != nil && err != storage.ErrKeyNotFound {
		es.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		es.log.Info("Initialize case counter as 0")
		es.caseCounter = 0
	} else {
		es.caseCounter = storage.BytesToUint64(v3)
	}

	// load case bin counter
	es.caseBinCounter = make(map[string]uint64)
	v4, err := storage.DefaultStorage.Get([]byte(binCaseCounterKey))
	if err != nil && err != storage.ErrKeyNotFound {
		es.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		es.log.Info("Initialize case bin counter as 0")
	} else {
		if err := encoding.Gob.Unmarshal(v4, &es.caseBinCounter); err != nil {
			es.log.Error(err)
		}
	}

	// load last events
	var lastEventsAsArray []pipeline.Event
	es.lastEventsBuffer = list.New()
	v5, err := storage.DefaultStorage.Get([]byte(lastEventsKey))
	if err != nil && err != storage.ErrKeyNotFound {
		es.log.Error(err)
	} else if err == storage.ErrKeyNotFound {
		es.log.Info("Initialize without last events buffer")
	} else {
		if err := encoding.Gob.Unmarshal(v5, &lastEventsAsArray); err != nil {
			es.log.Error(err)
		} else {
			for _, event := range lastEventsAsArray {
				es.lastEventsBuffer.PushBack(event)
			}
		}
	}
}

func (es *EventStore) Append(event pipeline.Event) error {
	es.Lock()
	defer es.Unlock()

	// increase event counter
	es.eventCounter++

	// increase event counter
	binKey := event.Timestamp.Format("2006010215")
	_, ok := es.eventBinCounter[binKey]
	if !ok {
		es.eventBinCounter[binKey] = 1
	} else {
		es.eventBinCounter[binKey]++
	}

	// add event to case
	caseInstance := es.addEventToCase(event)

	// if the case is new, increase case counter
	if len(caseInstance.Events) == 1 {
		es.caseCounter++
		_, ok := es.caseBinCounter[binKey]
		if !ok {
			es.caseBinCounter[binKey] = 1
		} else {
			es.caseBinCounter[binKey]++
		}
	}

	// add event to last events buffer
	es.lastEventsBuffer.PushBack(event)
	if es.lastEventsBuffer.Len() >= MAX_EVENTS_IN_LAST_EVENTS_BUFFER {
		es.lastEventsBuffer.Remove(es.lastEventsBuffer.Front())
	}

	return nil
}

func (es *EventStore) addEventToCase(event pipeline.Event) Case {
	caseInstantiation, ok := es.caseBuffer[event.CaseId]
	if !ok {
		caseInstantiation = Case{
			ID:             event.CaseId,
			FirstTimestamp: event.Timestamp,
			LastTimestamp:  event.Timestamp,
			Events:         []pipeline.Event{event},
		}
		es.caseBuffer[event.CaseId] = caseInstantiation
	} else {
		caseInstantiation.Events = append(caseInstantiation.Events, event)
	}
	return caseInstantiation
}

func (es *EventStore) GetLast(count int) ([]pipeline.Event, error) {
	es.RLock()
	defer es.RUnlock()

	var event_arr []pipeline.Event
	for element := es.lastEventsBuffer.Front(); len(event_arr) < count && element != nil; element = element.Next() {
		event_arr = append(event_arr, element.Value.(pipeline.Event))
	}

	/*
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
				err = encoding.Gob.Unmarshal(value, &evts)
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
	*/

	return event_arr, nil
}

func (es *EventStore) GetCaseBinCount() (map[string]uint64, error) {
	es.RLock()
	defer es.RUnlock()

	// we need to copy the map as concurrent read and write operations are not allowed (and we unlock the mutex after returning)
	copiedMap := make(map[string]uint64)
	for key, value := range es.caseBinCounter {
		copiedMap[key] = value
	}

	return copiedMap, nil
}

func (es *EventStore) GetEventBinCount() (map[string]uint64, error) {
	es.RLock()
	defer es.RUnlock()

	// we need to copy the map as concurrent read and write operations are not allowed (and we unlock the mutex after returning)
	copiedMap := make(map[string]uint64)
	for key, value := range es.eventBinCounter {
		copiedMap[key] = value
	}

	return copiedMap, nil
}

func (es *EventStore) GetCaseCount() uint64 {
	return es.caseCounter
}

func (es *EventStore) GetEventCount() uint64 {
	return es.eventCounter
}

func (es *EventStore) Close() {
	es.log.Info("Shutting down stores.EventStore")
	defer es.log.Info("Closed stores.EventStore")
	es.Lock()
	defer es.Unlock()
	if err := es.flush(true); err != nil {
		es.log.Error(err)
	}
}

// flush flushes the current event buffer as a block to the indexed database
func (es *EventStore) flush(force bool) error {
	// commit last events
	lastEventsAsArray := make([]pipeline.Event, 0)
	for e := es.lastEventsBuffer.Front(); e != nil; e = e.Next() {
		lastEventsAsArray = append(lastEventsAsArray, e.Value.(pipeline.Event))
	}

	lastEventsBuffer, err := encoding.Gob.Marshal(lastEventsAsArray)
	if err != nil {
		return err
	}
	err = storage.DefaultStorage.Set(lastEventsKey, lastEventsBuffer)
	if err != nil {
		return err
	}

	// commit the event counter
	if err := storage.DefaultStorage.Set(eventCounterKey, storage.Uint64ToBytes(es.eventCounter)); err != nil {
		return err
	}

	// commit the binned event counter
	b, err := encoding.Gob.Marshal(&es.eventBinCounter)
	if err != nil {
		return err
	}

	if err = storage.DefaultStorage.Set(binEventCounterKey, b); err != nil {
		return err
	}

	// commit the case counter
	if err = storage.DefaultStorage.Set(caseCounterKey, storage.Uint64ToBytes(es.caseCounter)); err != nil {
		return err
	}

	// commit the bin counter
	b2, err := encoding.Gob.Marshal(&es.caseBinCounter)
	if err != nil {
		return err
	}

	if err = storage.DefaultStorage.Set(binCaseCounterKey, b2); err != nil {
		return err
	}

	return nil
}
