package stores

import (
	"math/rand"
	"sync"
	"time"

	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores/backends"
	"github.com/pbudner/argosminer/stores/ulid"
)

type EventStore struct {
	sync.Mutex
	store         backends.StoreBackend
	ulidGenerator ulid.MonotonicULIDGenerator
}

func NewEventStore(storeGenerator backends.StoreBackendGenerator) *EventStore {
	return &EventStore{
		store:         storeGenerator("event_store"),
		ulidGenerator: *ulid.NewMonotonicULIDGenerator(rand.New(rand.NewSource(4711))),
	}
}

func (es *EventStore) Append(rawEvent []byte) error {
	es.Lock()
	defer es.Unlock()
	t := time.Now().UTC()
	ulid, err := es.ulidGenerator.New(t)
	if err != nil {
		return err
	}

	binID, err := ulid.MarshalBinary()
	if err != nil {
		panic(err)
	}

	return es.store.Set(binID, rawEvent)
}

func (es *EventStore) Get(id []byte) (*events.Event, error) {
	es.Lock()
	defer es.Unlock()
	value, err := es.store.Get(id)
	if err != nil {
		return nil, err
	}

	var event events.Event
	err = event.Unmarshal(value)
	if err != nil {
		return nil, err
	}

	return &event, nil
}

func (es *EventStore) GetLast(count int) ([]events.Event, error) {
	es.Lock()
	defer es.Unlock()
	rawValues, err := es.store.GetLast(count)
	if err != nil {
		return nil, err
	}

	events := make([]events.Event, len(rawValues))
	for i, rawValue := range rawValues {
		err = events[i].Unmarshal(rawValue)
		if err != nil {
			return nil, err
		}
	}

	return events, nil
}

/*
// this operation is waaaaaaay too expensive
func (es *EventStore) Count() (uint64, error) {
	es.Lock()
	defer es.Unlock()
	count, err := es.store.TotalCount()
	if err != nil {
		return 0, err
	}

	return count, nil
}
*/
