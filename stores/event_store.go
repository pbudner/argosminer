package stores

import (
	"math/rand"
	"sync"
	"time"

	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores/backends"
	"github.com/pbudner/argosminer/stores/ulid"
	"github.com/pbudner/argosminer/stores/utils"
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

func (es *EventStore) Append(event *events.Event) error {
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

	es.store.Increment(append([]byte{0x00}, []byte(event.Timestamp.Format("2006010215"))...))
	binEvent, err := event.Marshal()
	if err != nil {
		return err
	}

	return es.store.Set(binID, binEvent)
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

// this operation is waaaaaaay too expensive
func (es *EventStore) CountByDay() (map[string]uint64, error) {
	es.Lock()
	defer es.Unlock()
	result := make(map[string]uint64)
	values, err := es.store.Find([]byte{0x00})
	if err != nil {
		return nil, err
	}
	date := []byte{2, 0, 2, 1, '-', 1, 0, '-', 2, 5}
	for _, v := range values {
		date[0] = v.Key[1]
		date[1] = v.Key[2]
		date[2] = v.Key[3]
		date[3] = v.Key[4]
		date[5] = v.Key[5]
		date[6] = v.Key[6]
		date[8] = v.Key[7]
		date[9] = v.Key[8]
		result[string(date)] += utils.BytesToUint64(v.Value)
	}

	return result, nil
}
