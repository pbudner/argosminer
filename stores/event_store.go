package stores

import (
	"math/rand"
	"sync"
	"time"

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

func (es *EventStore) Append(rawEvente []byte) error {
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

	return es.store.Set(binID, rawEvente)
}
