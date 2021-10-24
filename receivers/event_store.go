package receivers

import (
	"github.com/google/uuid"
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores"
	"github.com/pbudner/argosminer/stores/utils"
	log "github.com/sirupsen/logrus"
)

// this receiver is primarily used for performance testing as it does not cost significant performance
type eventStoreReceiver struct {
	Id         uuid.UUID
	EventStore *stores.EventStore
	KvStore    *stores.KvStore
}

var eventStoreCounter = []byte("EventStore_Counter")

func NewEventStoreReceiver(eventStore *stores.EventStore, kvStore *stores.KvStore) *eventStoreReceiver {
	receiver := eventStoreReceiver{
		Id:         uuid.New(),
		EventStore: eventStore,
		KvStore:    kvStore,
	}
	log.Infof("Initialized new EventStore receiver with ID %s", receiver.Id)
	return &receiver
}

func (a *eventStoreReceiver) Append(event *events.Event) error {
	b, err := event.Marshal()
	if err != nil {
		return err
	}

	a.KvStore.Increment(eventStoreCounter)
	return a.EventStore.Append(b)
}

func (a *eventStoreReceiver) Count() (uint64, error) {
	c, err := a.KvStore.Get(eventStoreCounter)
	if err != nil {
		return 0, err
	}

	return utils.BytesToUint64(c), err
}

func (a *eventStoreReceiver) Close() {
	// nothing to do here
}
