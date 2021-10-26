package processors

import (
	"github.com/google/uuid"
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores"
	log "github.com/sirupsen/logrus"
)

// this receiver is primarily used for performance testing as it does not cost significant performance
type eventProcessor struct {
	Id         uuid.UUID
	EventStore *stores.EventStore
	KvStore    *stores.KvStore
}

var eventStoreCounter = []byte("EventStoreCounter")

func NewEventProcessor(eventStore *stores.EventStore, kvStore *stores.KvStore) *eventProcessor {
	receiver := &eventProcessor{
		Id:         uuid.New(),
		EventStore: eventStore,
		KvStore:    kvStore,
	}
	log.Infof("Initialized new EventStore receiver with ID %s", receiver.Id)
	return receiver
}

func (a *eventProcessor) Append(event *events.Event) error {
	_, err := a.KvStore.Increment(eventStoreCounter)
	if err != nil {
		return err
	}

	return a.EventStore.Append(event)
}

func (a *eventProcessor) Close() {
	// nothing to do here
}
