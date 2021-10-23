package receivers

import (
	"github.com/google/uuid"
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores"
	log "github.com/sirupsen/logrus"
)

// this receiver is primarily used for performance testing as it does not cost significant performance
type eventStoreReceiver struct {
	Id         uuid.UUID
	EventStore *stores.EventStore
}

func NewEventStoreReceiver(eventStore *stores.EventStore) *eventStoreReceiver {
	receiver := eventStoreReceiver{
		Id:         uuid.New(),
		EventStore: eventStore,
	}
	log.Infof("Initialized new EventStore receiver with ID %s", receiver.Id)
	return &receiver
}

func (a *eventStoreReceiver) Append(event *events.Event) error {
	b, err := event.Marshal()
	if err != nil {
		return err
	}

	return a.EventStore.Append(b)
}

func (a *eventStoreReceiver) Close() {
	// nothing to do here
}
