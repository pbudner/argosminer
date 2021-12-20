package processors

import (
	"github.com/google/uuid"
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores"
	"go.uber.org/zap"
)

// this receiver is primarily used for performance testing as it does not cost significant performance
type eventProcessor struct {
	Id         uuid.UUID
	EventStore *stores.EventStore
}

func NewEventProcessor(eventStore *stores.EventStore) *eventProcessor {
	receiver := &eventProcessor{
		Id:         uuid.New(),
		EventStore: eventStore,
	}

	log := zap.L().Sugar().With("service", "event-processor")
	log.Infof("Initialized new EventStore receiver with ID %s", receiver.Id)
	return receiver
}

func (a *eventProcessor) Append(event *events.Event) error {
	return a.EventStore.Append(event)
}

func (a *eventProcessor) Close() {
	// nothing to do here
}
