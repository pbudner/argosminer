package sinks

import (
	"sync"

	"github.com/google/uuid"
	"github.com/pbudner/argosminer/pipeline"
	"github.com/pbudner/argosminer/stores"
	"go.uber.org/zap"
)

// this receiver is primarily used for performance testing as it does not cost significant performance
type eventProcessor struct {
	pipeline.Consumer
	Id         uuid.UUID
	EventStore *stores.EventStore
	log        *zap.SugaredLogger
}

func NewEventProcessor(eventStore *stores.EventStore) *eventProcessor {
	receiver := &eventProcessor{
		Id:         uuid.New(),
		EventStore: eventStore,
		log:        zap.L().Sugar().With("service", "event-processor"),
	}

	receiver.log.Infof("Initialized new EventStore receiver with ID %s", receiver.Id)
	return receiver
}

func (ep *eventProcessor) Subscribe() chan interface{} {
	panic("A sink component must not be subscribed to")
}

func (ep *eventProcessor) Run(wg *sync.WaitGroup) {
	ep.log.Info("Starting pipeline.sinks.DFG")
	defer wg.Done()
	for input := range ep.Consumes {
		err := ep.EventStore.Append(input.(pipeline.Event))
		if err == nil {
			ep.Consumes <- true
		} else {
			ep.Consumes <- false
		}
	}
	ep.log.Info("Shutting down pipeline.sinks.DFG")
}

func (ep *eventProcessor) Close() {
	// nothing to do here
}
