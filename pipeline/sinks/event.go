package sinks

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/pbudner/argosminer/pipeline"
	"github.com/pbudner/argosminer/stores"
	"go.uber.org/zap"
)

var (
	eventProcessorSingletonOnce sync.Once
	eventProcessorSingleton     *eventProcessor
)

// this receiver is primarily used for performance testing as it does not cost significant performance
type eventProcessor struct {
	pipeline.Consumer
	Id         uuid.UUID
	EventStore *stores.EventStore
	log        *zap.SugaredLogger
}

func init() {
	pipeline.RegisterComponent("sinks.event", nil, func(config interface{}) pipeline.Component {
		eventProcessorSingletonOnce.Do(func() {
			eventProcessorSingleton = NewEventProcessor(stores.GetEventStore())
		})
		return eventProcessorSingleton
	})
}

func NewEventProcessor(eventStore *stores.EventStore) *eventProcessor {
	receiver := &eventProcessor{
		Id:         uuid.New(),
		EventStore: eventStore,
		log:        zap.L().Sugar().With("service", "sinks.event"),
	}
	receiver.log.Debugf("Initialized new EventStore receiver with ID %s", receiver.Id)
	return receiver
}

func (ep *eventProcessor) Subscribe() <-chan interface{} {
	panic("A sink component must not be subscribed to")
}

func (ep *eventProcessor) Run(wg *sync.WaitGroup, ctx context.Context) {
	ep.log.Info("Starting pipeline.sinks.Event")
	defer wg.Done()
	defer ep.log.Info("Closed pipeline.sinks.Event")
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-ep.Consumes:
			evt, ok := input.(pipeline.Event)
			if !ok {
				ep.log.Errorw("Did not receive a pipeline.Event", "input", input)
				continue
			}

			err := ep.EventStore.Append(evt)
			if err != nil {
				ep.log.Error(err)
				continue
			}
		}
	}
}
