package sinks

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/pbudner/argosminer/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// this receiver is primarily used for performance testing as it does not cost significant performance
type devNullProcessor struct {
	pipeline.Consumer
	Id uuid.UUID
}

var log *zap.SugaredLogger

var receivedNullEventsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_receivers_null",
	Name:      "events_total",
	Help:      "Total number of received events.",
}, []string{"guid"})

var lastReceivedNullEvent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_receivers_null",
	Name:      "last_received_event",
	Help:      "Last received event for this receiver.",
}, []string{"guid"})

var lastReceviedNullEventTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_receivers_null",
	Name:      "last_received_eventtime",
	Help:      "Last received event time for this receiver.",
}, []string{"guid"})

func init() {
	log = zap.L().Sugar()
	prometheus.MustRegister(receivedNullEventsCounter, lastReceivedNullEvent, lastReceviedNullEventTime)
	pipeline.RegisterComponent("sinks.nil", nil, func(config interface{}) pipeline.Component {
		return NewDevNullProcessor()
	})
}

func NewDevNullProcessor() *devNullProcessor {
	algo := devNullProcessor{
		Id: uuid.New(),
	}
	log.Infof("Initialized new dev/null receiver with ID %s", algo.Id)
	return &algo
}

func (dp *devNullProcessor) Subscribe() <-chan interface{} {
	panic("A sink component must not be subscribed to")
}

func (dp *devNullProcessor) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-dp.Consumes:
			lastReceivedNullEvent.WithLabelValues(dp.Id.String()).SetToCurrentTime()
			receivedNullEventsCounter.WithLabelValues(dp.Id.String()).Inc()
			lastReceviedNullEventTime.WithLabelValues(dp.Id.String()).Set(float64((input.(pipeline.Event)).Timestamp.Unix()))
		}
	}
}
