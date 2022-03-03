package sinks

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/pbudner/argosminer/pipeline"
	"github.com/pbudner/argosminer/stores"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type dfgStreamingAlgorithm struct {
	pipeline.Consumer
	Id    uuid.UUID
	Store *stores.SbarStore
	log   *zap.SugaredLogger
}

var receivedDfgEventsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_receivers_dfg",
	Name:      "events_total",
	Help:      "Total number of received events.",
}, []string{"guid"})

var lastReceivedDfgEvent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_receivers_dfg",
	Name:      "last_received_event",
	Help:      "Last received event for this receiver.",
}, []string{"guid"})

var lastReceviedDfgEventTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_receivers_dfg",
	Name:      "last_received_eventtime",
	Help:      "Last received event time for this receiver.",
}, []string{"guid"})

func init() {
	prometheus.MustRegister(receivedDfgEventsCounter, lastReceivedDfgEvent, lastReceviedDfgEventTime)
	pipeline.RegisterComponent("sinks.dfg", nil, func(config interface{}) pipeline.Component {
		return NewDfgStreamingAlgorithm(stores.GetSbarStore())
	})
}

func NewDfgStreamingAlgorithm(store *stores.SbarStore) *dfgStreamingAlgorithm {
	algo := dfgStreamingAlgorithm{
		Id:    uuid.New(),
		Store: store,
		log:   zap.L().Sugar().With("service", "dfg-streaming-algorithm"),
	}
	return &algo
}

func (a *dfgStreamingAlgorithm) Subscribe() chan interface{} {
	panic("A sink component must not be subscribed to")
}

func (a *dfgStreamingAlgorithm) Run(wg *sync.WaitGroup, ctx context.Context) {
	a.log.Info("Starting pipeline.sinks.DFG")
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			a.log.Info("Shutting down pipeline.sinks.DFG")
			return
		case input := <-a.Consumes:
			err := a.append(input.(pipeline.Event))
			if err == nil {
				a.Consumes <- true
			} else {
				a.Consumes <- false
			}
		}
	}

}

func (a *dfgStreamingAlgorithm) append(event pipeline.Event) error {
	// update metrics
	lastReceivedDfgEvent.WithLabelValues(a.Id.String()).SetToCurrentTime()
	receivedDfgEventsCounter.WithLabelValues(a.Id.String()).Inc()
	lastReceviedDfgEventTime.WithLabelValues(a.Id.String()).Set(float64(event.Timestamp.Unix()))
	activityName := event.ActivityName
	caseInstance := event.ProcessInstanceId
	timestamp := event.Timestamp
	a.log.Debugf("received activity %s with timestamp %s", event.ActivityName, event.Timestamp)
	// increment general activity counter
	err := a.Store.RecordActivity(activityName, timestamp)
	if err != nil {
		return err
	}
	lastActivityForCase, err := a.Store.GetLastActivityForCase(caseInstance)
	if err != nil {
		return err
	}
	if lastActivityForCase == "" {
		// 1. we have not seen this case thus far
		a.Store.RecordStartActivity(activityName)
		err = a.Store.RecordDirectlyFollowsRelation("", activityName, timestamp)
		if err != nil {
			return err
		}
	} else {
		// 2. we have seen this case
		err = a.Store.RecordDirectlyFollowsRelation(lastActivityForCase, activityName, timestamp)
		if err != nil {
			return err
		}
	}
	// always set the last seen activity for the current case to the current activity
	err = a.Store.RecordActivityForCase(activityName, caseInstance, timestamp)
	return err
}

func (a *dfgStreamingAlgorithm) Close() {
	a.Store.Close()
}
