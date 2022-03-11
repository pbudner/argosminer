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

var (
	dfgSingletonOnce         sync.Once
	dfgSingleton             *dfgStreamingAlgorithm
	receivedDfgEventsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "argosminer_receivers_dfg",
		Name:      "events_total",
		Help:      "Total number of received events.",
	}, []string{"guid"})

	lastReceivedDfgEvent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "argosminer_receivers_dfg",
		Name:      "last_received_event",
		Help:      "Last received event for this receiver.",
	}, []string{"guid"})

	lastReceviedDfgEventTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "argosminer_receivers_dfg",
		Name:      "last_received_eventtime",
		Help:      "Last received event time for this receiver.",
	}, []string{"guid"})
)

func init() {
	prometheus.MustRegister(receivedDfgEventsCounter, lastReceivedDfgEvent, lastReceviedDfgEventTime)
	pipeline.RegisterComponent("sinks.dfg", nil, func(config interface{}) pipeline.Component {
		dfgSingletonOnce.Do(func() {
			dfgSingleton = NewDfgStreamingAlgorithm(stores.GetSbarStore())
		})
		return dfgSingleton
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
			evt, ok := input.(pipeline.Event)
			if !ok {
				a.log.Errorw("Did not receive a pipeline.Event", "input", input)
				a.Consumes <- false
				continue
			}

			err := a.append(evt)
			a.Consumes <- err == nil
		}
	}

}

func (a *dfgStreamingAlgorithm) append(event pipeline.Event) error {
	// update metrics
	lastReceivedDfgEvent.WithLabelValues(a.Id.String()).SetToCurrentTime()
	receivedDfgEventsCounter.WithLabelValues(a.Id.String()).Inc()
	lastReceviedDfgEventTime.WithLabelValues(a.Id.String()).Set(float64(event.Timestamp.Unix()))
	a.log.Debugf("received activity %s with timestamp %s", event.ActivityName, event.Timestamp)
	// increment general activity counter
	err := a.Store.RecordActivity(event.ActivityName, event.Timestamp)
	if err != nil {
		return err
	}
	lastActivityForCase, err := a.Store.GetLastActivityForCase(event.CaseId)
	if err != nil {
		return err
	}
	if lastActivityForCase == "" {
		// 1. we have not seen this case thus far
		a.Store.RecordStartActivity(event.ActivityName)
		err = a.Store.RecordDirectlyFollowsRelation("", event.ActivityName, event.Timestamp)
		if err != nil {
			return err
		}
	} else {
		// 2. we have seen this case
		err = a.Store.RecordDirectlyFollowsRelation(lastActivityForCase, event.ActivityName, event.Timestamp)
		if err != nil {
			return err
		}
	}
	// always set the last seen activity for the current case to the current activity
	err = a.Store.RecordActivityForCase(event.ActivityName, event.CaseId, event.Timestamp)
	return err
}

func (a *dfgStreamingAlgorithm) Close() {
	a.Store.Close()
}
