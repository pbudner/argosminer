package processors

import (
	"github.com/google/uuid"
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type dfgStreamingAlgorithm struct {
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
	log = zap.L().Sugar()
}

func NewDfgStreamingAlgorithm(store *stores.SbarStore) *dfgStreamingAlgorithm {
	algo := dfgStreamingAlgorithm{
		Id:    uuid.New(),
		Store: store,
		log:   zap.L().Sugar().With("service", "dfg-streaming-algorithm"),
	}
	return &algo
}

func (a *dfgStreamingAlgorithm) Append(event *events.Event) error {
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
		// 1. we have not seen this case so far
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
