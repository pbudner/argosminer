package receivers

import (
	"github.com/google/uuid"
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type dfgStreamingAlgorithm struct {
	Id    uuid.UUID
	Store *stores.SbarStore
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
}

func NewDfgStreamingAlgorithm(store *stores.SbarStore) *dfgStreamingAlgorithm {
	algo := dfgStreamingAlgorithm{
		Id:    uuid.New(),
		Store: store,
	}
	return &algo
}

func (a *dfgStreamingAlgorithm) Append(event *events.Event) error {
	// update some Prometheus metrics
	lastReceivedDfgEvent.WithLabelValues(a.Id.String()).SetToCurrentTime()
	receivedDfgEventsCounter.WithLabelValues(a.Id.String()).Inc()
	lastReceviedDfgEventTime.WithLabelValues(a.Id.String()).Set(float64(event.Timestamp.Unix()))
	activity := []byte(event.ActivityName)
	caseInstance := []byte(event.ProcessInstanceId)
	timestamp := event.Timestamp
	log.Debugf("received activity %s with timestamp %s", event.ActivityName, event.Timestamp)

	// increment general activity counter
	err := a.Store.RecordActivity(activity, timestamp)
	if err != nil {
		return err
	}

	lastEventForCase, err := a.Store.GetLastActivityForCase(caseInstance)
	if err != nil {
		return err
	}
	if lastEventForCase == nil {
		// 1. we have not seen this case so far
		err = a.Store.RecordStartActivity(activity)
		if err != nil {
			return err
		}
		err = a.Store.RecordDirectlyFollowsRelation([]byte{0x00}, activity, timestamp)
		if err != nil {
			return err
		}
	} else {
		// 2. we have seen this case
		err = a.Store.RecordDirectlyFollowsRelation(lastEventForCase, activity, timestamp)
		if err != nil {
			return err
		}
	}

	// always set the last seen activity for the current case to the current activity
	err = a.Store.RecordActivityForCase(activity, caseInstance, timestamp)
	return err
}

func (a *dfgStreamingAlgorithm) Close() {
	a.Store.Close()
}
