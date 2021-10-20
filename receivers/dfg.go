package receivers

import (
	"strings"

	"github.com/google/uuid"
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type dfgStreamingAlgorithm struct {
	Id                   uuid.UUID
	StoreGenerator       stores.StoreGenerator
	CaseStore            stores.Store
	DirectlyFollowsStore stores.Store
	ActivityStore        stores.Store
	StartActivityStore   stores.Store
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

func NewDfgStreamingAlgorithm(storeGenerator stores.StoreGenerator) *dfgStreamingAlgorithm {
	algo := dfgStreamingAlgorithm{
		Id:             uuid.New(),
		StoreGenerator: storeGenerator,
	}

	algo.initStores()
	return &algo
}

func (a *dfgStreamingAlgorithm) Append(event *events.Event) error {
	// update some Prometheus metrics
	lastReceivedDfgEvent.WithLabelValues(a.Id.String()).SetToCurrentTime()
	receivedDfgEventsCounter.WithLabelValues(a.Id.String()).Inc()
	lastReceviedDfgEventTime.WithLabelValues(a.Id.String()).Set(float64(event.Timestamp.Unix()))
	cleanedActivityName := []byte(cleanActivityName(string(event.ActivityName)))

	caseInstance := []byte(event.ProcessInstanceId)
	timestamp := event.Timestamp
	log.Debugf("received activity %s with timestamp %s", event.ActivityName, event.Timestamp)

	// increment general activity counter
	_, err := a.ActivityStore.Increment(a.ActivityStore.EncodeActivity(cleanedActivityName), timestamp)
	if err != nil {
		return err
	}

	if !a.CaseStore.Contains(caseInstance) {
		// 1. we have not seen this case so far
		_, err = a.StartActivityStore.Increment(a.StartActivityStore.EncodeActivity(cleanedActivityName), timestamp)
		if err != nil {
			return err
		}
		_, err = a.DirectlyFollowsStore.Increment(a.DirectlyFollowsStore.EncodeDirectlyFollowsRelation([]byte{}, cleanedActivityName), timestamp)
		if err != nil {
			return err
		}
	} else {
		// 2. we have seen this case
		rawStart, err := a.CaseStore.Get([]byte(caseInstance))
		if err != nil {
			return err
		}
		start := rawStart
		relation := a.DirectlyFollowsStore.EncodeDirectlyFollowsRelation(start, cleanedActivityName)
		_, err = a.DirectlyFollowsStore.Increment(relation, timestamp)
		if err != nil {
			return err
		}
		log.Debugf("incremented directly-follows relation %s", relation)
	}

	// always set the last seen activity for the current case to the current activity
	a.CaseStore.Set(caseInstance, cleanedActivityName)
	return nil
}

func (a *dfgStreamingAlgorithm) Close() {
	a.ActivityStore.Close()
	a.CaseStore.Close()
	a.DirectlyFollowsStore.Close()
	a.StartActivityStore.Close()
}

func (a *dfgStreamingAlgorithm) initStores() {
	a.ActivityStore = a.StoreGenerator("activities")
	a.CaseStore = a.StoreGenerator("cases")
	a.DirectlyFollowsStore = a.StoreGenerator("df-relations")
	a.StartActivityStore = a.StoreGenerator("start-activities")
}

func cleanActivityName(activityName string) string {
	return strings.TrimSpace(activityName)
}
