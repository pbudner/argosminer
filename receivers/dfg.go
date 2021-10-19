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

func init() {
	prometheus.MustRegister(receivedDfgEventsCounter)
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
	receivedNullEventsCounter.WithLabelValues(a.Id.String()).Inc()
	cleanedActivityName := cleanActivityName(event.ActivityName)
	caseInstance := event.ProcessInstanceId
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
		_, err = a.DirectlyFollowsStore.Increment(a.DirectlyFollowsStore.EncodeDirectlyFollowsRelation("", cleanedActivityName), timestamp)
		if err != nil {
			return err
		}
	} else {
		// 2. we have seen this case
		rawStart, err := a.CaseStore.Get(caseInstance)
		if err != nil {
			return err
		}
		start := rawStart.(string)
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
	a.ActivityStore = a.StoreGenerator(10)
	a.CaseStore = a.StoreGenerator(11)
	a.DirectlyFollowsStore = a.StoreGenerator(12)
	a.StartActivityStore = a.StoreGenerator(13)
}

func cleanActivityName(activityName string) string {
	return strings.TrimSpace(activityName)
}
