package algorithms

import (
	"fmt"
	"strings"

	"github.com/pbudner/argosminer-collector/pkg/events"
	"github.com/pbudner/argosminer-collector/pkg/stores"
	log "github.com/sirupsen/logrus"
)

type dfgStreamingAlgorithm struct {
	StoreGenerator       stores.StoreGenerator
	CaseStore            stores.Store
	DirectlyFollowsStore stores.Store
	ActivityStore        stores.Store
	StartActivityStore   stores.Store
}

func NewDfgStreamingAlgorithm(storeGenerator stores.StoreGenerator) *dfgStreamingAlgorithm {
	algo := dfgStreamingAlgorithm{
		StoreGenerator: storeGenerator,
	}

	algo.initStores()
	return &algo
}

func (a *dfgStreamingAlgorithm) Append(event events.Event) error {
	cleanedActivityName := cleanActivityName(event.ActivityName)
	caseInstance := event.ProcessInstanceId
	// timestamp := event.Timestamp.Unix()
	log.Debugf("received activity %s with timestamp %s", event.ActivityName, event.Timestamp)

	// increment general activity counter
	_, err := a.ActivityStore.Increment(cleanedActivityName)
	if err != nil {
		return err
	}

	if !a.CaseStore.Contains(caseInstance) {
		// 1. we have not seen this case so far
		_, err = a.StartActivityStore.Increment(cleanedActivityName)
		if err != nil {
			return err
		}
		_, err = a.DirectlyFollowsStore.Increment(fmt.Sprintf("->%s", cleanedActivityName))
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
		relation := fmt.Sprintf("%s->%s", start, cleanedActivityName)
		_, err = a.DirectlyFollowsStore.Increment(relation)
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
