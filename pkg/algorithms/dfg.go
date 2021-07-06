package algorithms

import (
	"github.com/pbudner/argosminer-collector/pkg/events"
	"github.com/pbudner/argosminer-collector/pkg/stores"
	log "github.com/sirupsen/logrus"
)

type dfgStreamingAlgorithm struct {
	Store stores.Store
}

func NewDfgStreamingAlgorithm(store stores.Store) dfgStreamingAlgorithm {
	algo := dfgStreamingAlgorithm{
		Store: store,
	}

	return algo
}

func (algo dfgStreamingAlgorithm) Append(event events.Event) error {
	val, err := algo.Store.Increment(event.ActivityName)
	if err != nil {
		return err
	}
	log.Infof("%s: %s [%d]", event.Timestamp, event.ActivityName, val)
	return nil
}

func (algo dfgStreamingAlgorithm) Close() {
	// do nothing
}
