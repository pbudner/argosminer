package null

import (
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores"
	"github.com/prometheus/client_golang/prometheus"
)

type devNullAlgorithm struct {
}

var receivedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_algorithms_null",
	Name:      "received_events",
	Help:      "Total number of received events.",
})

func init() {
	prometheus.MustRegister(receivedEvents)
}

func NewDevNullAlgorithm(storeGenerator stores.StoreGenerator) *devNullAlgorithm {
	algo := devNullAlgorithm{}
	return &algo
}

func (a *devNullAlgorithm) Append(event events.Event) error {
	// nothing to do here
	return nil
}

func (a *devNullAlgorithm) Close() {
	// nothing to do here
}
