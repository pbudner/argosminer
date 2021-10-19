package receivers

import (
	"github.com/google/uuid"
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/stores"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// this receiver is primarily used for performance testing as it does not cost significant performance
type devNullReceiver struct {
	Id uuid.UUID
}

var receivedNullEventsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_receivers_null",
	Name:      "events_total",
	Help:      "Total number of received events.",
}, []string{"guid"})

func init() {
	prometheus.MustRegister(receivedNullEventsCounter)
}

func NewDevNullReceiver(storeGenerator stores.StoreGenerator) *devNullReceiver {
	algo := devNullReceiver{
		Id: uuid.New(),
	}
	log.Infof("Initialized new dev/null receiver with ID %s", algo.Id)
	return &algo
}

func (a *devNullReceiver) Append(event *events.Event) error {
	receivedNullEventsCounter.WithLabelValues(a.Id.String()).Inc()
	return nil
}

func (a *devNullReceiver) Close() {
	// nothing to do here
}
