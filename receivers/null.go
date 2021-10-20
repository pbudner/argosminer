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

var lastReceivedNullEvent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_receivers_null",
	Name:      "last_received_event",
	Help:      "Last received event for this receiver.",
}, []string{"guid"})

var lastReceviedNullEventTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_receivers_null",
	Name:      "last_received_eventtime",
	Help:      "Last received event time for this receiver.",
}, []string{"guid"})

func init() {
	prometheus.MustRegister(receivedNullEventsCounter, lastReceivedNullEvent, lastReceviedNullEventTime)
}

func NewDevNullReceiver(storeGenerator stores.StoreGenerator) *devNullReceiver {
	algo := devNullReceiver{
		Id: uuid.New(),
	}
	log.Infof("Initialized new dev/null receiver with ID %s", algo.Id)
	return &algo
}

func (a *devNullReceiver) Append(event *events.Event) error {
	lastReceivedNullEvent.WithLabelValues(a.Id.String()).SetToCurrentTime()
	receivedNullEventsCounter.WithLabelValues(a.Id.String()).Inc()
	lastReceviedNullEventTime.WithLabelValues(a.Id.String()).Set(float64(event.Timestamp.Unix()))
	return nil
}

func (a *devNullReceiver) Close() {
	// nothing to do here
}
