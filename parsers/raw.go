package parsers

import (
	"github.com/pbudner/argosminer/events"
	"github.com/prometheus/client_golang/prometheus"
)

// this parser is primarily used for performance testing as it does not cost significant performance
type rawParser struct {
	Event *events.Event
}

var rawSkippedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_parsers_raw",
	Name:      "skipped_events",
	Help:      "Total number of skipped events.",
})

func init() {
	prometheus.MustRegister(rawSkippedEvents)
}

func NewRawParser() rawParser {
	return rawParser{
		Event: &events.Event{},
	}
}

func (p rawParser) Parse(input string) (*events.Event, error) {
	return p.Event, nil
}

func (p rawParser) Close() {
	// nothing to do here
}
