package transforms

import (
	"sync"

	"github.com/pbudner/argosminer/pipeline"
	"github.com/prometheus/client_golang/prometheus"
)

// this parser is primarily used for performance testing as it does not cost significant performance
type rawParser struct {
	pipeline.Component
	pipeline.Consumer
	pipeline.Publisher
	Event *pipeline.Event
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
		Event: &pipeline.Event{},
	}
}

func (rp *rawParser) Run(wg *sync.WaitGroup) {
	defer wg.Done()
	for input := range rp.Consumes {
		rp.Consumes <- true
		rp.Publish(input, true)
	}
}

func (rp *rawParser) Close() {
	rp.Publisher.Close()
}
