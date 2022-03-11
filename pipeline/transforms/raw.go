package transforms

import (
	"context"
	"sync"

	"github.com/pbudner/argosminer/pipeline"
	"github.com/prometheus/client_golang/prometheus"
)

// this parser is primarily used for performance testing as it does not cost significant performance
type rawParser struct {
	pipeline.Consumer
	pipeline.Publisher
}

var rawSkippedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_parsers_raw",
	Name:      "skipped_events",
	Help:      "Total number of skipped events.",
})

func init() {
	prometheus.MustRegister(rawSkippedEvents)
	pipeline.RegisterComponent("transforms.raw_parser", nil, func(config interface{}) pipeline.Component {
		return NewRawParser()
	})
}

func NewRawParser() *rawParser {
	return &rawParser{}
}

func (rp *rawParser) Run(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-rp.Consumes:
			rp.Publish(input)
		}
	}
}

func (rp *rawParser) Close() {
	rp.Publisher.Close()
}
