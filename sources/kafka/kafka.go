package kafka

import (
	"context"
	"sync"

	"github.com/pbudner/argosminer-collector/pkg/algorithms"
	"github.com/pbudner/argosminer-collector/pkg/parsers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type kafkaSource struct {
	Config    KafkaSourceConfig
	Reader    *kafka.Reader
	Parser    parsers.Parser
	Receivers []algorithms.StreamingAlgorithm
}

var receivedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_source_kafka",
	Name:      "received_events",
	Help:      "Total number of received events.",
})

var receivedEventsWithError = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_source_kafka",
	Name:      "received_events_error",
	Help:      "Total number of received events that produced an error.",
})

func init() {
	prometheus.MustRegister(receivedEvents)
	prometheus.MustRegister(receivedEventsWithError)
}

func NewFileSource(config KafkaSourceConfig, parser parsers.Parser, receivers []algorithms.StreamingAlgorithm) kafkaSource {
	return kafkaSource{
		Config:    config,
		Parser:    parser,
		Receivers: receivers,
	}
}

func (s *kafkaSource) Close() {
	s.Reader.Close()
}

func (fs *kafkaSource) Run(ctx context.Context, wg *sync.WaitGroup) {
	log.Debug("Initializing kafka source..")
	defer wg.Done()
	log.Info("Shutting kafka source..")
}
