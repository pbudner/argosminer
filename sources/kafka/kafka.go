package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/pbudner/argosminer-collector/algorithms"
	"github.com/pbudner/argosminer-collector/parsers"
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

func NewKafkaSource(config KafkaSourceConfig, parser parsers.Parser, receivers []algorithms.StreamingAlgorithm) kafkaSource {
	return kafkaSource{
		Config:    config,
		Parser:    parser,
		Receivers: receivers,
	}
}

func (s *kafkaSource) Close() {
	s.Reader.Close()
}

func (s *kafkaSource) Run(ctx context.Context, wg *sync.WaitGroup) {
	log.Debug("Initializing kafka source..")
	defer wg.Done()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  s.Config.Brokers,
		GroupID:  s.Config.GroupID,
		Topic:    s.Config.Topic,
		MinBytes: s.Config.MinBytes,
		MaxBytes: s.Config.MaxBytes,
	}) // TODO: Add TLS * SASL/SCRAM auth options

	for {
		m, err := r.ReadMessage(ctx) // TODO: Handle commits on our own
		if err != nil {
			break
		}

		fmt.Println(string(m.Value))
		break
	}

	if err := r.Close(); err != nil {
		log.Error("Failed to close kafka source reader:", err)
	}

	log.Info("Shutting kafka source..")
}
