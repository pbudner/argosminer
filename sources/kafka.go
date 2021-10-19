package sources

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pbudner/argosminer/algorithms"
	"github.com/pbudner/argosminer/parsers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type KafkaSourceConfig struct {
	Brokers  []string `yaml:"brokers"`
	GroupID  string   `yaml:"group-id"`
	Topic    string   `yaml:"topic"`
	MinBytes int      `yaml:"min-bytes"`
	MaxBytes int      `yaml:"max-bytes"`
}

type kafkaSource struct {
	Config    KafkaSourceConfig
	Reader    *kafka.Reader
	Parser    parsers.Parser
	Receivers []algorithms.StreamingAlgorithm
}

var receivedKafkaEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_source_kafka",
	Name:      "received_events",
	Help:      "Total number of received events.",
})

var receivedKafkaEventsWithError = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_source_kafka",
	Name:      "received_events_error",
	Help:      "Total number of received events that produced an error.",
})

func init() {
	prometheus.MustRegister(receivedKafkaEvents)
	prometheus.MustRegister(receivedKafkaEventsWithError)
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
			log.Error("An unexpected error occurred during ReadMessage:", err)
			break
		}

		event, err := s.Parser.Parse(string(m.Value))
		if err != nil {
			log.Error(err)
			receivedKafkaEventsWithError.Inc()
			continue
		}
		fmt.Println(event)
		time.Sleep(1 * time.Second)

		if event != nil {
			for _, receiver := range s.Receivers {
				err := receiver.Append(*event)
				if err != nil {
					log.Error(err)
					receivedKafkaEventsWithError.Inc()
				}
			}
		}
	}

	if err := r.Close(); err != nil {
		log.Error("Failed to close kafka source reader:", err)
	}

	log.Info("Closed kafka source")
}
