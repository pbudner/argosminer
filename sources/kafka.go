package sources

import (
	"context"
	"errors"
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
	Brokers  []string      `yaml:"brokers"`
	GroupID  string        `yaml:"group-id"`
	Topic    string        `yaml:"topic"`
	MinBytes int           `yaml:"min-bytes"`
	MaxBytes int           `yaml:"max-bytes"`
	Timeout  time.Duration `yaml:"timeout"`
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

func NewKafkaSource(config KafkaSourceConfig, parser parsers.Parser) kafkaSource {
	return kafkaSource{
		Config:    config,
		Parser:    parser,
		Receivers: []algorithms.StreamingAlgorithm{},
	}
}

func (s *kafkaSource) AddReceiver(receiver algorithms.StreamingAlgorithm) {
	s.Receivers = append(s.Receivers, receiver)
}

func (s *kafkaSource) Close() {
	s.Reader.Close()
}

func (s *kafkaSource) Run(ctx context.Context, wg *sync.WaitGroup) {
	log.Debug("Initializing kafka source..")
	defer wg.Done()
	dialer := &kafka.Dialer{
		Timeout:   s.Config.Timeout,
		DualStack: true,
		// SASLMechanism: mechanism,
		// TODO: Add TLS * SASL/SCRAM auth options & timeout for connection dialing via Dialer struct
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Dialer:   dialer,
		Brokers:  s.Config.Brokers,
		GroupID:  s.Config.GroupID,
		Topic:    s.Config.Topic,
		MinBytes: s.Config.MinBytes,
		MaxBytes: s.Config.MaxBytes,
	})

	for {
		log.Debug("Waiting for kafka message..")
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info("Shutting down kafka source..")
			} else {
				log.Error("An unexpected error occurred during ReadMessage:", err)
			}
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

		if err := r.CommitMessages(ctx, m); err != nil {
			log.Error("Failed to commit messages:", err)
		}
	}

	if err := r.Close(); err != nil {
		log.Error("Failed to close kafka source reader:", err)
	}

	log.Info("Successfully closed kafka source")
}
