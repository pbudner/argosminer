package sources

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/pbudner/argosminer/parsers"
	"github.com/pbudner/argosminer/receivers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type KafkaSourceConfig struct {
	Brokers            []string      `yaml:"brokers"`
	GroupID            string        `yaml:"group-id"`
	Topic              string        `yaml:"topic"`
	MinBytes           int           `yaml:"min-bytes"`
	MaxBytes           int           `yaml:"max-bytes"`
	Timeout            time.Duration `yaml:"timeout"`
	StartFromBeginning bool          `yaml:"start_from_beginning"`
}

type kafkaSource struct {
	Config    KafkaSourceConfig
	Reader    *kafka.Reader
	Parser    parsers.Parser
	Receivers []receivers.StreamingReceiver
}

var receivedKafkaEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_sources_kafka",
	Name:      "events_total",
	Help:      "Total number of received events.",
}, []string{"broker", "topic", "group_id"})

var receivedKafkaEventsWithError = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_sources_kafka",
	Name:      "errors_total",
	Help:      "Total number of received events that produced an error.",
}, []string{"broker", "topic", "group_id"})

func init() {
	prometheus.MustRegister(receivedKafkaEvents)
	prometheus.MustRegister(receivedKafkaEventsWithError)
}

func NewKafkaSource(config KafkaSourceConfig, parser parsers.Parser) kafkaSource {
	return kafkaSource{
		Config:    config,
		Parser:    parser,
		Receivers: []receivers.StreamingReceiver{},
	}
}

func (s *kafkaSource) AddReceiver(receiver receivers.StreamingReceiver) {
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
		m, err := r.ReadMessage(ctx)
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
			receivedKafkaEventsWithError.WithLabelValues(s.Config.Brokers[0], s.Config.Topic, s.Config.GroupID).Inc()
			continue
		}

		if event != nil {
			for _, receiver := range s.Receivers {
				err := receiver.Append(event)
				if err != nil {
					log.Error(err)
					receivedKafkaEventsWithError.WithLabelValues(s.Config.Brokers[0], s.Config.Topic, s.Config.GroupID).Inc()
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
