package sources

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/pbudner/argosminer/parsers"
	"github.com/pbudner/argosminer/processors"
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
	CommitInterval     time.Duration `yaml:"commit-interval"`
	Timeout            time.Duration `yaml:"timeout"`
	StartFromBeginning bool          `yaml:"start-from-beginning"`
}

type kafkaSource struct {
	Config    KafkaSourceConfig
	Reader    *kafka.Reader
	Parser    parsers.Parser
	Receivers []processors.StreamingProcessor
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

var lastReceivedKafkaEvent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_sources_kafka",
	Name:      "last_received_event",
	Help:      "Last received event for this source.",
}, []string{"broker", "topic", "group_id"})

func init() {
	prometheus.MustRegister(receivedKafkaEvents, receivedKafkaEventsWithError, lastReceivedKafkaEvent)
}

func NewKafkaSource(config KafkaSourceConfig, parser parsers.Parser) kafkaSource {
	return kafkaSource{
		Config:    config,
		Parser:    parser,
		Receivers: []processors.StreamingProcessor{},
	}
}

func (s *kafkaSource) AddReceiver(receiver processors.StreamingProcessor) {
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
		Dialer:         dialer,
		Brokers:        s.Config.Brokers,
		GroupID:        s.Config.GroupID,
		Topic:          s.Config.Topic,
		MinBytes:       s.Config.MinBytes,
		MaxBytes:       s.Config.MaxBytes,
		CommitInterval: s.Config.CommitInterval,
	})

	brokerList := strings.Join(s.Config.Brokers, ",")

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

		receivedKafkaEvents.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).Inc()
		lastReceivedKafkaEvent.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).SetToCurrentTime()

		event, err := s.Parser.Parse(m.Value)
		if err != nil {
			log.Error(err)
			receivedKafkaEventsWithError.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).Inc()
			continue
		}

		if event != nil {
			for _, receiver := range s.Receivers {
				if err := receiver.Append(event); err != nil {
					log.Error(err)
					receivedKafkaEventsWithError.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).Inc()
				}
			}
		}

		if err := r.CommitMessages(context.Background(), m); err != nil {
			log.Error("Failed to commit messages:", err)
		}
	}

	if err := r.Close(); err != nil {
		log.Error("Failed to close kafka source reader:", err)
	}

	log.Info("Closed Kafka source")
}
