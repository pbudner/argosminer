package sources

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/parsers"
	"github.com/pbudner/argosminer/processors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

type KafkaSourceConfig struct {
	Brokers            string           `yaml:"brokers"`
	Tls                bool             `yaml:"tls"`
	GroupID            string           `yaml:"group-id"`
	Topic              string           `yaml:"topic"`
	MinBytes           int              `yaml:"min-bytes"`
	MaxBytes           int              `yaml:"max-bytes"`
	CommitInterval     time.Duration    `yaml:"commit-interval"`
	Timeout            time.Duration    `yaml:"timeout"`
	StartFromBeginning bool             `yaml:"start-from-beginning"`
	SaslConfig         *KafkaSaslConfig `yaml:"sasl-config"`
}

type KafkaSaslConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type kafkaSource struct {
	Config    KafkaSourceConfig
	Reader    *kafka.Reader
	Parser    parsers.Parser
	Receivers []processors.StreamingProcessor
	log       *zap.SugaredLogger
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
		log:       zap.L().Sugar().With("service", "kafka-source"),
	}
}

func (s *kafkaSource) AddReceiver(receiver processors.StreamingProcessor) {
	s.Receivers = append(s.Receivers, receiver)
}

func (s *kafkaSource) Close() {
	s.Reader.Close()
}

func (s *kafkaSource) Run(ctx context.Context, wg *sync.WaitGroup) {
	s.log.Debug("Initializing kafka source..")
	defer wg.Done()
	dialer := &kafka.Dialer{
		Timeout:   s.Config.Timeout,
		DualStack: true,
	}

	if s.Config.Tls {
		dialer.TLS = &tls.Config{}
	}

	if s.Config.SaslConfig != nil {
		mechanism, err := scram.Mechanism(scram.SHA512, s.Config.SaslConfig.Username, s.Config.SaslConfig.Password)
		if err != nil {
			panic(err)
		}

		dialer.SASLMechanism = mechanism
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Dialer:         dialer,
		Brokers:        strings.Split(s.Config.Brokers, ","),
		GroupID:        s.Config.GroupID,
		Topic:          s.Config.Topic,
		MinBytes:       s.Config.MinBytes,
		MaxBytes:       s.Config.MaxBytes,
		CommitInterval: s.Config.CommitInterval,
	})

	brokerList := s.Config.Brokers

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				s.log.Info("Shutting down kafka source..")
			} else {
				s.log.Error("An unexpected error occurred during ReadMessage:", err)
			}

			break
		}

		receivedKafkaEvents.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).Inc()
		lastReceivedKafkaEvent.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).SetToCurrentTime()

		var event *events.Event
		var parseErr error
		event, parseErr = s.Parser.Parse(m.Value)

		/*
			for _, parser := range s.Parsers {
				event, parseErr = parser.Parse(m.Value)
				if parseErr == nil && event != nil {
					break
				}
			}*/

		if parseErr != nil {
			s.log.Error(err)
			receivedKafkaEventsWithError.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).Inc()
			continue
		}

		if event != nil {
			for _, receiver := range s.Receivers {
				if err := receiver.Append(event); err != nil {
					s.log.Error(err)
					receivedKafkaEventsWithError.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).Inc()
				}
			}
		}

		if err := r.CommitMessages(context.Background(), m); err != nil {
			s.log.Error("Failed to commit messages:", err)
		}
	}

	if err := r.Close(); err != nil {
		s.log.Error("Failed to close kafka source reader:", err)
	}

	s.log.Info("Closed Kafka source")
}
