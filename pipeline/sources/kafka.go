package sources

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/pbudner/argosminer/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	goKafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

type KafkaConfig struct {
	Brokers            string        `yaml:"brokers"`
	Tls                bool          `yaml:"tls"`
	GroupID            string        `yaml:"group-id"`
	Topic              string        `yaml:"topic"`
	MinBytes           int           `yaml:"min-bytes"`
	MaxBytes           int           `yaml:"max-bytes"`
	CommitInterval     time.Duration `yaml:"commit-interval"`
	Timeout            time.Duration `yaml:"timeout"`
	StartFromBeginning bool          `yaml:"start-from-beginning"`
	SaslConfig         *struct {
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"sasl-config"`
}

type kafka struct {
	pipeline.Consumer
	pipeline.Publisher
	Config KafkaConfig
	Reader *goKafka.Reader
	log    *zap.SugaredLogger
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
	pipeline.RegisterComponent("sources.kafka", KafkaConfig{}, func(config interface{}) pipeline.Component {
		return NewKafka(config.(KafkaConfig))
	})
}

func NewKafka(config KafkaConfig) *kafka {
	return &kafka{
		Config: config,
		log:    zap.L().Sugar().With("component", "sources.kafka"),
	}
}

func (s *kafka) Close() {
	s.Reader.Close()
	s.Publisher.Close()
}

func (s *kafka) Link(parent chan interface{}) {
	panic("A source component must not be linked to a parent pipeline component")
}

func (s *kafka) Run(wg *sync.WaitGroup, ctx context.Context) {
	s.log.Debug("Initializing kafka source..")
	defer wg.Done()

	dialer := &goKafka.Dialer{
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

	r := goKafka.NewReader(goKafka.ReaderConfig{
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

		s.Publish(m.Value, false) // we only want to send an input to one working parser

		if err := r.CommitMessages(context.Background(), m); err != nil {
			s.log.Error("Failed to commit messages:", err)
		}
	}

	if err := r.Close(); err != nil {
		s.log.Error("Failed to close kafka source reader:", err)
	}

	s.log.Info("Closed Kafka source")
}
