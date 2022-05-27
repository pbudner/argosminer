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
	log    *zap.SugaredLogger
}

var receivedKafkaEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_sources_kafka",
	Name:      "events_total",
	Help:      "Total number of received events.",
}, []string{"broker", "topic", "group_id"})

var kafkaErrorsOnReadMessage = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "argosminer_sources_kafka",
	Name:      "readmessage_errors_total",
	Help:      "Total number of errors that ReadMessage produced.",
}, []string{"broker", "topic", "group_id"})

var lastReceivedKafkaEvent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: "argosminer_sources_kafka",
	Name:      "last_received_event",
	Help:      "Last received event for this source.",
}, []string{"broker", "topic", "group_id"})

func init() {
	prometheus.MustRegister(receivedKafkaEvents, kafkaErrorsOnReadMessage, lastReceivedKafkaEvent)
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

func (s *kafka) Link(parent <-chan interface{}) {
	panic("A source component must not be linked to a parent pipeline component")
}

func (s *kafka) Run(wg *sync.WaitGroup, ctx context.Context) {
	s.log.Info("Starting pipeline.sources.Kafka")
	defer wg.Done()
	defer s.log.Info("Closed pipeline.sources.Kafka")

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
	lostConnectionErrorOccurred := false

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			} else {
				kafkaErrorsOnReadMessage.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).Inc()
				if strings.HasSuffix(err.Error(), "connection refused") || strings.HasSuffix(err.Error(), "i/o timeout") {
					// log error message only once every second
					if !lostConnectionErrorOccurred {
						s.log.Error("Lost connection to Kafka. Retrying in 15 seconds.")
						lostConnectionErrorOccurred = true
					}

					time.Sleep(15 * time.Second)
				} else {
					s.log.Errorw("An unexpected error occurred during FetchMessage.", "error", err)
				}
				continue
			}
		}

		lostConnectionErrorOccurred = false
		receivedKafkaEvents.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).Inc()
		lastReceivedKafkaEvent.WithLabelValues(brokerList, s.Config.Topic, s.Config.GroupID).SetToCurrentTime()
		s.Publish(m.Value)
		if err := r.CommitMessages(context.Background(), m); err != nil {
			s.log.Errorw("Failed to commit message to Kafka.", "error", err)
		}
	}

	if err := r.Close(); err != nil {
		s.log.Error("Failed to close kafka source reader:", err)
	}
}
