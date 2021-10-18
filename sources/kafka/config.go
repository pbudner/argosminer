package kafka

type KafkaSourceConfig struct {
	Brokers  []string `yaml:"brokers"`
	GroupID  string   `yaml:"group-id"`
	Topic    string   `yaml:"topic"`
	MinBytes int      `yaml:"min-bytes"`
	MaxBytes int      `yaml:"max-bytes"`
}
