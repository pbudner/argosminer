package kafka

type KafkaSourceConfig struct {
	Brokers  []string `yaml:"brokers"`
	GroupID  string   `yaml:"group_id"`
	Topic    string   `yaml:"topic"`
	MinBytes int      `yaml:"min_bytes"`
	MaxBytes int      `yaml:"max_bytes"`
}
