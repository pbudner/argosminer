package json

type JsonParserConfig struct {
	ActivityPath        string `yaml:"activity-path"`
	ProcessInstancePath string `yaml:"process-instance-path"`
	TimestampPath       string `yaml:"timestamp-path"`
	TimestampFormat     string `yaml:"timestamp-format"`      // https://golang.org/src/time/format.go
	TimestampTzIanakey  string `yaml:"timestamp-tz-iana-key"` // https://golang.org/src/time/format.go
	IgnoreWhen          []struct {
		Path      string `yaml:"path"`
		Condition string `yaml:"condition"`
		Value     string `yaml:"value"`
	} `yaml:"ignore-when"`
}
