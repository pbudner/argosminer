package csv

type CsvParserConfig struct {
	Delimiter             string `yaml:"delimiter"`
	ActivityColumn        uint   `yaml:"activity-column"`
	ProcessInstanceColumn uint   `yaml:"process-instance-column"`
	TimestampColumn       uint   `yaml:"timestamp-column"`
	TimestampFormat       string `yaml:"timestamp-format"`      // https://golang.org/src/time/format.go
	TimestampTzIanakey    string `yaml:"timestamp-tz-iana-key"` // https://golang.org/src/time/format.go
	IgnoreWhen            []struct {
		Column    uint   `yaml:"column"`
		Condition string `yaml:"condition"`
		Value     string `yaml:"value"`
	} `yaml:"ignore-when"`
}
