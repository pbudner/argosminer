package parsers

import (
	"fmt"
	"strings"
	"time"

	"github.com/pbudner/argosminer/events"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

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

type csvParser struct {
	config     CsvParserConfig
	conditions []csvConditionLiteral
}

type csvConditionLiteral func([]string) (bool, error)

var csvSkippedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_parsers_csv",
	Name:      "skipped_events",
	Help:      "Total number of skipped events.",
})

func init() {
	prometheus.MustRegister(csvSkippedEvents)
}

func NewCsvParser(config CsvParserConfig) csvParser {
	conditionFuncs := make([]csvConditionLiteral, len(config.IgnoreWhen))
	for i, ignoreWhen := range config.IgnoreWhen {
		conditionFuncs[i] = func(eventColumns []string) (bool, error) {
			if int(ignoreWhen.Column) >= len(eventColumns) {
				return false, fmt.Errorf("the parsed event has no column with index %d (len %d)", ignoreWhen.Column, len(eventColumns))
			}

			val := eventColumns[ignoreWhen.Column]

			if ignoreWhen.Condition == "==" {
				return val == ignoreWhen.Value, nil
			}

			return val != ignoreWhen.Value, nil
		}
	}
	return csvParser{
		config:     config,
		conditions: conditionFuncs,
	}
}

func (p csvParser) Parse(input string) (*events.Event, error) {
	eventColumns := strings.Split(input, p.config.Delimiter)
	for _, condition := range p.conditions {
		lineShouldBeIgnored, err := condition(eventColumns)
		if err != nil {
			return nil, err
		}

		if lineShouldBeIgnored {
			csvSkippedEvents.Inc()
			log.Debug("skipping a line as an ignore condition is fulfilled")
			return nil, nil
		}
	}

	numOfColumnsInEvent := len(eventColumns)
	if p.config.ProcessInstanceColumn >= uint(numOfColumnsInEvent) || p.config.ActivityColumn >= uint(numOfColumnsInEvent) || p.config.TimestampColumn >= uint(numOfColumnsInEvent) {
		return nil, fmt.Errorf("the event does not contain all neccessary columns to parse it")
	}

	processInstanceId := strings.Trim(eventColumns[p.config.ProcessInstanceColumn], " ")
	activityName := strings.Trim(eventColumns[p.config.ActivityColumn], " ")
	rawTimestamp := eventColumns[p.config.TimestampColumn]
	var timestamp time.Time
	var err error
	if p.config.TimestampTzIanakey != "" {
		tz, err := time.LoadLocation(p.config.TimestampTzIanakey)
		if err != nil {
			return nil, err
		}

		timestamp, err = time.ParseInLocation(p.config.TimestampFormat, rawTimestamp, tz)
		if err != nil {
			return nil, err
		}
	} else {
		timestamp, err = time.Parse(p.config.TimestampFormat, rawTimestamp)
		if err != nil {
			return nil, err
		}
	}

	event := events.NewEvent(processInstanceId, activityName, timestamp.UTC())
	return &event, nil
}

func (p csvParser) Close() {
	// do nothing
}
