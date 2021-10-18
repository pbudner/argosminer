package parsers

import (
	"fmt"
	"strings"
	"time"

	"github.com/pbudner/argosminer-collector/pkg/config"
	"github.com/pbudner/argosminer-collector/pkg/events"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type csvParser struct {
	config     config.CsvParser
	conditions []conditionLiteral
}

type conditionLiteral func([]string) (bool, error)

var skippedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_csv_parser",
	Name:      "skipped_events",
	Help:      "Total number of skipped events.",
})

func init() {
	prometheus.MustRegister(skippedEvents)
}

func NewCsvParser(config config.CsvParser) csvParser {
	conditionFuncs := make([]conditionLiteral, len(config.IgnoreWhen))
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
			skippedEvents.Inc()
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
