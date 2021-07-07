package parsers

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pbudner/argosminer-collector/pkg/algorithms"
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

var receivedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_filesource",
	Name:      "received_events",
	Help:      "Total number of received events.",
})

var receivedEventsWithError = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_filesource",
	Name:      "received_events_error",
	Help:      "Total number of received events that produced an error.",
})

var skippedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_csv_parser",
	Name:      "skipped_events",
	Help:      "Total number of skipped events.",
})

func init() {
	prometheus.MustRegister(receivedEvents)
	prometheus.MustRegister(receivedEventsWithError)
	prometheus.MustRegister(skippedEvents)
}

func NewCsvParser(config config.CsvParser) csvParser {
	conditionFuncs := make([]conditionLiteral, len(config.IgnoreWhen))
	for i, ignoreWhen := range config.IgnoreWhen {
		conditionFuncs[i] = func(eventColumns []string) (bool, error) {
			if int(ignoreWhen.Column) >= len(eventColumns) {
				return false, fmt.Errorf("the parsed event has no column with index %d (len %d)", ignoreWhen.Column, len(eventColumns))
			}

			if ignoreWhen.Condition == "==" {
				return eventColumns[ignoreWhen.Column] == ignoreWhen.Value, nil
			}

			return eventColumns[ignoreWhen.Column] != ignoreWhen.Value, nil
		}
	}
	return csvParser{
		config:     config,
		conditions: conditionFuncs,
	}
}

func (p csvParser) Parse(reader io.Reader, receivers []algorithms.StreamingAlgorithm) error {

	r := csv.NewReader(reader)
	r.Comma = rune(p.config.Delimiter[0])

	for {
		// read a line
		eventColumns, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error(err)
			continue
		}

		receivedEvents.Inc()

		// check whether a skipping condition is fulfilled
		for _, condition := range p.conditions {
			lineShouldBeIgnored, err := condition(eventColumns)
			if err != nil {
				log.Error(err)
				continue
			}

			if lineShouldBeIgnored {
				skippedEvents.Inc()
				log.Debug("skipping a line as an ignore condition is triggered")
				continue
			}
		}

		// validate entry
		numOfColumnsInEvent := len(eventColumns)
		if p.config.ProcessInstanceColumn >= uint(numOfColumnsInEvent) || p.config.ActivityColumn >= uint(numOfColumnsInEvent) || p.config.TimestampColumn >= uint(numOfColumnsInEvent) {
			log.Errorf("the event does not contain all neccessary columns to parse it")
			continue
		}

		// retrieve column data
		processInstanceId := strings.Trim(eventColumns[p.config.ProcessInstanceColumn], " ")
		activityName := strings.Trim(eventColumns[p.config.ActivityColumn], " ")
		rawTimestamp := eventColumns[p.config.TimestampColumn]
		rawTimestamp = strings.Replace(rawTimestamp, ",", ".", -1) //TODO Remove this is a hotfix with Go 1.17: https://github.com/golang/go/commit/f02a26be

		// parse timestamp
		var timestamp time.Time
		if p.config.TimestampTzIanakey != "" {
			tz, err := time.LoadLocation(p.config.TimestampTzIanakey)
			if err != nil {
				log.Error(err)
				continue
			}

			timestamp, err = time.ParseInLocation(p.config.TimestampFormat, rawTimestamp, tz)
			if err != nil {
				log.Error(err)
				continue
			}
		} else {
			timestamp, err = time.Parse(p.config.TimestampFormat, rawTimestamp)
			if err != nil {
				log.Error(err)
				continue
			}
		}

		// broadcast new event
		event := events.NewEvent(processInstanceId, activityName, timestamp.UTC())
		for _, receiver := range receivers {
			err := receiver.Append(event)
			if err != nil {
				log.Error(err)
				receivedEventsWithError.Inc()
			}
		}
	}

	return nil
}

func (p csvParser) Close() {
	// do nothing
}
