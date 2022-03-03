package transforms

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pbudner/argosminer/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type CsvParserConfig struct {
	Delimiter          string `yaml:"delimiter"`
	ActivityColumn     uint   `yaml:"activity-column"`
	CaseIdColumn       uint   `yaml:"case-id-column"`
	TimestampColumn    uint   `yaml:"timestamp-column"`
	TimestampFormat    string `yaml:"timestamp-format"`      // https://golang.org/src/time/format.go
	TimestampTzIanakey string `yaml:"timestamp-tz-iana-key"` // https://golang.org/src/time/format.go
	IgnoreWhen         []struct {
		Column    uint   `yaml:"column"`
		Condition string `yaml:"condition"`
		Value     string `yaml:"value"`
	} `yaml:"ignore-when"`
}

type csvParser struct {
	pipeline.Consumer
	pipeline.Publisher
	config          CsvParserConfig
	conditions      []csvConditionLiteral
	timestampParser *TimestampParser
	log             *zap.SugaredLogger
}

type csvConditionLiteral func([]string) (bool, error)

var (
	csvSkippedEvents = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "argosminer_parsers_csv",
		Name:      "skipped_events",
		Help:      "Total number of skipped events.",
	})
)

func init() {
	prometheus.MustRegister(csvSkippedEvents)
	pipeline.RegisterComponent("transforms.csv_parser", CsvParserConfig{}, func(config interface{}) pipeline.Component {
		return NewCsvParser(config.(CsvParserConfig))
	})
}

func NewCsvParser(config CsvParserConfig) *csvParser {
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
	return &csvParser{
		log:             zap.L().Sugar().With("service", "csv-parser"),
		config:          config,
		conditions:      conditionFuncs,
		timestampParser: NewTimestampParser(config.TimestampFormat, config.TimestampTzIanakey),
	}
}

func (cp *csvParser) Run(wg *sync.WaitGroup, ctx context.Context) {
	cp.log.Info("Starting pipeline.transforms.CsvParser")
	defer wg.Done()
	defer cp.log.Info("Shutting down pipeline.transforms.CsvParser")
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-cp.Consumes:
			evt, err := cp.parse(input.([]byte))
			if err != nil {
				cp.Consumes <- true
				cp.Publish(evt, true)
			}
		}
	}
}

func (p *csvParser) parse(input []byte) (pipeline.Event, error) {
	event := pipeline.Event{}
	eventColumns := strings.Split(string(input), p.config.Delimiter)
	for _, condition := range p.conditions {
		lineShouldBeIgnored, err := condition(eventColumns)
		if err != nil {
			return event, err
		}

		if lineShouldBeIgnored {
			csvSkippedEvents.Inc()
			p.log.Debug("skipping a line as an ignore condition is fulfilled")
			return event, nil
		}
	}

	numOfColumnsInEvent := len(eventColumns)
	if p.config.CaseIdColumn >= uint(numOfColumnsInEvent) || p.config.ActivityColumn >= uint(numOfColumnsInEvent) || p.config.TimestampColumn >= uint(numOfColumnsInEvent) {
		return event, fmt.Errorf("the event does not contain all neccessary columns to parse it")
	}

	processInstanceId := strings.Trim(eventColumns[p.config.CaseIdColumn], " ")
	activityName := strings.Trim(eventColumns[p.config.ActivityColumn], " ")
	timestamp, err := p.timestampParser.Parse(eventColumns[p.config.TimestampColumn])
	if err != nil {
		return event, err
	}

	return pipeline.NewEvent(processInstanceId, activityName, timestamp.UTC(), nil), nil
}

func (cp *csvParser) Close() {
	cp.Publisher.Close()
}
