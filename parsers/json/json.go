package json

import (
	"strings"
	"time"

	"github.com/pbudner/argosminer/events"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type jsonParser struct {
	config     JsonParserConfig
	conditions []conditionLiteral
}

type conditionLiteral func(string) (bool, error)

var skippedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_parser_json",
	Name:      "skipped_events",
	Help:      "Total number of skipped events.",
})

func init() {
	prometheus.MustRegister(skippedEvents)
}

func NewJsonParser(config JsonParserConfig) jsonParser {
	conditionFuncs := make([]conditionLiteral, len(config.IgnoreWhen))
	for i, ignoreWhen := range config.IgnoreWhen {
		conditionFuncs[i] = func(json string) (bool, error) {
			val := gjson.Get(json, ignoreWhen.Path).String()

			if ignoreWhen.Condition == "==" {
				return val == ignoreWhen.Value, nil
			}

			return val != ignoreWhen.Value, nil
		}
	}
	return jsonParser{
		config:     config,
		conditions: conditionFuncs,
	}
}

func (p jsonParser) Parse(input string) (*events.Event, error) {
	for _, condition := range p.conditions {
		lineShouldBeIgnored, err := condition(input)
		if err != nil {
			return nil, err
		}

		if lineShouldBeIgnored {
			skippedEvents.Inc()
			log.Debug("skipping a line as an ignore condition is fulfilled")
			return nil, nil
		}
	}

	// TODO: Sanity checks

	processInstanceId := strings.Trim(gjson.Get(input, p.config.ProcessInstancePath).String(), " ")
	activityName := strings.Trim(gjson.Get(input, p.config.ActivityPath).String(), " ")
	rawTimestamp := strings.Trim(gjson.Get(input, p.config.TimestampPath).String(), " ")
	var timestamp time.Time
	var err error

	// TODO: Move timestamp parsing to a shared base
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

func (p jsonParser) Close() {
	// nothing to do here
}
