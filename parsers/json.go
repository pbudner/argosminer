package parsers

import (
	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/parsers/utils"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type JsonParserConfig struct {
	JsonPath           string `yaml:"json-path"` // refactor that
	ActivityPath       string `yaml:"activity-path"`
	CaseIdPath         string `yaml:"case-id-path"`
	TimestampPath      string `yaml:"timestamp-path"`
	TimestampFormat    string `yaml:"timestamp-format"`      // https://golang.org/src/time/format.go
	TimestampTzIanakey string `yaml:"timestamp-tz-iana-key"` // https://golang.org/src/time/format.go
	IgnoreWhen         []struct {
		Path      string `yaml:"path"`
		Condition string `yaml:"condition"`
		Value     string `yaml:"value"`
	} `yaml:"ignore-when"`
}

type jsonParser struct {
	Parser
	config          JsonParserConfig
	conditions      []jsonConditionLiteral
	timestampParser utils.TimestampParser
}

type jsonConditionLiteral func([]byte) (bool, error)

var jsonSkippedEvents = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "argosminer_parsers_json",
	Name:      "skipped_events",
	Help:      "Total number of skipped events.",
})

func init() {
	prometheus.MustRegister(jsonSkippedEvents)
}

func NewJsonParser(config JsonParserConfig) jsonParser {
	conditionFuncs := make([]jsonConditionLiteral, len(config.IgnoreWhen))
	for i, ignoreWhen := range config.IgnoreWhen {
		conditionFuncs[i] = func(json []byte) (bool, error) {
			val := gjson.GetBytes(json, ignoreWhen.Path).String()

			if ignoreWhen.Condition == "==" {
				return val == ignoreWhen.Value, nil
			}

			return val != ignoreWhen.Value, nil
		}
	}
	return jsonParser{
		config:          config,
		conditions:      conditionFuncs,
		timestampParser: utils.NewTimestampParser(config.TimestampFormat, config.TimestampTzIanakey),
	}
}

func (p jsonParser) Parse(input []byte) (*events.Event, error) {
	for _, condition := range p.conditions {
		lineShouldBeIgnored, err := condition(input)
		if err != nil {
			return nil, err
		}

		if lineShouldBeIgnored {
			jsonSkippedEvents.Inc()
			log.Debug("skipping a line as an ignore condition is fulfilled")
			return nil, nil
		}
	}

	// TODO: Sanity checks

	// JSON in JSON
	if p.config.JsonPath != "" {
		result := gjson.GetBytes(input, p.config.JsonPath)
		input = []byte(result.Str)
	}

	results := gjson.GetManyBytes(input, p.config.CaseIdPath, p.config.ActivityPath, p.config.TimestampPath)
	timestamp, err := p.timestampParser.Parse(results[2].Str)
	if err != nil {
		return nil, err
	}

	event := events.NewEvent(results[0].Str, results[1].Str, *timestamp)
	return &event, nil
}

func (p jsonParser) Close() {
	// nothing to do here
}
