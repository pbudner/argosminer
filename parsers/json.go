package parsers

import (
	"fmt"

	"github.com/pbudner/argosminer/events"
	"github.com/pbudner/argosminer/parsers/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
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
	timestampParser *utils.TimestampParser
	log             *zap.SugaredLogger
}

type jsonConditionLiteral func(gjson.Result) (bool, error)

var (
	jsonSkippedEvents = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "argosminer_parsers_json",
		Name:      "skipped_events",
		Help:      "Total number of skipped events.",
	})
)

func init() {
	prometheus.MustRegister(jsonSkippedEvents)
}

func NewJsonParser(config JsonParserConfig) jsonParser {
	conditionFuncs := make([]jsonConditionLiteral, len(config.IgnoreWhen))
	for i, ignoreWhen := range config.IgnoreWhen {
		conditionFuncs[i] = func(json gjson.Result) (bool, error) {
			val := json.Get(ignoreWhen.Path).String()

			if ignoreWhen.Condition == "==" {
				return val == ignoreWhen.Value, nil
			}

			return val != ignoreWhen.Value, nil
		}
	}
	return jsonParser{
		log:             zap.L().Sugar().With("service", "json-parser"),
		config:          config,
		conditions:      conditionFuncs,
		timestampParser: utils.NewTimestampParser(config.TimestampFormat, config.TimestampTzIanakey),
	}
}

func (p jsonParser) Parse(input []byte) (nilEvent events.Event, err error) {
	// JSON in JSON
	if p.config.JsonPath != "" {
		result := gjson.GetBytes(input, p.config.JsonPath)
		input = []byte(result.Str)
	}

	parsedJson := gjson.ParseBytes(input)

	var lineShouldBeIgnored bool

	for _, condition := range p.conditions {
		lineShouldBeIgnored, err = condition(parsedJson)
		if err != nil {
			return
		}

		if lineShouldBeIgnored {
			jsonSkippedEvents.Inc()
			p.log.Debug("skipping a line as an ignore condition is fulfilled")
			return
		}
	}

	caseId := parsedJson.Get(p.config.CaseIdPath).String()
	activity := parsedJson.Get(p.config.ActivityPath).String()
	rawTimestamp := parsedJson.Get(p.config.TimestampPath).String()
	timestamp, err := p.timestampParser.Parse(rawTimestamp)
	if err != nil {
		return
	}

	if caseId == "" || activity == "" {
		return nilEvent, fmt.Errorf("could not create a new event as some required fields are empty: case_id=%s, activity=%s", caseId, activity)
	}

	additionalFields := make(map[string]string)
	parsedJson.ForEach(func(key, value gjson.Result) bool {
		additionalFields[key.String()] = value.String()
		return true
	})

	return events.NewEvent(caseId, activity, timestamp, additionalFields), nil
}

func (p jsonParser) Close() {
	// nothing to do here
}
