package transforms

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/pbudner/argosminer/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

type JsonParserConfigs struct {
	Parsers []JsonParserConfig `yaml:"parsers"`
}

type JsonParserConfig struct {
	JsonPath           string `yaml:"json-path"` // refactor that
	ActivityPath       string `yaml:"activity-path"`
	CaseIdPath         string `yaml:"case-id-path"`
	TimestampPath      string `yaml:"timestamp-path"`
	TimestampFormat    string `yaml:"timestamp-format"`      // https://golang.org/src/time/format.go
	TimestampTzIanakey string `yaml:"timestamp-tz-iana-key"` // https://golang.org/src/time/format.go
	ActivityPrefix     string `yaml:"activity-prefix"`
	IgnoreWhen         []struct {
		Path      string `yaml:"path"`
		Condition string `yaml:"condition"`
		Value     string `yaml:"value"`
	} `yaml:"ignore-when"`
}

type jsonParsers struct {
	pipeline.Publisher
	pipeline.Consumer
	config  JsonParserConfigs
	parsers []*jsonParser
	log     *zap.SugaredLogger
}

type jsonParser struct {
	pipeline.Publisher
	pipeline.Consumer
	config          JsonParserConfig
	conditions      []jsonConditionLiteral
	timestampParser *TimestampParser
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
	pipeline.RegisterComponent("transforms.json_parser", JsonParserConfigs{}, func(config interface{}) pipeline.Component {
		return NewJsonParsers(config.(JsonParserConfigs))
	})
}

func NewJsonParsers(config JsonParserConfigs) *jsonParsers {
	parsers := make([]*jsonParser, len(config.Parsers))
	for i := range config.Parsers {
		parsers[i] = newJsonParser(config.Parsers[i])
	}
	return &jsonParsers{
		log:     zap.L().Sugar().With("service", "json-parser"),
		config:  config,
		parsers: parsers,
	}
}

func (jp *jsonParsers) Run(wg *sync.WaitGroup, ctx context.Context) {
	jp.log.Info("Starting pipeline.transforms.JsonParser")
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			jp.log.Info("Shutting down pipeline.transforms.JsonParser")
			return
		case input := <-jp.Consumes:
			b, ok := input.([]byte)
			if !ok {
				jp.log.Errorw("Expected []byte input", "input", input)
				continue
			}

			parsed := false
			var parseError error
			for _, p := range jp.parsers {
				evt, err := p.parse(b)
				if evt.IsParsed && err == nil {
					parsed = true
					jp.Publish(evt)
					break
				}
				if err != nil {
					parseError = err
				}
			}

			if !parsed {
				if parseError != nil {
					jp.log.Errorw("could not parse an event", "last_error", parseError)
				} else {
					jp.log.Infow("could not parse an event (without error)", string(b))
				}
			}
		}
	}
}

func (jp *jsonParsers) Close() {
	jp.Publisher.Close()
}

func newJsonParser(config JsonParserConfig) *jsonParser {
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
	return &jsonParser{
		log:             zap.L().Sugar().With("service", "json-parser"),
		config:          config,
		conditions:      conditionFuncs,
		timestampParser: NewTimestampParser(config.TimestampFormat, config.TimestampTzIanakey),
	}
}

func (jp *jsonParser) parse(input []byte) (nilEvent pipeline.Event, err error) {
	// JSON in JSON
	if jp.config.JsonPath != "" {
		result := gjson.GetBytes(input, jp.config.JsonPath)
		input = []byte(result.Str)
		if len(input) == 0 {
			return nilEvent, errors.New("could not find JsonPath")
		}
	}

	parsedJson := gjson.ParseBytes(input)

	var lineShouldBeIgnored bool

	for _, condition := range jp.conditions {
		lineShouldBeIgnored, err = condition(parsedJson)
		if err != nil {
			return
		}

		if lineShouldBeIgnored {
			jsonSkippedEvents.Inc()
			jp.log.Debug("skipping a line as an ignore condition is fulfilled")
			return
		}
	}

	caseId := parsedJson.Get(jp.config.CaseIdPath).String()
	activity := parsedJson.Get(jp.config.ActivityPath).String()
	rawTimestamp := parsedJson.Get(jp.config.TimestampPath).String()

	if caseId == "" || activity == "" || rawTimestamp == "" {
		return nilEvent, fmt.Errorf("could not parse all required fields: case_id=%s, activity=%s, timestmap=%s", caseId, activity, rawTimestamp)
	}

	timestamp, err := jp.timestampParser.Parse(rawTimestamp)
	if err != nil {
		return
	}

	// prefix activites if wanted
	if jp.config.ActivityPrefix != "" {
		activity = jp.config.ActivityPrefix + activity
	}

	additionalFields := make(map[string]string)
	parsedJson.ForEach(func(key, value gjson.Result) bool {
		additionalFields[key.String()] = value.String()
		return true
	})

	return pipeline.NewEvent(caseId, activity, timestamp, additionalFields), nil
}
