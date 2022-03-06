package transforms

import (
	"context"
	"sync"
	"testing"

	"github.com/pbudner/argosminer/pipeline"
	"github.com/stretchr/testify/require"
)

func TestJsoninJsonParser(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx := context.Background()

	parser := NewJsonParser(JsonParserConfig{
		JsonPath:           "my_json",
		ActivityPath:       "activity",
		CaseIdPath:         "case_id",
		TimestampPath:      "@timestamp",
		TimestampFormat:    "2006-01-02T15:04:05.000",
		TimestampTzIanakey: "Europe/Berlin",
	})
	parse := make(chan interface{})
	parser.Link(parse)
	receiver := parser.Subscribe()
	go parser.Run(wg, ctx)

	// JSON in JSON
	parse <- []byte("{\"my_json\": \"{\\\"activity\\\": \\\"my_activity\\\", \\\"case_id\\\": \\\"my_instance\\\", \\\"@timestamp\\\": \\\"2021-11-22T12:13:14.000\\\", \\\"inner_attribute\\\": \\\"foo\\\"}\", \"outer_attribute\": \"bar\"}")
	require.EqualValues(t, true, <-parse)
	evt := (<-receiver).(pipeline.Event)
	require.EqualValues(t, "my_instance", evt.CaseId)
	require.EqualValues(t, "my_activity", evt.ActivityName)
	require.EqualValues(t, 2021, evt.Timestamp.Year())

	// JSON in JSON + additional attributes
	parse <- []byte("{\"my_json\": \"{\\\"activity\\\": \\\"my_activity\\\", \\\"case_id\\\": \\\"my_instance\\\", \\\"@timestamp\\\": \\\"2021-11-22T12:13:14.000\\\", \\\"inner_attribute\\\": \\\"foo\\\"}\", \"outer_attribute\": \"bar\"}")
	require.EqualValues(t, true, <-parse)
	evt = (<-receiver).(pipeline.Event)
	require.EqualValues(t, "my_instance", evt.CaseId)
	require.EqualValues(t, "my_activity", evt.ActivityName)
	require.EqualValues(t, 2021, evt.Timestamp.Year())
	require.EqualValues(t, "foo", evt.AdditionalFields["inner_attribute"])
	t.Logf("Additional fields has length of %d", len(evt.AdditionalFields))

	// normal JSON
	parser.config.JsonPath = ""
	parse <- []byte("{\"activity\": \"my_activity\", \"case_id\": \"my_instance\", \"@timestamp\": \"2021-11-22T12:13:14.000\", \"inner_attribute\": \"foo\"}")
	require.EqualValues(t, true, <-parse)
	evt = (<-receiver).(pipeline.Event)
	require.EqualValues(t, "my_instance", evt.CaseId)
	require.EqualValues(t, "my_activity", evt.ActivityName)
	require.EqualValues(t, 2021, evt.Timestamp.Year())

	// invalid json
	parse <- []byte("foobar")
	require.EqualValues(t, false, <-parse)

	// not required fields
	parse <- []byte("{\"foo\": \"bar\"}")
	require.EqualValues(t, false, <-parse)

	parser.Close()
	wg.Wait()
}
