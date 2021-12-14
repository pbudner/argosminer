package parsers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJsoninJsonParser(t *testing.T) {
	parser := NewJsonParser(JsonParserConfig{
		JsonPath:              "my_json",
		ActivityPath:          "activity",
		ProcessInstanceIdPath: "process_instance_id",
		TimestampPath:         "@timestamp",
		TimestampFormat:       "2006-01-02T15:04:05.000",
		TimestampTzIanakey:    "Europe/Berlin",
	})

	// JSON in JSON
	evt, err := parser.Parse([]byte("{\"my_json\": \"{\\\"activity\\\": \\\"my_activity\\\", \\\"process_instance_id\\\": \\\"my_instance\\\", \\\"@timestamp\\\": \\\"2021-11-22T12:13:14.000\\\", \\\"inner_attribute\\\": \\\"foo\\\"}\", \"outer_attribute\": \"bar\"}"))
	require.NoError(t, err)
	require.EqualValues(t, "my_instance", evt.ProcessInstanceId)
	require.EqualValues(t, "my_activity", evt.ActivityName)
	require.EqualValues(t, 2021, evt.Timestamp.Year())

	// normal JSON
	parser.config.JsonPath = ""
	evt2, err := parser.Parse([]byte("{\"activity\": \"my_activity\", \"process_instance_id\": \"my_instance\", \"@timestamp\": \"2021-11-22T12:13:14.000\", \"inner_attribute\": \"foo\"}"))
	require.NoError(t, err)
	require.EqualValues(t, "my_instance", evt2.ProcessInstanceId)
	require.EqualValues(t, "my_activity", evt2.ActivityName)
	require.EqualValues(t, 2021, evt2.Timestamp.Year())

	// invalid json
	_, err = parser.Parse([]byte("foobar"))
	require.Error(t, err)

	// not required fields
	_, err = parser.Parse([]byte("{\"foo\": \"bar\"}"))
	require.Error(t, err)
}
