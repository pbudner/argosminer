package parsers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJsoninJsonParser(t *testing.T) {
	parser := NewJsonParser(JsonParserConfig{
		JsonPath:           "my_json",
		ActivityPath:       "activity",
		CaseIdPath:         "case_id",
		TimestampPath:      "@timestamp",
		TimestampFormat:    "2006-01-02T15:04:05.000",
		TimestampTzIanakey: "Europe/Berlin",
	})

	// JSON in JSON
	evt, err := parser.Parse([]byte("{\"my_json\": \"{\\\"activity\\\": \\\"my_activity\\\", \\\"case_id\\\": \\\"my_instance\\\", \\\"@timestamp\\\": \\\"2021-11-22T12:13:14.000\\\", \\\"inner_attribute\\\": \\\"foo\\\"}\", \"outer_attribute\": \"bar\"}"))
	require.NoError(t, err)
	require.EqualValues(t, "my_instance", evt.ProcessInstanceId)
	require.EqualValues(t, "my_activity", evt.ActivityName)
	require.EqualValues(t, 2021, evt.Timestamp.Year())

	// JSON in JSON + additional attributes
	evt3, err := parser.Parse([]byte("{\"my_json\": \"{\\\"activity\\\": \\\"my_activity\\\", \\\"case_id\\\": \\\"my_instance\\\", \\\"@timestamp\\\": \\\"2021-11-22T12:13:14.000\\\", \\\"inner_attribute\\\": \\\"foo\\\"}\", \"outer_attribute\": \"bar\"}"))
	require.NoError(t, err)
	require.EqualValues(t, "my_instance", evt3.ProcessInstanceId)
	require.EqualValues(t, "my_activity", evt3.ActivityName)
	require.EqualValues(t, 2021, evt3.Timestamp.Year())
	require.EqualValues(t, "foo", evt3.AdditionalFields["inner_attribute"])
	t.Logf("Additional fields has length of %d", len(evt3.AdditionalFields))

	// normal JSON
	parser.config.JsonPath = ""
	evt2, err := parser.Parse([]byte("{\"activity\": \"my_activity\", \"case_id\": \"my_instance\", \"@timestamp\": \"2021-11-22T12:13:14.000\", \"inner_attribute\": \"foo\"}"))
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
