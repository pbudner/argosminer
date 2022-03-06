package pipeline

import (
	"encoding/json"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type Event struct {
	IsParsed         bool              `json:"-"`
	CaseId           string            `json:"case_id"`
	ActivityName     string            `json:"activity_name"`
	Timestamp        time.Time         `json:"timestamp"`
	AdditionalFields map[string]string `json:"additional_fields"`
}

func NewEvent(caseId string, activityName string, timestamp time.Time, additionalFields map[string]string) Event {
	return Event{
		IsParsed:         true,
		CaseId:           caseId,
		ActivityName:     activityName,
		Timestamp:        timestamp,
		AdditionalFields: additionalFields,
	}
}

func (e *Event) String() (string, error) {
	b, err := json.Marshal(&e)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func (e *Event) Marshal() ([]byte, error) {
	return msgpack.Marshal(&e)
}

func (e *Event) Unmarshal(bytes []byte) error {
	return msgpack.Unmarshal(bytes, &e)
}
