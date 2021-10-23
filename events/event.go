package events

import (
	"encoding/json"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type Event struct {
	ProcessInstanceId string    `json:"process_instance_id"`
	ActivityName      string    `json:"activity_name"`
	Timestamp         time.Time `json:"timestamp"`
}

func NewEvent(processInstanceId string, activityName string, timestamp time.Time) Event {
	return Event{
		ProcessInstanceId: processInstanceId,
		ActivityName:      activityName,
		Timestamp:         timestamp,
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
