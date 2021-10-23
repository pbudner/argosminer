package events

import (
	"fmt"
	"time"
)

type Event struct {
	ProcessInstanceId string
	ActivityName      string
	Timestamp         time.Time
}

func NewEvent(processInstanceId string, activityName string, timestamp time.Time) Event {
	return Event{
		ProcessInstanceId: processInstanceId,
		ActivityName:      activityName,
		Timestamp:         timestamp,
	}
}

func (e *Event) String() string {
	return fmt.Sprintf("{\"timestamp\":\"%s\",\"activity_name\":\"%s\",\"process_instance_id\":\"%s\"}", e.Timestamp.Format("2006-01-02T15:04:05Z07:00"), e.ActivityName, e.ProcessInstanceId)
}

func (e *Event) Byte() []byte {
	return []byte(e.String())
}
