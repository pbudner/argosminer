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
	return fmt.Sprintf("{\"timestamp\":\"%s\",\"activity_name\":\"%s\",\"process_instance_id\":\"%s\"}", e.Timestamp, e.ActivityName, e.ProcessInstanceId)
}
