package events

import "time"

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
