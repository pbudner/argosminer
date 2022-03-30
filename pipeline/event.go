package pipeline

import (
	"time"
)

type Event struct {
	CaseId           string            `json:"case_id"`
	ActivityName     string            `json:"activity_name"`
	Timestamp        time.Time         `json:"timestamp"`
	AdditionalFields map[string]string `json:"additional_fields"`
}

func NewEvent(caseId string, activityName string, timestamp time.Time, additionalFields map[string]string) Event {
	return Event{
		CaseId:           caseId,
		ActivityName:     activityName,
		Timestamp:        timestamp,
		AdditionalFields: additionalFields,
	}
}
