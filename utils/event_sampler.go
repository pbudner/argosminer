package utils

import (
	"time"

	"github.com/pbudner/argosminer/stores"
)

type EventSampler struct {
	ticker          *time.Ticker
	doneChannel     chan bool
	store           *stores.EventStore
	lastValue       uint64
	lastTimestamp   time.Time
	eventsPerSecond int
}

func NewEventSampler(es *stores.EventStore) *EventSampler {
	result := &EventSampler{
		ticker:          time.NewTicker(1000 * time.Millisecond),
		store:           es,
		lastValue:       0,
		eventsPerSecond: 0,
		doneChannel:     make(chan bool),
	}

	go result.tick()
	return result
}

func (es *EventSampler) GetSample() int {
	return es.eventsPerSecond
}

func (es *EventSampler) Close() {
	close(es.doneChannel)
	es.ticker.Stop()
}

func (es *EventSampler) tick() {
	for {
		select {
		case <-es.doneChannel:
			return
		case <-es.ticker.C:
			timeNow := time.Now()
			newValue := es.store.GetCount()
			if es.lastValue > 0 {
				elapsedTime := timeNow.Sub(es.lastTimestamp)
				es.eventsPerSecond = int(float64(newValue-es.lastValue) / elapsedTime.Seconds())
			}
			es.lastValue = newValue
			es.lastTimestamp = timeNow
		}
	}
}
