package utils

import (
	"sync"
	"time"

	"github.com/pbudner/argosminer/stores"
)

var (
	eventSamplerSingletonOnce sync.Once
	eventSamplerSingleton     *EventSampler
)

type EventSampler struct {
	ticker          *time.Ticker
	doneChannel     chan bool
	lastValue       uint64
	lastTimestamp   time.Time
	eventsPerSecond int
}

func GetEventSampler() *EventSampler {
	eventSamplerSingletonOnce.Do(func() {
		eventSamplerSingleton = &EventSampler{
			ticker:          time.NewTicker(1000 * time.Millisecond),
			lastValue:       0,
			eventsPerSecond: 0,
			doneChannel:     make(chan bool),
		}

		go eventSamplerSingleton.tick()
	})
	return eventSamplerSingleton
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
			newValue := stores.GetEventStore().GetCount()
			if es.lastValue > 0 {
				elapsedTime := timeNow.Sub(es.lastTimestamp)
				es.eventsPerSecond = int(float64(newValue-es.lastValue) / elapsedTime.Seconds())
			}
			es.lastValue = newValue
			es.lastTimestamp = timeNow
		}
	}
}
