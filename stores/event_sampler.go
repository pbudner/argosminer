package stores

import (
	"sync"
	"time"
)

var (
	eventSamplerSingletonOnce sync.Once
	eventSamplerSingleton     *eventSampler
)

type eventSampler struct {
	ticker          *time.Ticker
	doneChannel     chan bool
	lastValue       uint64
	lastTimestamp   time.Time
	eventsPerSecond int
}

func GetEventSampler() *eventSampler {
	eventSamplerSingletonOnce.Do(func() {
		eventSamplerSingleton = &eventSampler{
			ticker:          time.NewTicker(1000 * time.Millisecond),
			lastValue:       0,
			eventsPerSecond: 0,
			doneChannel:     make(chan bool),
		}

		go eventSamplerSingleton.tick()
	})
	return eventSamplerSingleton
}

func (es *eventSampler) GetSample() int {
	return es.eventsPerSecond
}

func (es *eventSampler) Close() {
	close(es.doneChannel)
	es.ticker.Stop()
}

func (es *eventSampler) tick() {
	for {
		select {
		case <-es.doneChannel:
			return
		case <-es.ticker.C:
			timeNow := time.Now()
			newValue := GetEventStore().GetEventCount()
			if es.lastValue > 0 {
				elapsedTime := timeNow.Sub(es.lastTimestamp)
				es.eventsPerSecond = int(float64(newValue-es.lastValue) / elapsedTime.Seconds())
			}
			es.lastValue = newValue
			es.lastTimestamp = timeNow
		}
	}
}
