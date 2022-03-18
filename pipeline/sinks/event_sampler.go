package sinks

import (
	"context"
	"sync"
	"time"

	"github.com/pbudner/argosminer/pipeline"
	"github.com/pbudner/argosminer/stores"
)

var (
	eventSamplerSingletonOnce sync.Once
	eventSamplerSingleton     *eventSampler
)

type eventSampler struct {
	pipeline.Consumer
	ticker          *time.Ticker
	doneChannel     chan bool
	lastValue       uint64
	lastTimestamp   time.Time
	eventsPerSecond int
}

func init() {
	pipeline.RegisterComponent("sinks.event_sampler", nil, func(config interface{}) pipeline.Component {
		return GetEventSampler()
	})
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

func (a *eventSampler) Subscribe() <-chan interface{} {
	panic("A sink component must not be subscribed to")
}

func (jp *eventSampler) Run(wg *sync.WaitGroup, ctx context.Context) {
	// nothing to do here
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
			newValue := stores.GetEventStore().GetEventCount()
			if es.lastValue > 0 {
				elapsedTime := timeNow.Sub(es.lastTimestamp)
				es.eventsPerSecond = int(float64(newValue-es.lastValue) / elapsedTime.Seconds())
			}
			es.lastValue = newValue
			es.lastTimestamp = timeNow
		}
	}
}
