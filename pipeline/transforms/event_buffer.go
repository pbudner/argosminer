package transforms

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/pbudner/argosminer/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type EventBufferConfig struct {
	MaxEvents             int           `yaml:"max-events"`
	MaxAge                time.Duration `yaml:"max-age"`
	FlushInterval         time.Duration `yaml:"flush-interval"`
	IgnoreEventsOlderThan time.Duration `yaml:"ignore-events-older-than"`
}

type eventBuffer struct {
	pipeline.Publisher
	pipeline.Consumer
	config EventBufferConfig
	buffer timeOrderedEventBuffer
	log    *zap.SugaredLogger
}

var (
	eventBufferSingletonOnce sync.Once
	eventBufferSingleton     *eventBuffer
	eventBufferCurrentItems  = prometheus.NewGauge(prometheus.GaugeOpts{
		Subsystem: "argosminer_event_buffer",
		Name:      "current_events",
		Help:      "Number of current events in buffer.",
	})

	eventBufferIgnoredItems = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "argosminer_event_buffer",
		Name:      "ignored_items_count",
		Help:      "Total count of of outaged events.",
	})
)

func init() {
	prometheus.MustRegister(eventBufferCurrentItems, eventBufferIgnoredItems)
	pipeline.RegisterComponent("transforms.event_buffer", EventBufferConfig{}, func(config interface{}) pipeline.Component {
		eventBufferSingletonOnce.Do(func() {
			eventBufferSingleton = NewEventBuffer(config.(EventBufferConfig))
		})

		return eventBufferSingleton
	})
}

func NewEventBuffer(config EventBufferConfig) *eventBuffer {
	return &eventBuffer{
		log:    zap.L().Sugar().With("service", "event-buffer"),
		config: config,
	}
}

func (eb *eventBuffer) Run(wg *sync.WaitGroup, ctx context.Context) {
	eb.log.Info("Starting pipeline.transforms.EventBuffer")
	defer wg.Done()
	defer eb.log.Info("Shutting down pipeline.transforms.EventBuffer")
	ticker := time.NewTicker(eb.config.FlushInterval)
	heap.Init(&eb.buffer)
	var counter uint64 = 0
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			eb.flush(true)
			return
		case <-ticker.C:
			eb.flush(false)
		case input := <-eb.Consumes:
			evt, ok := input.(pipeline.Event)
			if !ok {
				eb.log.Error("Received a non pipeline.Event message.")
				eb.Consumes <- false
				continue
			}

			if -time.Until(evt.Timestamp) > eb.config.IgnoreEventsOlderThan {
				eventBufferIgnoredItems.Inc()
				eb.log.Info("Ignored an incoming event, since it is too old")
				eb.Consumes <- false
				continue
			}
			heap.Push(&eb.buffer, &eventBufferItem{
				value:        evt,
				time:         evt.Timestamp.UnixNano(),
				receivedTime: time.Now(),
				counter:      counter,
			})
			counter++
			eventBufferCurrentItems.Inc()
			eb.Consumes <- true
			eb.flush(false)
		}
	}
}

func (eb *eventBuffer) flush(force bool) {
	// flush aged items or the oldest items if we have too many
	for (force && eb.buffer.Len() > 0) || (eb.buffer.Len() > eb.config.MaxEvents || (eb.buffer.Len() > 0 && -time.Until(eb.buffer[0].receivedTime) > eb.config.MaxAge)) {
		evt := heap.Pop(&eb.buffer).(*eventBufferItem).value
		eb.log.Debugw("Found an outaged item, flushing it now", "Case ID", evt.CaseId, "Timestamp", evt.Timestamp, "Activity", evt.ActivityName)
		eventBufferCurrentItems.Dec()
		eb.Publish(evt, true)
	}
}

type eventBufferItem struct {
	value        pipeline.Event
	time         int64
	receivedTime time.Time
	index        int
	counter      uint64
}

type timeOrderedEventBuffer []*eventBufferItem

func (pq timeOrderedEventBuffer) Len() int { return len(pq) }

func (pq timeOrderedEventBuffer) Less(i, j int) bool {
	return (pq[i].time == pq[j].time && pq[i].counter < pq[j].counter) || pq[i].time < pq[j].time // preserves order
}

func (pq timeOrderedEventBuffer) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *timeOrderedEventBuffer) Push(x interface{}) {
	n := len(*pq)
	item := x.(*eventBufferItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *timeOrderedEventBuffer) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
