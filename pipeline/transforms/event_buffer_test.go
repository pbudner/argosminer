package transforms

import (
	"testing"
)

func TestBuffer(t *testing.T) {
	/*wg := &sync.WaitGroup{}
	ctx := context.Background()
	buffer := NewEventBuffer(EventBufferConfig{
		MaxEvents:     5,
		MaxAge:        500 * time.Millisecond,
		FlushInterval: 100 * time.Millisecond,
		//IgnoreEventsOlderThan: *(1 * time.Minute),
	})
	evts := make(chan interface{})
	buffer.Link(evts)
	receiver := buffer.Subscribe()
	go buffer.Run(wg, ctx)

	start := time.Now()
	evts <- pipeline.Event{
		ActivityName: "Activity A",
		CaseId:       "ID-4711",
		Timestamp:    time.Now(),
	}
	require.EqualValues(t, true, <-evts)
	evt := (<-receiver).(pipeline.Event)
	require.GreaterOrEqual(t, -time.Until(start), 5*time.Millisecond)
	require.EqualValues(t, "Activity A", evt.ActivityName)

	now := time.Now()
	evts <- pipeline.Event{
		ActivityName: "Activity B",
		CaseId:       "ID-4711",
		Timestamp:    now,
	}
	require.EqualValues(t, true, <-evts)

	evts <- pipeline.Event{
		ActivityName: "Activity C",
		CaseId:       "ID-4711",
		Timestamp:    now.Add(-100 * time.Millisecond),
	}
	require.EqualValues(t, true, <-evts)

	evts <- pipeline.Event{
		ActivityName: "Activity D",
		CaseId:       "ID-4711",
		Timestamp:    now.Add(100 * time.Millisecond),
	}
	require.EqualValues(t, true, <-evts)

	evts <- pipeline.Event{
		ActivityName: "Activity E",
		CaseId:       "ID-4711",
		Timestamp:    now.Add(-200 * time.Millisecond),
	}
	require.EqualValues(t, true, <-evts)

	evts <- pipeline.Event{
		ActivityName: "Activity F",
		CaseId:       "ID-4711",
		Timestamp:    now.Add(200 * time.Millisecond),
	}
	require.EqualValues(t, true, <-evts)

	evts <- pipeline.Event{
		ActivityName: "Activity G",
		CaseId:       "ID-4711",
		Timestamp:    now.Add(200 * time.Millisecond),
	}
	require.EqualValues(t, true, <-evts)

	evts <- pipeline.Event{
		ActivityName: "Activity Invalid",
		CaseId:       "ID-4711",
		Timestamp:    now.Add(-2 * time.Minute),
	}
	require.EqualValues(t, false, <-evts) // reject event

	evt = (<-receiver).(pipeline.Event)
	require.EqualValues(t, "Activity E", evt.ActivityName)
	evt = (<-receiver).(pipeline.Event)
	require.EqualValues(t, "Activity C", evt.ActivityName)
	evt = (<-receiver).(pipeline.Event)
	require.EqualValues(t, "Activity B", evt.ActivityName)
	evt = (<-receiver).(pipeline.Event)
	require.EqualValues(t, "Activity D", evt.ActivityName)
	evt = (<-receiver).(pipeline.Event)
	require.EqualValues(t, "Activity F", evt.ActivityName)
	evt = (<-receiver).(pipeline.Event)
	require.EqualValues(t, "Activity G", evt.ActivityName)

	// now check that "Activity invalid" is not published
	wg2 := &sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		evt = (<-receiver).(pipeline.Event)
		wg2.Done()
	}()
	wg2.Done() // this should not panic, because the goroutine should never finish
	wg2.Wait() // this should not timeout the unit test
	*/
}
