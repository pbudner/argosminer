package receivers

import "github.com/pbudner/argosminer/events"

type StreamingReceiver interface {
	Append(event *events.Event) error
	Close()
}
