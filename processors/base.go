package processors

import "github.com/pbudner/argosminer/events"

type StreamingProcessor interface {
	Append(event *events.Event) error
	Close()
}
