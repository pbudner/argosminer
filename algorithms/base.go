package algorithms

import "github.com/pbudner/argosminer/events"

type StreamingAlgorithm interface {
	Append(event events.Event) error
	Close()
}
