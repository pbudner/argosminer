package algorithms

import "github.com/pbudner/argosminer-collector/events"

type StreamingAlgorithm interface {
	Append(event events.Event) error
	Close()
}
