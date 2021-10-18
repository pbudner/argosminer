package algorithms

import "github.com/pbudner/argosminer-collector/pkg/events"

type StreamingAlgorithm interface {
	Append(event events.Event) error
	Close()
}
