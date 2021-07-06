package algorithms

import "github.com/pbudner/argosminer-collector/pkg/events"

type dfgStreamingAlgorithm struct {
}

func (algo dfgStreamingAlgorithm) Append(event events.Event) error {
	return nil
}

func (algo dfgStreamingAlgorithm) Close() {
	// do nothing
}
