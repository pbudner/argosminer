package parsers

import "github.com/pbudner/argosminer-collector/pkg/events"

type Parser interface {
	Parse(input string) (*events.Event, error)
	Close()
}
